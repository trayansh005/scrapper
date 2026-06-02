// OpenRent scraper using Playwright with Crawlee
// Agent ID: 90
// Website: openrent.co.uk
// Usage:
// node backend/scraper-agent-90.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 90;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	pagesProcessed: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalUpdated: 0,
	totalCreated: 0,
	totalSkipped: 0,
	totalErrors: 0,
};

const MAX_PROPERTIES_PER_PAGE = 50;

// BROWSERLESS SETUP

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

function normalizePropertyUrl(rawUrl) {
	if (!rawUrl) return null;
	try {
		const url = new URL(rawUrl, "https://www.openrent.co.uk");
		return `${url.origin}${url.pathname}`.replace(/\/+$/, "");
	} catch (error) {
		return rawUrl.trim();
	}
}

function buildListingProperties() {
	return Array.from(document.querySelectorAll("a.search-property-card, a[href*='/property/']"))
		.map((card) => {
			const href = card.getAttribute("href");
			if (!href) return null;

			const fullText = (card.textContent || "").trim();

			const priceEl = card.querySelector("[class*='price'], .price, .property-price, .listing-price");
			const priceText =
				(priceEl && priceEl.textContent && priceEl.textContent.trim()) ||
				(fullText.match(/\u00a3[\d,]+(?:\.\d+)?/) || [""])[0] ||
				"";

			const titleEl =
				card.querySelector(".p-name, .property-title, h2, h3, [class*='title'], [data-testid='listing-title']");
			const title = (
				(titleEl ? titleEl.textContent.trim() : null) ||
				fullText
					.split("\n")
					.map((line) => line.trim())
					.filter(Boolean)
					.find((line) => line.length > 5) ||
				"Property"
			).substring(0, 150);

			let bedrooms = null;
			const bedMatch = fullText.match(/(\d+)\s*(bedroom|bedrooms|bed)/i);
			if (bedMatch) bedrooms = parseInt(bedMatch[1], 10);

			const statusTextEl = card.querySelector("[class*='status'], .status, .label, .badge");
			const statusText =
				(statusTextEl && statusTextEl.textContent && statusTextEl.textContent.trim().toLowerCase()) ||
				fullText.toLowerCase();

			return {
				link: href.startsWith("http") ? href : `https://www.openrent.co.uk${href}`,
				title,
				priceText,
				bedrooms,
				statusText,
			};
		})
		.filter(Boolean);
}

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();
	let detailHtml = null;

	try {
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		await detailPage.waitForTimeout(1500);

		detailHtml = await detailPage.content();

		const detailData = await detailPage.evaluate(() => {
			const parseFloatSafe = (value) => {
				const normalized = String(value || "").trim();
				const num = parseFloat(normalized);
				return Number.isFinite(num) ? num : null;
			};

			let lat = null;
			let lng = null;

			const mapDiv = document.querySelector("[data-lat][data-lng]");
			if (mapDiv) {
				lat = parseFloatSafe(mapDiv.getAttribute("data-lat"));
				lng = parseFloatSafe(mapDiv.getAttribute("data-lng"));
			}

			if (!lat || !lng) {
				const latEl = document.querySelector("[data-lat]");
				const lngEl = document.querySelector("[data-lng]");
				if (latEl && lngEl) {
					lat = parseFloatSafe(latEl.getAttribute("data-lat") || latEl.getAttribute("data-lat") || latEl.textContent);
					lng = parseFloatSafe(lngEl.getAttribute("data-lng") || lngEl.textContent);
				}
			}

			if ((!lat || !lng) && window.location.href) {
				const iframe = document.querySelector("iframe[src*='maps.google.com'], iframe[src*='google.com/maps']");
				if (iframe) {
					const match = iframe.src.match(/@([0-9.-]+),([0-9.-]+),/);
					if (match) {
						lat = parseFloatSafe(match[1]);
						lng = parseFloatSafe(match[2]);
					}
				}
			}

			return { lat, lng };
		});

		const result = await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			detailHtml,
			detailData.lat,
			detailData.lng,
		);

		return {
			success: true,
			coordsFound: detailData.lat !== null && detailData.lng !== null,
			result,
		};
	} catch (error) {
		logger.error(`Error scraping detail page`, error, null, null);
		return { success: false, error: error.message || String(error) };
	} finally {
		await detailPage.close();
	}
}

async function autoScroll(page) {
	await page.evaluate(async () => {
		const distance = 700;
		let previousHeight = document.body.scrollHeight;
		for (let i = 0; i < 10; i += 1) {
			window.scrollBy(0, distance);
			await new Promise((resolve) => setTimeout(resolve, 380));
			const currentHeight = document.body.scrollHeight;
			if (currentHeight === previousHeight) break;
			previousHeight = currentHeight;
		}
	});
}

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNumber, area } = request.userData;
	logger.page(pageNumber, label, `Loading ${area} page`, null);

	try {
		await page.goto(request.url, {
			waitUntil: "networkidle",
			timeout: 90000,
		});

		await page.waitForTimeout(6000);

		for (let i = 0; i < 3; i += 1) {
			await autoScroll(page);
			await page.waitForTimeout(1500);
		}

		const pageTitle = await page.title();
		logger.page(pageNumber, label, `Page title: ${pageTitle}`);

		const cardSelector = "a.search-property-card, a[href*='/property/']";
		const cardCount = await page.$$eval(cardSelector, (els) => els.length).catch(() => 0);

		if (cardCount === 0) {
			const bodySnippet = await page.evaluate(() => document.body?.innerText?.slice(0, 800) || "EMPTY BODY").catch(() => "EVAL ERROR");
			logger.warn(`No listing cards found on page. HTML snippet: ${bodySnippet}`, pageNumber, label);
			return;
		}

		const properties = await page.evaluate(buildListingProperties);
		logger.page(pageNumber, label, `Found ${properties.length} potential cards`, null);

		if (!properties.length) {
			logger.warn("No property data could be extracted from listing cards.", pageNumber, label);
			return;
		}

		const deduped = [];
		const seenLinks = new Set();
		for (const property of properties) {
			const normalizedLink = normalizePropertyUrl(property.link);
			if (!normalizedLink || seenLinks.has(normalizedLink)) continue;
			seenLinks.add(normalizedLink);
			deduped.push({ ...property, link: normalizedLink });
		}

		const batch = deduped.slice(0, MAX_PROPERTIES_PER_PAGE);
		logger.page(pageNumber, label, `Processing ${batch.length} deduplicated properties`, null);

		for (const property of batch) {
			if (isSoldProperty(property.statusText || "")) {
				stats.totalSkipped += 1;
				continue;
			}

			const price = parsePrice(property.priceText);
			if (!price) {
				stats.totalSkipped += 1;
				logger.property(pageNumber, label, property.title, property.priceText || "NO PRICE", property.link, isRental, null, "SKIPPED");
				continue;
			}

			const dbResult = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
		);

			if (dbResult && dbResult.error) {
				stats.totalErrors += 1;
				logger.property(pageNumber, label, property.title, formatPriceDisplay(price, isRental), property.link, isRental, null, "ERROR");
				continue;
			}

			if (dbResult?.isExisting && !dbResult.missingData) {
				stats.totalScraped += 1;
				if (dbResult.updated) {
					stats.totalSaved += 1;
					stats.totalUpdated += 1;
				}
				logger.property(
					pageNumber,
					label,
					property.title,
					formatPriceDisplay(price, isRental),
					property.link,
					isRental,
					null,
					dbResult.updated ? "UPDATED" : "UNCHANGED",
				);
				continue;
			}

			const detailResult = await scrapePropertyDetail(page.context(), property, isRental);
			if (!detailResult.success) {
				stats.totalErrors += 1;
				logger.property(pageNumber, label, property.title, formatPriceDisplay(price, isRental), property.link, isRental, null, "ERROR");
				continue;
			}

			stats.totalScraped += 1;
			stats.totalSaved += 1;
			if (dbResult?.isExisting) {
				stats.totalUpdated += 1;
			} else {
				stats.totalCreated += 1;
			}

			logger.property(
				pageNumber,
				label,
				property.title,
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				null,
				dbResult?.isExisting ? "UPDATED" : "CREATED",
				detailResult.coordsFound ? detailResult.result?.latitude : null,
				detailResult.coordsFound ? detailResult.result?.longitude : null,
			);
		}
	} catch (error) {
		stats.totalErrors += 1;
		logger.error("Error in handleListingPage", error, pageNumber, label);
	}
}

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 600,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: [
					"--no-sandbox",
					"--disable-setuid-sandbox",
					"--disable-blink-features=AutomationControlled",
				],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await page.addInitScript(() => {
					Object.defineProperty(navigator, "webdriver", {
						get: () => undefined,
					});
					Object.defineProperty(navigator, "plugins", {
						get: () => [
							{ name: "Chrome PDF Plugin" },
							{ name: "Chrome PDF Viewer" },
							{ name: "Native Client" },
						],
					});
					Object.defineProperty(navigator, "languages", {
						get: () => ["en-GB", "en"],
					});
					delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
					delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
					delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
				});

			await page.setExtraHTTPHeaders({
				"User-Agent":
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
				"Accept-Language": "en-GB,en;q=0.9",
				Accept:
					"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
				"Sec-Fetch-Site": "none",
				"Sec-Fetch-Mode": "navigate",
				"Sec-Fetch-User": "?1",
				"Sec-Fetch-Dest": "document",
				"Upgrade-Insecure-Requests": "1",
			});

			await page.setViewportSize({ width: 1366, height: 768 });
		},
	],
	requestHandler: handleListingPage,
	failedRequestHandler({ request }) {
		logger.error(`Failed listing page: ${request.url}`);
	},
});
}

async function scrapeOpenRent() {
	logger.step(`Starting OpenRent Scraper (Agent ${AGENT_ID})...`);
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const AREAS = [{ name: "Greater London", term: "Greater%20London", pages: 350 }];

	for (const area of AREAS) {
		const requests = [];
		const path = area.term.toLowerCase().replace(/%20/g, "-");
		const baseUrl = `https://www.openrent.co.uk/properties-to-rent/${path}?term=${area.term}&isLive=true`;

		for (let p = 0; p < area.pages; p += 1) {
			const skip = p * 20;
			const url = skip === 0 ? baseUrl : `${baseUrl}&skip=${skip}`;
			requests.push({
				url,
				userData: {
					pageNumber: p + 1,
					isRental: true,
					label: "RENTALS",
					area: area.name,
				},
			});
		}
		await crawler.addRequests(requests);
	}

	await crawler.run();

	logger.step(
		`Finished OpenRent - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}, Created: ${stats.totalCreated}, Updated: ${stats.totalUpdated}, Skipped: ${stats.totalSkipped}, Errors: ${stats.totalErrors}`,
	);

	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
	try {
		await scrapeOpenRent();
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error during scrape", err);
		process.exit(1);
	}
})();
