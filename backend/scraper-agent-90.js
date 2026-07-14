// OpenRent scraper using Playwright with Crawlee
// Agent ID: 90
// Website: openrent.co.uk

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
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
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 90;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	totalCreated: 0,
	totalUpdated: 0,
	totalSkipped: 0,
	totalErrors: 0,
};

const MAX_PROPERTIES_PER_PAGE = 50;

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

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

// ==================== BEST TITLE EXTRACTION ====================
function buildListingProperties() {
	return Array.from(document.querySelectorAll("a.search-property-card, a[href*='/property-to-rent/'], a[href*='/property/'], article a, .listing-card a"))
		.map((card) => {
			const href = card.getAttribute("href");
			if (!href) return null;

			const fullText = (card.innerText || card.textContent || "").trim();

			let title = "";

			// 1. Try headings first (most reliable)
			const heading = card.querySelector("h1, h2, h3, h4, .fs-d-4, .fs-lg-d-3");
			if (heading) title = heading.innerText.trim();

			// 2. Try address/title classes
			if (!title || title.length < 20) {
				const addressEl = card.querySelector('[class*="address"], [class*="title"], .property-address, .listing-title');
				if (addressEl) title = addressEl.innerText.trim();
			}

			// 3. Best fallback: First long clean line from full text
			if (!title || title.length < 25) {
				const lines = fullText
					.split(/\n|\s{2,}/)
					.map(l => l.trim())
					.filter(l => l.length > 20);

				for (const line of lines) {
					if (/^£|pcm|pm|pa|bed|bath|available|let agreed|withdrawn|to rent/i.test(line)) continue;
					title = line;
					break;
				}
			}

			// 4. Final fallback from URL slug (Exact property name as in URL)
			if ((!title || title.length < 20) && (href.includes('/property/') || href.includes('/property-to-rent/') || href.includes('/property-for-sale/'))) {
				const parts = href.split('/');
				let slug = "";
				if (parts.length >= 4 && parts[3].length > 10) {
					slug = parts[3];
				} else if (parts.length >= 3 && parts[2].length > 10) {
					slug = parts[2];
				}

				if (slug && slug.length > 5 && !/^\d+$/.test(slug)) {
					title = slug
						.replace(/-/g, ' ')
						.replace(/\b\w/g, c => c.toUpperCase())
						.trim();
				}
			}

			if (!title || title.length < 10) title = "Property";

			// Clean price and junk
			title = title
				.replace(/\s*[•-]\s*£[\d,]+.*$/gi, "")
				.replace(/£[\d,]+.*$/gi, "")
				.replace(/\s+/g, " ")
				.trim()
				.substring(0, 150);

			// Price
			let priceText = "";
			const priceEl = card.querySelector('[class*="price"], .price, strong, b');
			if (priceEl) {
				priceText = priceEl.innerText.trim();
			} else {
				const match = fullText.match(/£[\d,]+(?:\.\d+)?/);
				priceText = match ? match[0] : "";
			}

			// Bedrooms
			const bedMatch = fullText.match(/(\d+)\s*(?:bed|bedroom|bedrooms)/i);

			return {
				link: href.startsWith("http") ? href : `https://www.openrent.co.uk${href}`,
				title,
				priceText,
				bedrooms: bedMatch ? parseInt(bedMatch[1]) : null,
				statusText: fullText.toLowerCase(),
			};
		})
		.filter(Boolean);
}

function parseListingHtmlWithCheerio(html) {
	const $ = cheerio.load(html);
	const items = [];

	$("a.search-property-card, a[href*='/property-to-rent/'], a[href*='/property/']").each((_, el) => {
		const card = $(el);
		const href = card.attr("href");
		if (!href) return;

		const fullText = card.text().trim();
		let title = card.find("h1, h2, h3, h4, .fs-d-4, .fs-lg-d-3, [class*='title']").first().text().trim();

		if (!title || title.length < 10) {
			title = fullText.split(/\n|\s{2,}/).map(l => l.trim()).find(l => l.length > 20 && !/^£|pcm|pm|pa|bed|bath/i.test(l)) || "Property";
		}

		title = title.replace(/\s*[•-]\s*£[\d,]+.*$/gi, "").replace(/£[\d,]+.*$/gi, "").replace(/\s+/g, " ").trim().substring(0, 150);

		let priceText = card.find('[class*="price"], .price, strong, b').first().text().trim();
		if (!priceText) {
			const match = fullText.match(/£[\d,]+(?:\.\d+)?/);
			priceText = match ? match[0] : "";
		}

		const bedMatch = fullText.match(/(\d+)\s*(?:bed|bedroom|bedrooms)/i);

		items.push({
			link: href.startsWith("http") ? href : `https://www.openrent.co.uk${href}`,
			title,
			priceText,
			bedrooms: bedMatch ? parseInt(bedMatch[1]) : null,
			statusText: fullText.toLowerCase(),
		});
	});

	return items;
}

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources({ page: detailPage });

		await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 60000 });
		await detailPage.waitForTimeout(2000);

		const detailHtml = await detailPage.content();

		const detailData = await detailPage.evaluate(() => {
			const parseFloatSafe = (v) => Number.isFinite(parseFloat(v)) ? parseFloat(v) : null;
			let lat = null, lng = null;
			const mapDiv = document.querySelector("[data-lat][data-lng]");
			if (mapDiv) {
				lat = parseFloatSafe(mapDiv.getAttribute("data-lat"));
				lng = parseFloatSafe(mapDiv.getAttribute("data-lng"));
			}

			// Get the exact property name from the main heading
			const titleHeading = document.querySelector("h1, .property-title");
			const title = titleHeading ? titleHeading.innerText.trim() : null;

			return { lat, lng, title };
		});

		const finalTitle = detailData.title || property.title;

		const result = await processPropertyWithCoordinates(
			property.link,
			property.price,
			finalTitle,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			detailHtml,
			detailData.lat,
			detailData.lng,
		);

		return { success: true, coordsFound: !!(detailData.lat && detailData.lng), result };
	} catch (error) {
		logger.error(`Detail page error for ${property.link}`, error);
		return { success: false };
	} finally {
		await detailPage.close();
	}
}

async function autoScroll(page) {
	await page.evaluate(async () => {
		for (let i = 0; i < 8; i++) {
			window.scrollBy(0, 800);
			await new Promise(r => setTimeout(r, 400));
		}
	});
}

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNum, totalPages, area } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });
		await page.waitForSelector('a.search-property-card, a[href*="/property-to-rent/"], a[href*="/property/"]', { timeout: 10000 }).catch(() => { });

		await autoScroll(page);

		let properties = await page.evaluate(buildListingProperties);
		
		if (!properties || properties.length === 0) {
			const html = await page.content();
			properties = parseListingHtmlWithCheerio(html);
		}

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

		if (properties.length === 0) return;

		const seen = new Set();
		const batch = properties
			.map(p => ({ ...p, link: normalizePropertyUrl(p.link) }))
			.filter(p => p.link && !seen.has(p.link) && seen.add(p.link))
			.slice(0, MAX_PROPERTIES_PER_PAGE);

		for (const property of batch) {
			if (isSoldProperty(property.statusText || "")) {
				counts.totalSkipped++;
				logger.property(pageNum, label, property.title.substring(0, 30), "", property.link, isRental, totalPages, "SKIPPED");
				continue;
			}

			const price = parsePrice(property.priceText);
			if (!price) {
				counts.totalSkipped++;
				continue;
			}

			property.price = price;

			const dbResult = await updatePriceByPropertyURLOptimized(
				property.link, price, property.title, property.bedrooms || null, AGENT_ID, isRental
			);

			if (dbResult?.error) {
				counts.totalErrors++;
				continue;
			}

			let propertyAction = "UNCHANGED";
			if (dbResult.updated) propertyAction = "UPDATED";

			if (dbResult?.isExisting && !dbResult.missingData) {
				counts.totalScraped++;
				if (dbResult.updated) counts.totalUpdated++;

				logger.property(
					pageNum,
					label,
					property.title.substring(0, 30),
					formatPriceDisplay(price, isRental),
					property.link,
					isRental,
					totalPages,
					propertyAction
				);
				continue;
			}

			// New property or missing data -> visit detail page
			if (!dbResult.isExisting) propertyAction = "CREATED";

			const detailResult = await scrapePropertyDetail(page.context(), property, isRental);

			counts.totalScraped++;
			counts.totalSaved++;
			if (dbResult?.isExisting) counts.totalUpdated++;
			else counts.totalCreated++;

			logger.property(
				pageNum,
				label,
				property.title.substring(0, 30),
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				totalPages,
				propertyAction
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500); // DB politeness
			}
		}
	} catch (error) {
		counts.totalErrors++;
		logger.error(`Listing page error on page ${pageNum}`, error);
	}
}

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 600,
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await page.setExtraHTTPHeaders({
					"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
				});
				await blockNonEssentialResources({ page });
			},
		],
		requestHandler: handleListingPage,
	});
}

async function scrapeOpenRent() {
	logger.step(`Starting OpenRent Scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const crawler = createCrawler(getBrowserlessEndpoint());

	const AREAS = [{ name: "Greater London", term: "Greater%20London", pages: 350 }];

	for (const area of AREAS) {
		const requests = [];
		const baseUrl = `https://www.openrent.co.uk/properties-to-rent/greater-london?term=Greater%20London&isLive=true`;
		const totalPages = area.pages;

		const effectiveStartPage = Math.max(1, startPage);
		for (let p = effectiveStartPage - 1; p < area.pages; p++) {
			const skip = p * 20;
			const url = skip === 0 ? baseUrl : `${baseUrl}&skip=${skip}`;
			requests.push({
				url,
				userData: { pageNum: p + 1, totalPages, isRental: true, label: "RENTALS", area: area.name },
			});
		}
		await crawler.addRequests(requests);
	}

	await crawler.run();

	logger.step(
		`Finished OpenRent - Total scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Created: ${counts.totalCreated}, Updated: ${counts.totalUpdated}, Skipped: ${counts.totalSkipped}`
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

(async () => {
	try {
		await scrapeOpenRent();
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
