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
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 90;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

// BROWSERLESS SETUP

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// DETAIL PAGE SCRAPING FUNCTION

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();

	try {
		// Block unnecessary resources
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

		// Small human-like delay
		await detailPage.waitForTimeout(1500);

		// Extract coordinates from map
		const detailData = await detailPage.evaluate(() => {
			let lat = null;
			let lng = null;

			const mapDiv =
				document.querySelector("#map[data-lat][data-lng]") ||
				document.querySelector("div[data-lat][data-lng]");

			if (mapDiv) {
				lat = parseFloat(mapDiv.getAttribute("data-lat"));
				lng = parseFloat(mapDiv.getAttribute("data-lng"));
			}

			return { lat, lng };
		});

		// Save to database
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			null, // html (optional)
			detailData.lat,
			detailData.lng,
		);

		stats.totalScraped++;
		stats.totalSaved++;

		console.log(`    ✅ Detail scraped: ${property.title}`);
	} catch (error) {
		console.error(`    ❌ Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// Auto-scroll to load lazy-loaded properties
async function autoScroll(page) {
	await page.evaluate(async () => {
		await new Promise((resolve) => {
			let totalHeight = 0;
			const distance = 700;
			const timer = setInterval(() => {
				window.scrollBy(0, distance);
				totalHeight += distance;
				if (totalHeight >= document.body.scrollHeight - 1200) {
					clearInterval(timer);
					resolve();
				}
			}, 380);
		});
	});
}

// REQUEST HANDLER

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNumber, area } = request.userData;
	console.log(`\n Loading [${label}] ${area} Page ${pageNumber}: ${request.url}`);

	try {
		await page.goto(request.url, {
			waitUntil: "networkidle",
			timeout: 90000,
		});

		// Extra time for heavy JS
		await page.waitForTimeout(8000);

		// Multiple scrolls
		for (let i = 0; i < 3; i++) {
			await autoScroll(page);
			await page.waitForTimeout(2000);
		}

		// Debug: log page title to detect bot blocks or challenges
		const pageTitle = await page.title();
		console.log(`    📄 Page title: "${pageTitle}"`);

		// OpenRent listing cards: selector is now `a.search-property-card` with numeric hrefs (e.g. /2889736)
		await page
			.waitForSelector("a.search-property-card", { timeout: 15000 })
			.catch(() => { });

		// Debug: dump a portion of the HTML if no cards found
		const cardCount = await page.$$eval("a.search-property-card", els => els.length).catch(() => 0);
		if (cardCount === 0) {
			const bodySnippet = await page.evaluate(() => document.body?.innerHTML?.substring(0, 800) || "EMPTY BODY").catch(() => "EVAL ERROR");
			console.log(`    🐛 HTML snippet (first 800 chars):\n${bodySnippet}`);
		}

		const properties = await page.evaluate(() => {
			const cards = Array.from(document.querySelectorAll("a.search-property-card"));
			const items = [];

			cards.forEach((card) => {
				const href = card.getAttribute("href");
				if (!href) return;

				const fullText = (card.textContent || "").trim();

				// Price: first £ occurrence in card text
				const priceText = (fullText.match(/\u00a3[\d,]+/) || [""])[0];

				// Title: look for the property title element, fallback to first non-empty line
				const titleEl = card.querySelector(".p-name, .property-title, h2, h3");
				const title = (
					titleEl
						? titleEl.textContent.trim()
						: fullText.split("\n").map((l) => l.trim()).find((l) => l.length > 5) || "Property"
				).substring(0, 150);

				// Bedrooms: look for "X bed" pattern
				let bedrooms = null;
				const bedMatch = fullText.match(/(\d+)\s*(bed|beds)/i);
				if (bedMatch) bedrooms = parseInt(bedMatch[1]);

				items.push({
					link: href.startsWith("http") ? href : "https://www.openrent.co.uk" + href,
					title,
					priceText,
					bedrooms,
					statusText: fullText.toLowerCase(),
				});
			});

			return items;
		});

		console.log(`    🔍 Found ${properties.length} property cards on Page ${pageNumber}`);
		if (properties.length > 0) console.log("    Sample:", properties.slice(0, 2));

		if (properties.length === 0) {
			console.log("    ⚠️ Still no properties. Page may be blocked or heavily protected.");
			return;
		}

		// Deduplication + batch processing
		const seen = new Set();
		const deduped = [];
		for (const p of properties) {
			if (!p?.link) continue;
			const key = p.link.trim();
			if (seen.has(key)) continue;
			seen.add(key);
			deduped.push(p);
		}

		const batch = deduped.slice(0, 50);
		console.log(`    Processing ${batch.length} properties (deduped) on Page ${pageNumber}`);

		for (const property of batch) {
			const isSold = isSoldProperty(property.statusText || "");
			if (isSold) continue;

			const price = parsePrice(property.priceText);
			if (!price) {
				console.log(`    ⚠️ Skipping (bad price) ${property.link}`);
				continue;
			}

			// Detail scraping (coords from detail page)
			await scrapePropertyDetail(page.context(), property, isRental);
		}
	} catch (error) {
		console.error(` Error in handleListingPage: ${error.message}`);
	}
}

// CRAWLER SETUP

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1, // Stay at 1 for OpenRent to avoid immediate 429
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
				// Override automation fingerprints to bypass AWS WAF
				await page.addInitScript(() => {
					// Hide webdriver flag
					Object.defineProperty(navigator, "webdriver", {
						get: () => undefined,
					});
					// Fake plugins list (real browsers have plugins)
					Object.defineProperty(navigator, "plugins", {
						get: () => [
							{ name: "Chrome PDF Plugin" },
							{ name: "Chrome PDF Viewer" },
							{ name: "Native Client" },
						],
					});
					// Set realistic languages
					Object.defineProperty(navigator, "languages", {
						get: () => ["en-GB", "en"],
					});
					// Remove Chrome automation property
					delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
					delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
					delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
				});

				// Set a real browser user-agent
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

				// Set realistic viewport
				await page.setViewportSize({ width: 1366, height: 768 });
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(` Failed listing page: ${request.url}`);
		},
	});
}


// MAIN SCRAPER LOGIC

async function scrapeOpenRent() {
	console.log(` Starting OpenRent Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	// Focus only on Greater London (6,000+ properties)
	const AREAS = [{ name: "Greater London", term: "Greater%20London", pages: 350 }];

	for (const area of AREAS) {
		const requests = [];
		const path = area.term.toLowerCase().replace(/%20/g, "-");
		const baseUrl = `https://www.openrent.co.uk/properties-to-rent/${path}?term=${area.term}&isLive=true`;

		for (let p = 0; p < area.pages; p++) {
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

	console.log(
		`\n Finished OpenRent - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// MAIN EXECUTION

(async () => {
	try {
		await scrapeOpenRent();
		await updateRemoveStatus(AGENT_ID);
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();

