// Jackie Quinn scraper using Playwright with Crawlee
// Agent ID: 8
// Website: www.jackiequinn.co.uk
// Usage:
// node backend/scraper-agent-8.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	extractBedroomsFromHTML,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 8;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedLettings: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "networkidle",   // better than domcontentloaded
			timeout: 60000,
		});

		// Wait for map container to appear
		await detailPage.waitForSelector('#map, .map, [id*="map"], iframe[src*="google.com/maps"], .property-map',
			{ timeout: 15000 }).catch(() => { });

		// Click any "View Map" / "Map" link/tab if present
		await detailPage.evaluate(() => {
			const mapTriggers = [
				'a[href*="map"]',
				'button:contains("Map")',
				'.map-tab',
				'[data-tab*="map"]',
				'a:contains("map")'
			];
			for (const sel of mapTriggers) {
				const el = document.querySelector(sel);
				if (el) {
					el.click();
					return;
				}
			}
		}).catch(() => { });

		await detailPage.waitForTimeout(3000); // Give map JS time to run

		// Extract coordinates using multiple strategies
		const coords = await detailPage.evaluate(() => {
			// Strategy 1: Look for common patterns in window / global variables
			if (window.mapData || window.propertyData || window.__PRELOADED_STATE__) {
				const data = window.mapData || window.propertyData || window.__PRELOADED_STATE__;
				if (data.latitude && data.longitude) {
					return { lat: data.latitude, lng: data.longitude };
				}
			}

			// Strategy 2: Google Maps iframe src (common)
			const iframe = document.querySelector('iframe[src*="google.com/maps"], iframe[src*="maps.googleapis"]');
			if (iframe) {
				const src = iframe.src;
				const match = src.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/) ||
					src.match(/q=(-?\d+\.\d+),(-?\d+\.\d+)/);
				if (match) return { lat: parseFloat(match[1]), lng: parseFloat(match[2]) };
			}

			// Strategy 3: data attributes on map div
			const mapDiv = document.querySelector('#map, .map-container, [class*="map"]');
			if (mapDiv) {
				const lat = mapDiv.dataset.lat || mapDiv.dataset.latitude;
				const lng = mapDiv.dataset.lng || mapDiv.dataset.longitude;
				if (lat && lng) return { lat: parseFloat(lat), lng: parseFloat(lng) };
			}

			// Strategy 4: Search in all script tags for JSON containing lat/lng
			const scripts = Array.from(document.querySelectorAll('script'));
			for (const script of scripts) {
				const text = script.textContent || '';
				const latMatch = text.match(/"latitude":\s*(-?\d+\.\d+)/i) ||
					text.match(/"lat":\s*(-?\d+\.\d+)/i);
				const lngMatch = text.match(/"longitude":\s*(-?\d+\.\d+)/i) ||
					text.match(/"lng":\s*(-?\d+\.\d+)/i);
				if (latMatch && lngMatch) {
					return {
						lat: parseFloat(latMatch[1]),
						lng: parseFloat(lngMatch[1])
					};
				}
			}

			return null;
		});

		const htmlContent = await detailPage.content();

		const dbResult = await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			coords ? coords.lat : null,     // Pass extracted coords
			coords ? coords.lng : null
		);

		// ... rest of your stats

		return coords || { latitude: null, longitude: null };

	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
		return { latitude: null, longitude: null };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// PARSING LOGIC (Listing Page)
// ============================================================================

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const results = [];

	$(".propertyBox").each((_, el) => {
		const $item = $(el);

		const $linkEl = $item.find("h2.searchProName a");
		const rawHref = $linkEl.attr("href");
		if (!rawHref) return;

		const link = rawHref.startsWith("http") ? rawHref : `https://www.jackiequinn.co.uk${rawHref}`;
		const title = $linkEl.text().trim();

		const priceText = $item.find("h3 div").text().trim();
		if (isSoldProperty(priceText)) return;

		const price = formatPriceUk(priceText);
		if (!price) return;

		const description = $item.find(".featuredDescriptions").text().trim();
		const allText = $item.text();

		// Use centralized helper for bedroom extraction
		const bedrooms = extractBedroomsFromHTML(allText);

		results.push({ link, title, price, bedrooms });
	});

	return results;
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNumber, totalPages, isRental, label } = request.userData;
	logger.page(pageNumber, label, request.url, totalPages);

	await page.waitForSelector(".propertyBox", { timeout: 15000 }).catch(() => { });
	await page.waitForSelector(".propertyBox", { timeout: 30000 }).catch(() => { });

	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	logger.page(
		pageNumber,
		label,
		`Found ${properties.length} properties on page ${pageNumber}`,
		totalPages,
	);

	for (const property of properties) {
		stats.totalScraped++;
		if (!property.link) continue;
		if (processedUrls.has(property.link)) {
			logger.property(
				pageNumber,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(property.price, isRental),
				property.link,
				isRental,
				totalPages,
				"SKIPPED: ALREADY PROCESSED",
			);
			continue;
		}
		processedUrls.add(property.link);

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "UNCHANGED";
		let coords = { latitude: null, longitude: null };

		if (result.updated) propertyAction = "UPDATED";

		if (!result.isExisting && !result.error) {
			propertyAction = "CREATED";
			coords = await scrapePropertyDetail(page.context(), property, isRental);
		}
		if (propertyAction === "CREATED") {
			stats.totalSaved++;

			if (isRental) stats.savedLettings++;
			else stats.savedSales++;
		}

		logger.property(
			pageNumber,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(property.price, isRental),
			property.link,
			isRental,
			totalPages,
			propertyAction,
			coords.latitude,
			coords.longitude,
		);

		if (propertyAction === "CREATED") {
			await sleep(1000);
		}
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeJackieQuinn() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Jackie Quinn scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const totalPages = 21;
	const requests = [];
	for (let pageNum = Math.max(1, startPage); pageNum <= totalPages; pageNum++) {
		requests.push({
			url: `https://www.jackiequinn.co.uk/search?category=1&listingtype=5&statusids=1%2C10%2C4%2C16%2C3&obc=Price&obd=Descending&page=${pageNum}`,
			userData: {
				pageNumber: pageNum,
				totalPages,
				isRental: false,
				label: "SALES",
			},
		});
	}

	if (requests.length > 0) {
		logger.step(`Queueing ${requests.length} listing pages starting from page ${startPage}...`);
		await crawler.run(requests);
	}

	logger.step(
		`Completed Jackie Quinn - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status for properties not seen in this run...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

(async () => {
	try {
		await scrapeJackieQuinn();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
