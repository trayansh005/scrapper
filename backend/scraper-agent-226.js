// Palmer Partners scraper using Playwright + Homeflow .ljson API
// Agent ID: 226
// Website: palmerpartners.com (Homeflow)
// Strategy: Navigate listing HTML page with Playwright (for bot protection),
//           then use in-browser fetch to .ljson API endpoint to get lat/lng
//           for all properties. No detail page visits needed.
// Usage:
// node backend/scraper-agent-226.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 226;
const BASE_URL = "https://www.palmerpartners.com";
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================
function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
}

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================
function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================
async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNum = 1 } = request.userData;
	logger.page(pageNum, label, request.url, "?");

	await page.waitForTimeout(2000);
	// Wait for window.properties to be populated by the server-rendered inline script
	await page
		.waitForFunction(() => typeof properties !== "undefined" && properties.length > 0, {
			timeout: 30000,
		})
		.catch(() => {});

	const rawProperties = await page.evaluate(() =>
		typeof properties !== "undefined" && Array.isArray(properties) ? properties : [],
	);

	if (!rawProperties.length) {
		logger.page(pageNum, label, `No properties found — stopping pagination`, "?");
		return;
	}

	// Queue next page — pagination continues until a page returns no properties
	const baseUrl = request.url.replace(/[?&]page=\d+/, "").replace(/\/+$/, "");
	await crawler.addRequests([
		{
			url: `${baseUrl}/?page=${pageNum + 1}`,
			userData: { isRental, label, pageNum: pageNum + 1 },
		},
	]);

	logger.page(pageNum, label, `Found ${rawProperties.length} properties in window.properties`, "?");
	stats.totalFound += rawProperties.length;

	for (const prop of rawProperties) {
		try {
			const relativeUrl = prop.Url || prop.url;
			if (!relativeUrl) continue;

			const fullUrl = relativeUrl.startsWith("http") ? relativeUrl : `${BASE_URL}${relativeUrl}`;

			if (processedUrls.has(fullUrl)) continue;
			processedUrls.add(fullUrl);
			stats.totalScraped++;

			// statusName: e.g. "Under Offer", "Sold", "For Sale"
			if (isSoldProperty(prop.statusName || "")) {
				stats.totalSkipped++;
				logger.property(
					pageNum,
					label,
					(prop.fullAddress || prop.label || "Property").substring(0, 40),
					formatPriceDisplay(null, isRental),
					fullUrl,
					isRental,
					"?",
					"SKIPPED",
				);
				continue;
			}

			// lowPrice is a numeric value
			const price = prop.lowPrice ? Math.round(prop.lowPrice) : 0;
			if (!price) {
				stats.totalSkipped++;
				continue;
			}

			const title = prop.fullAddress || prop.label || "Property";
			const bedrooms = prop.NumberBedrooms ? parseInt(prop.NumberBedrooms, 10) : null;
			// Coordinates from coordinates.lat/lng (number) or latitude/longitude (string)
			const lat = prop.coordinates?.lat
				? parseFloat(prop.coordinates.lat)
				: prop.latitude
					? parseFloat(prop.latitude)
					: null;
			const lng = prop.coordinates?.lng
				? parseFloat(prop.coordinates.lng)
				: prop.longitude
					? parseFloat(prop.longitude)
					: null;

			const result = await updatePriceByPropertyURLOptimized(
				fullUrl,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";

			if (result.error) {
				propertyAction = "ERROR";
				stats.totalSkipped++;
			} else if (!result.isExisting) {
				// New property — save with coordinates from inline JSON (no detail page needed)
				await processPropertyWithCoordinates(
					fullUrl,
					price,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					null,
					lat,
					lng,
				);
				propertyAction = "CREATED";
				stats.totalSaved++;
				if (isRental) stats.savedRentals++;
				else stats.savedSales++;
			} else if (result.updated) {
				propertyAction = "UPDATED";
				stats.totalSaved++;
				if (isRental) stats.savedRentals++;
				else stats.savedSales++;
			}

			logger.property(
				pageNum,
				label,
				title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				fullUrl,
				isRental,
				"?",
				propertyAction,
				lat,
				lng,
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500);
			}
		} catch (err) {
			logger.error(`Error processing property`, err, pageNum, label);
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
		requestHandlerTimeoutSecs: 600,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			const { pageNum, label } = request.userData || {};
			logger.error(`Failed listing page: ${request.url}`, null, pageNum, label);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================
async function scrapePalmerPartners() {
	const scrapeStartTime = new Date();
	const startPage = getStartPage();
	const isPartialRun = startPage > 1;
	logger.step(`Starting Palmer Partners scraper (Agent ${AGENT_ID})...`);
	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const PROPERTY_TYPES = [
		{
			baseUrl: "https://www.palmerpartners.com/buy/property-for-sale",
			isRental: false,
			label: "SALES",
		},
		{
			baseUrl: "https://www.palmerpartners.com/let/property-to-let",
			isRental: true,
			label: "RENTALS",
		},
	];

	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label}...`);

		const url =
			startPage === 1 ? `${propertyType.baseUrl}/` : `${propertyType.baseUrl}/?page=${startPage}`;

		const crawler = createCrawler(browserWSEndpoint);
		await crawler.run([
			{
				url,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
					pageNum: startPage,
				},
			},
		]);
	}

	logger.step(
		`Completed Palmer Partners Agent ${AGENT_ID}`,
		`found=${stats.totalFound}, scraped=${stats.totalScraped}, saved=${stats.totalSaved}, skipped=${stats.totalSkipped}, sales=${stats.savedSales}, rentals=${stats.savedRentals}`,
	);

	if (!isPartialRun) {
		logger.step(`Updating remove status...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

(async () => {
	try {
		await scrapePalmerPartners();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
