// Guild Property scraper using Playwright with Crawlee
// Agent ID: 35
//
// Usage:
// node backend/scraper-agent-35.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { formatPriceUk } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 35;
const logger = createAgentLogger(AGENT_ID);
const scrapeStartTime = new Date();

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

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

async function handleListingPage({ page, request }) {
	const { pageNum, totalPages, isRental, label } = request.userData;
	logger.page(pageNum, label, `Processing map search page ${request.url}`, totalPages);

	// Wait for the map or any results indicator
	await page.waitForTimeout(3000);

	// Extract locations data from window.locations
	const propertiesData = await page.evaluate(() => {
		return window.locations || [];
	});

	logger.page(pageNum, label, `Found ${propertiesData.length} properties via map data`, totalPages);

	// Process each property from the map data
	// Data structure from research: [lat, lon, img, price, address, beds, baths, ?, id, url]
	for (const data of propertiesData) {
		try {
			const latitude = parseFloat(data[0]);
			const longitude = parseFloat(data[1]);
			const rawPrice = data[3];
			const title = data[4];
			const bedrooms = data[5];
			const relativeUrl = data[9];
			const link = relativeUrl.startsWith("http")
				? relativeUrl
				: "https://www.guildproperty.co.uk" + relativeUrl;

			const price = formatPriceUk(rawPrice);

			// Update price in database (or insert minimal record if new)
			const result = await updatePriceByPropertyURLOptimized(
				link,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			if (result.updated) {
				stats.totalSaved++;
				logger.property(pageNum, label, title, price, link, isRental, totalPages, "UPDATED");
			} else if (result.isExisting) {
				logger.property(pageNum, label, title, price, link, isRental, totalPages, "UNCHANGED");
			}

			// If new property, we already have coordinates, so we can process fully
			if (!result.isExisting && !result.error) {
				await processPropertyWithCoordinates(
					link,
					price,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					"", // No HTML needed as we have manual coords
					latitude,
					longitude,
				);
				stats.totalScraped++;
				stats.totalSaved++;
				logger.property(
					pageNum,
					label,
					title,
					price,
					link,
					isRental,
					totalPages,
					"CREATED",
					latitude,
					longitude,
				);
			}
		} catch (err) {
			logger.error(`Error processing property from map data`, err, pageNum, label);
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
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(
				`Failed listing page: ${request.url}`,
				null,
				request.userData.pageNum,
				request.userData.label,
			);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeGuildProperty() {
	logger.step(`Starting Guild Property scraper (Agent ${AGENT_ID})`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	// Map search usually shows up to 200 properties.
	const totalSalesPages = 45;
	const totalLettingsPages = 10;

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let p = Math.max(1, startPage); p <= totalSalesPages; p++) {
		allRequests.push({
			url: `https://www.guildproperty.co.uk/search/map?page=${p}&national=false&p_department=RS&location=London&searchRadius=50&availability=1`,
			userData: {
				pageNum: p,
				totalPages: totalSalesPages,
				isRental: false,
				label: `SALES_PAGE`,
			},
		});
	}

	// Build Lettings requests (only if full run or specifically targeted)
	if (startPage === 1) {
		for (let p = 1; p <= totalLettingsPages; p++) {
			allRequests.push({
				url: `https://www.guildproperty.co.uk/search/map?page=${p}&national=false&p_department=RL&location=London&searchRadius=50&availability=1`,
				userData: {
					pageNum: p,
					totalPages: totalLettingsPages,
					isRental: true,
					label: `LETTINGS_PAGE`,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.warn(`No pages to scrape with current arguments.`);
		return;
	}

	logger.step(`Queueing ${allRequests.length} map pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Guild Property - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);

	// Enhanced Remove-Status Strategy
	if (startPage === 1) {
		logger.step(`Performing cleanup of removed properties...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step(`Partial run detected (startPage: ${startPage}). Skipping remove status update.`);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeGuildProperty();
		logger.step(`All done!`);
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error:`, err);
		process.exit(1);
	}
})();
