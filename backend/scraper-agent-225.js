// Taylforths scraper using Playwright with Crawlee
// Agent ID: 225
// Website: taylforths.co.uk
// Usage:
// node backend/scraper-agent-225.js [startPage]

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

const AGENT_ID = 225;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

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
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 40000,
		});
		const htmlContent = await detailPage.content();

		await processPropertyWithCoordinates(
			property.link.trim(),
			property.priceNum,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			htmlContent,
		);
	} catch (err) {
		logger.error(`Error scraping detail page ${property.link}`, err);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// CRAWLER CONFIGURATION
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
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForTimeout(2000);
		await page.waitForSelector("li.type-property", { timeout: 20000 }).catch(() => {
			logger.warn(`No listing container found on page ${pageNum}`);
		});

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);
		counts.totalFound += properties.length;

		for (const property of properties) {
			if (!property.link || processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			counts.totalScraped++;

			// Skip sold properties
			if (isSoldProperty(property.statusText)) {
				counts.totalSkipped++;
				logger.property(
					property.title.substring(0, 40),
					formatPriceDisplay(null, isRental),
					property.link,
					isRental ? "RENTALS" : "SALES",
					"SKIPPED",
				);
				continue;
			}

			const priceNum = parsePrice(property.rawPrice);
			if (priceNum === null) {
				counts.totalSkipped++;
				continue;
			}

			const result = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				priceNum,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let action = "UNCHANGED";
			if (result.updated) {
				action = "UPDATED";
				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (!result.isExisting && !result.error) {
				action = "CREATED";
				await scrapePropertyDetail(
					page.context(),
					{
						...property,
						priceNum,
					},
					isRental,
				);
				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (result.error) {
				action = "ERROR";
				counts.totalSkipped++;
			}

			logger.property(
				property.title.substring(0, 40),
				formatPriceDisplay(priceNum, isRental),
				property.link,
				isRental ? "RENTALS" : "SALES",
				action,
			);

			if (action !== "UNCHANGED") {
				await sleep(100);
			}
		}
	} catch (error) {
		logger.error(`Error processing page ${pageNum} for ${label}`, error);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		const scrapeStartTime = new Date();
		const startPage = getStartPage();
		const isPartialRun = startPage > 1;

		logger.step(`Starting Taylforths scraper (Agent ${AGENT_ID})`, `startPage=${startPage}`);

		const browserWSEndpoint = getBrowserlessEndpoint();
		logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

		const crawler = createCrawler(browserWSEndpoint);

		const PROPERTY_TYPES = [
			{
				baseUrl: "https://www.taylforths.co.uk/find-a-property/page/",
				params:
					"/?address_keyword&radius=20&minimum_bedrooms&maximum_rent&maximum_price&department=residential-sales",
				totalPages: 5,
				isRental: false,
				label: "SALES",
			},
			{
				baseUrl: "https://www.discoverpm.co.uk/find-a-property/page/",
				params: "",
				totalPages: 5,
				isRental: true,
				label: "RENTALS",
			},
		];

		const requests = [];
		for (const propertyType of PROPERTY_TYPES) {
			for (let pg = startPage; pg <= propertyType.totalPages; pg++) {
				requests.push({
					url: `${propertyType.baseUrl}${pg}${propertyType.params}`,
					userData: {
						pageNum: pg,
						isRental: propertyType.isRental,
						label: propertyType.label,
						totalPages: propertyType.totalPages,
					},
				});
			}
		}

		await crawler.addRequests(requests);
		await crawler.run();

		logger.step(
			`Completed Taylforths Agent ${AGENT_ID}`,
			`found=${counts.totalFound}, scraped=${counts.totalScraped}, saved=${counts.totalSaved}, skipped=${counts.totalSkipped}, sales=${counts.savedSales}, rentals=${counts.savedRentals}`,
		);

		if (!isPartialRun) {
			logger.step("Updating remove status...");
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		} else {
			logger.warn("Partial run detected. Skipping updateRemoveStatus.");
		}

		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
