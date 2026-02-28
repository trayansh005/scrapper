// Bairstow Eves scraper using Playwright to bypass Cloudflare and fetch `.ljson` API
// Agent ID: 13
// Usage:
// node backend/scraper-agent-13.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 13;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
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

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages, apiUrl } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		// Wait for CF challenge to clear and normal page to load
		await page.waitForTimeout(3000);

		// Attempt to wait for the main React container to ensure CF is bypassed
		await page.waitForSelector("#homeflow-search-results", { timeout: 15000 }).catch(() => {});

		// Now execute a fetch INSIDE the browser context to get the .ljson!
		// Because we're inside the browser, Cloudflare cookies are automatically attached.
		const data = await page.evaluate(async (url) => {
			try {
				const response = await fetch(url, {
					headers: {
						accept: "application/json, text/javascript, */*; q=0.01",
						"x-requested-with": "XMLHttpRequest",
					},
				});
				if (!response.ok) return null;
				return await response.json();
			} catch (err) {
				return null;
			}
		}, apiUrl);

		if (!data || !data.properties) {
			logger.error(`Failed to fetch JSON API data inside browser on page ${pageNum} for ${label}.`);
			return;
		}

		const properties = data.properties || [];
		logger.page(pageNum, label, `Found ${properties.length} properties via JSON API`, totalPages);

		for (const prop of properties) {
			const relativeLink = prop.url;
			if (!relativeLink) continue;

			const link = relativeLink.startsWith("http")
				? relativeLink
				: `https://www.bairstoweves.co.uk${relativeLink}`;

			// Basic skipping
			if (isSoldProperty(prop.status || "")) continue;

			// Price extraction
			const priceText = prop.priceValue || prop.price || "";
			const price = parsePrice(priceText.toString());

			if (!price) continue;

			const title = prop.displayAddress || prop.addressWithCommas || "Property";
			const bedrooms = prop.bedrooms ? parseInt(prop.bedrooms, 10) : null;

			const lat = prop.lat ? parseFloat(prop.lat) : null;
			const lon = prop.lng ? parseFloat(prop.lng) : null;

			// Check if property exists first
			const result = await updatePriceByPropertyURLOptimized(
				link,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			if (result.updated) {
				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (result.isExisting) {
				counts.totalScraped++;
			}

			let propertyAction = "UNCHANGED";
			if (result.updated) propertyAction = "UPDATED";

			if (!result.isExisting && !result.error) {
				propertyAction = "CREATED";
				// Insert new property with coordinates
				await processPropertyWithCoordinates(
					link,
					price,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					null, // HTML config not needed
					lat,
					lon,
				);
				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			}

			logger.property(
				pageNum,
				label,
				title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				link,
				isRental,
				totalPages,
				propertyAction,
			);

			await sleep(500); // DB politeness delay
		}
	} catch (error) {
		logger.error(`Error processing page ${pageNum} for ${label}`, error);
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

async function scrapeBairstowEves() {
	logger.step(`Starting Bairstow Eves API scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const PROPERTY_TYPES = [
		{
			urlPath: "properties/sales/status-available/most-recent-first",
			totalRecords: 2825,
			recordsPerPage: 50,
			isRental: false,
			label: "SALES",
		},
		{
			urlPath: "properties/lettings/status-available/most-recent-first",
			totalRecords: 634,
			recordsPerPage: 50,
			isRental: true,
			label: "LETTINGS",
		},
	];

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
		const effectiveStartPage = Math.max(1, startPage);

		for (let pg = effectiveStartPage; pg <= totalPages; pg++) {
			allRequests.push({
				url: `https://www.bairstoweves.co.uk/${type.urlPath}/page-${pg}/#/`,
				userData: {
					pageNum: pg,
					totalPages,
					isRental: type.isRental,
					label: `${type.label}`,
					apiUrl: `https://www.bairstoweves.co.uk/${type.urlPath}.ljson?page=${pg}`,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Bairstow Eves - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeBairstowEves();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
