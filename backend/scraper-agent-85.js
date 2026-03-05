// Abode scraper using Playwright to bypass Cloudflare and fetch `.ljson` API
// Agent ID: 85
// Site: https://www.abodeweb.co.uk/properties/sales (Homeflow platform)
// Usage:
//   node backend/scraper-agent-85.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 85;
const BASE_URL = "https://www.abodeweb.co.uk";
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
		// Wait for CF challenge to clear and page to load
		await page.waitForTimeout(3000);

		// Wait for the Homeflow results container (confirms CF bypass)
		await page
			.waitForSelector("#homeflow-search-results, .results-page, .property-list", {
				timeout: 15000,
			})
			.catch(() => {});

		// Execute fetch INSIDE the browser context to access the .ljson API
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
			// Page may be beyond last page — treat as graceful end, not error
			logger.page(
				pageNum,
				label,
				`No data returned (page may exceed total) — skipping`,
				totalPages,
			);
			return;
		}

		const properties = data.properties || [];
		logger.page(pageNum, label, `Found ${properties.length} properties via JSON API`, totalPages);

		for (const prop of properties) {
			const relativeLink = prop.property_url; // Abode uses property_url, not url
			if (!relativeLink) continue;

			const link = relativeLink.startsWith("http") ? relativeLink : `${BASE_URL}${relativeLink}`;

			// Skip sold/SSTC/let/stc properties
			const statusUpper = (prop.status || "").toUpperCase();
			if (
				statusUpper === "SSTC" ||
				statusUpper === "SOLD" ||
				statusUpper === "LET" ||
				isSoldProperty(prop.status || "")
			) {
				logger.page(pageNum, label, `Skipped [${prop.status}]: ${prop.property_url}`, totalPages);
				continue;
			}

			// Price extraction — Homeflow .ljson fields vary by site
			const priceRaw =
				prop.price ||
				prop.priceValue ||
				prop.priceWithoutQualifier ||
				prop.price_value ||
				prop.display_price ||
				"";
			const price = parsePrice(priceRaw.toString());

			if (!price) {
				logger.page(pageNum, label, `Skipping (no price found): ${link}`, totalPages);
				continue;
			}

			const title = prop.display_address || prop.displayAddress || prop.address || "Property";
			const bedrooms = prop.bedrooms ? parseInt(prop.bedrooms, 10) : null;

			const lat = prop.lat ? parseFloat(prop.lat) : null;
			const lon = prop.lng ? parseFloat(prop.lng) : null;

			// Check if property exists / update price
			const result = await updatePriceByPropertyURLOptimized(
				link,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";
			if (result.updated) propertyAction = "UPDATED";

			if (!result.isExisting && !result.error) {
				propertyAction = "CREATED";
				// Insert new property with coordinates from API (no detail page needed)
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
			} else if (result.updated) {
				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (result.isExisting) {
				counts.totalScraped++;
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

			// Only delay on write operations (BASELINE_RULES: conditional sleep)
			if (propertyAction !== "UNCHANGED") {
				await sleep(500);
			}
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

async function scrapeAbode() {
	logger.step(`Starting Abode API scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	// Abode (abodeweb.co.uk) - sales only based on server-backup.js research
	// Estimate ~200 sales properties (~10 pages of 20)
	const PROPERTY_TYPES = [
		{
			urlPath: "properties/sales",
			totalRecords: 300, // Approximate; will stop early if fewer pages returned
			recordsPerPage: 20,
			isRental: false,
			label: "SALES",
		},
		{
			urlPath: "properties/lettings",
			totalRecords: 300,
			recordsPerPage: 20,
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
			// Homeflow URL pattern: page 1 = /properties/sales, page 2+ = /properties/sales/page-2
			const pageUrl =
				pg === 1 ? `${BASE_URL}/${type.urlPath}` : `${BASE_URL}/${type.urlPath}/page-${pg}`;

			// Homeflow .ljson API pattern
			const apiUrl =
				pg === 1
					? `${BASE_URL}/${type.urlPath}.ljson`
					: `${BASE_URL}/${type.urlPath}/page-${pg}.ljson`;

			allRequests.push({
				url: pageUrl,
				userData: {
					pageNum: pg,
					totalPages,
					isRental: type.isRental,
					label: type.label,
					apiUrl,
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
		`Completed Abode - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
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
		await scrapeAbode();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
