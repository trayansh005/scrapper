// Sequence Home scraper using Hybrid API/Browser approach
// Agent ID: 15
// Usage:
// node backend/scraper-agent-15.js

const { PlaywrightCrawler } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

const AGENT_ID = 15;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
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

function formatPriceDisplay(price, isRental) {
	if (!price) return isRental ? "£0 pcm" : "£0";
	return `£${price}${isRental ? " pcm" : ""}`;
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
// API FETCHING (via Browser Context)
// ============================================================================

async function fetchFromApi(page, channel, pageNum) {
	const result = await page.evaluate(
		async ({ channel, pageNum }) => {
			const fragment = pageNum > 1 ? `/page-${pageNum}` : "";
			const apiUrl = `https://www.sequencehome.co.uk/search.ljson?channel=${channel}&fragment=${encodeURIComponent(fragment)}`;

			try {
				const response = await fetch(apiUrl, {
					headers: {
						Accept: "application/json",
						"X-Requested-With": "XMLHttpRequest",
					},
				});

				if (!response.ok) return null;
				return await response.json();
			} catch (e) {
				return null;
			}
		},
		{ channel, pageNum },
	);

	return result;
}

// ============================================================================
// PROPERTY PROCESSING
// ============================================================================

async function processProperties(properties, pageNum, label, isRental, totalPages) {
	logger.page(pageNum, label, `Processing ${properties.length} properties from API`);

	let createdInBatch = 0;

	for (const prop of properties) {
		const rawLink = prop.property_url || null;
		if (!rawLink) continue;

		const link = rawLink.startsWith("http")
			? rawLink
			: `https://www.sequencehome.co.uk${rawLink}`;

		const title = prop.display_address || prop.short_description || "Property";
		const price = Number.isFinite(prop.price_value) ? prop.price_value : null;

		if (isSoldProperty(prop.status || "")) {
			logger.property(
				pageNum,
				label,
				title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				link,
				isRental,
				totalPages,
				"SKIPPED",
			);
			continue;
		}

		if (processedUrls.has(link)) {
			continue;
		}
		processedUrls.add(link);

		const bedrooms = Number.isFinite(prop.bedrooms) ? prop.bedrooms : null;
		const latitude = Number.isFinite(prop.lat) ? prop.lat : null;
		const longitude = Number.isFinite(prop.lng) ? prop.lng : null;

		if (!price) {
			logger.property(
				pageNum,
				label,
				title.substring(0, 40),
				"No Price",
				link,
				isRental,
				totalPages,
				"SKIPPED",
			);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			link,
			price,
			title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "UNCHANGED";
		if (result.updated) {
			stats.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			await processPropertyWithCoordinates(
				link.trim(),
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
				null,
				latitude,
				longitude,
			);

			stats.totalSaved++;
			stats.totalScraped++;
			if (isRental) stats.savedRentals++;
			else stats.savedSales++;
			
			propertyAction = "CREATED";
			createdInBatch++;
			
			// Rule 5: Conditional Loop Sleep - only sleep if property was actually CREATED
			await sleep(2000); 
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
			latitude,
			longitude,
		);
	}
	
	return createdInBatch;
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleRequest({ page, request }) {
	const { channel, pageNum, label, isRental } = request.userData;

	// Initial wait to ensure page is loaded/Cloudflare handled
	await page.waitForTimeout(2000);

	let currentPage = pageNum;

	while (true) {
		const currentLabel = `${label}_PAGE_${currentPage}`;
		logger.page(currentPage, currentLabel, `Fetching and processing page ${currentPage}...`);

		const data = await fetchFromApi(page, channel, currentPage);

		if (!data || !Array.isArray(data.properties) || data.properties.length === 0) {
			logger.page(currentPage, currentLabel, `No more properties found for ${label}. Ending pagination.`);
			break;
		}

		// Try to extract total pages for better logging if available
		const totalPages = data.total_pages || 0;

		await processProperties(data.properties, currentPage, currentLabel, isRental, totalPages);

		currentPage++;
		// Rule 5: Skip the sleep between pages if we are efficiently skipping unchanged/updated records
		// But keep a minimal safety pulse if we hit too many pages at once
		if (currentPage % 5 === 0) {
			await sleep(500);
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
		requestHandlerTimeoutSecs: 600000, // Increased to 1 hour to handle many pages in a single loop
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleRequest,
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeSequenceHome() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Sequence Home scraper (Hybrid version) at ${scrapeStartTime.toISOString()}...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;

	if (isPartialRun) {
		logger.step(
			`CRITICAL: Partial run detected (startPage: ${startPage}). Automatic cleanup will be disabled.`,
		);
	}

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const CHANNELS = [
		{ name: "sales", label: "SALES", isRental: false, url: "https://www.sequencehome.co.uk/properties/sales/" },
		{ name: "lettings", label: "LETTINGS", isRental: true, url: "https://www.sequencehome.co.uk/properties/lettings/" },
	];

	const requests = CHANNELS.map((channel) => ({
		url: channel.url,
		userData: {
			channel: channel.name,
			pageNum: startPage,
			label: channel.label,
			isRental: channel.isRental,
		},
	}));

	await crawler.addRequests(requests);
	await crawler.run();

	logger.step(
		`Completed Sequence Home - Total collected: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	return { scrapeStartTime, isPartialRun };
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		const { scrapeStartTime, isPartialRun } = await scrapeSequenceHome();

		if (!isPartialRun) {
			logger.step("Full run completed. Starting cleanup of stale properties...");
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
			logger.step("Cleanup finished successfully.");
		}

		logger.step("Summary of Scraper Run:");
		logger.step(`- Total Collected: ${stats.totalScraped}`);
		logger.step(`- Total Saved to DB: ${stats.totalSaved}`);
		logger.step(`- Sales Saved: ${stats.savedSales}`);
		logger.step(`- Rentals Saved: ${stats.savedRentals}`);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
