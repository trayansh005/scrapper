// Beresfords scraper using Playwright with Crawlee
// Agent ID: 245
// Usage:
// node backend/scraper-agent-245.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const {
	processPropertyWithCoordinates,
	updatePriceByPropertyURLOptimized,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 245;
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
	const { pageNum, isRental, label } = request.userData;

	// Fetch JSON data using in-browser fetch to bypass Cloudflare if needed
	const apiUrl = isRental
		? `https://www.beresfords.co.uk/wp-json/properties/v1/search?page=${pageNum}&per_page=12&radius=0&marketing_mode=letting&sort_by=price_high&units=miles`
		: `https://www.beresfords.co.uk/wp-json/properties/v1/search?page=${pageNum}&per_page=12&radius=0&sort_by=price_high&units=miles`;

	const data = await page.evaluate(async (url) => {
		try {
			const res = await fetch(url);
			if (!res.ok) return { error: `HTTP ${res.status}` };
			return await res.json();
		} catch (e) {
			return { error: e.message };
		}
	}, apiUrl);

	if (!data || data.error) {
		logger.error(
			`Error fetching API for ${label} page ${pageNum}: ${data?.error || "Unknown error"}`,
		);
		return;
	}

	const totalPages = data.pages || 1;
	logger.page(pageNum, label, request.url, totalPages);

	const properties = data.results || [];
	logger.page(pageNum, label, `Found ${properties.length} properties (API)`, totalPages);

	for (const prop of properties) {
		try {
			const link = prop.link;
			if (!link) continue;

			if (processedUrls.has(link)) continue;
			processedUrls.add(link);

			const price = parseInt(prop.price) || 0;
			const bedrooms = parseInt(prop.beds) || null;
			const title = prop.title || "Property";
			const lat = parseFloat(prop.latitude) || null;
			const lng = parseFloat(prop.longitude) || null;

			// Check if property is sold/unavailable
			if (
				prop.status &&
				(prop.status.toLowerCase().includes("sold") ||
					prop.status.toLowerCase().includes("unavailable"))
			) {
				continue;
			}

			// 1. Try to update price first
			const result = await updatePriceByPropertyURLOptimized(
				link.trim(),
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

			// 2. If new property, save with coordinates from API
			if (!result.isExisting && !result.error) {
				await updatePriceByPropertyURL(
					link.trim(),
					price,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					lat,
					lng,
				);

				stats.totalSaved++;
				stats.totalScraped++;
				if (isRental) stats.savedRentals++;
				else stats.savedSales++;
				propertyAction = "CREATED";
			} else if (result.error) {
				propertyAction = "ERROR";
			}

			logger.property(
				pageNum,
				label,
				title,
				formatPriceDisplay(price, isRental),
				link,
				isRental,
				totalPages,
				propertyAction,
				lat,
				lng,
			);

			if (propertyAction === "CREATED") {
				await sleep(500);
			}
		} catch (err) {
			logger.error(`Error processing property ${prop.link || "unknown"}: ${err.message}`);
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
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeBeresfords() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Beresfords scraper (API Mode - Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	// The problem is that PlaywrightCrawler's requestHandler might be set before we wrap it
	// or the wrapping logic is not being reached because it's after crawler initialization.
	// Actually we are setting crawler.requestHandler = dynamicHandler below.

	// Initial requests for first pages
	const initialRequests = [];

	if (startPage === 1) {
		initialRequests.push({
			url: "https://www.beresfords.co.uk/find-a-property/for-sale/",
			userData: { pageNum: 1, isRental: false, label: "SALES", processedDynamic: false },
		});
		initialRequests.push({
			url: "https://www.beresfords.co.uk/find-a-property/to-rent/",
			userData: { pageNum: 1, isRental: true, label: "LETTINGS", processedDynamic: false },
		});
	} else {
		initialRequests.push({
			url: `https://www.beresfords.co.uk/find-a-property/for-sale/page/${startPage}/`,
			userData: { pageNum: startPage, isRental: false, label: "SALES", processedDynamic: false },
		});
	}

	// Wrapper to handle dynamic pagination
	const dynamicHandler = async (context) => {
		const { request, crawler: crawlerInstance } = context;
		const { pageNum, isRental, label, processedDynamic } = request.userData;

		// 1. Process the current page
		await handleListingPage(context);

		// 2. If it's the first page we hit for this category, queue the rest
		if (processedDynamic === false) {
			const apiUrl = isRental
				? `https://www.beresfords.co.uk/wp-json/properties/v1/search?page=${pageNum}&per_page=12&radius=0&marketing_mode=letting&sort_by=price_high&units=miles`
				: `https://www.beresfords.co.uk/wp-json/properties/v1/search?page=${pageNum}&per_page=12&radius=0&sort_by=price_high&units=miles`;

			const data = await context.page.evaluate(async (url) => {
				try {
					const res = await fetch(url);
					return await res.json();
				} catch (e) {
					return null;
				}
			}, apiUrl);

			if (data && data.pages && data.pages > pageNum) {
				const totalPages = data.pages;
				const moreRequests = [];
				for (let p = pageNum + 1; p <= totalPages; p++) {
					moreRequests.push({
						url: isRental
							? `https://www.beresfords.co.uk/find-a-property/to-rent/page/${p}/`
							: `https://www.beresfords.co.uk/find-a-property/for-sale/page/${p}/`,
						userData: {
							pageNum: p,
							isRental,
							label,
							processedDynamic: true, // Mark as queued so we don't repeat this
						},
					});
				}
				if (moreRequests.length > 0) {
					logger.step(
						`Queueing ${moreRequests.length} more ${label} pages (Total: ${totalPages})...`,
					);
					await crawlerInstance.addRequests(moreRequests);
				}
			}
		}
	};

	// Create a new crawler instance with the dynamic handler directly
	const browserWSEndpointDynamic = getBrowserlessEndpoint();
	const finalCrawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint: browserWSEndpointDynamic,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: dynamicHandler,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});

	await finalCrawler.run(initialRequests);

	logger.step(
		`Completed Beresfords - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);

	if (startPage === 1) {
		logger.step(`Updating remove status for Agent ${AGENT_ID}...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step("Partial run detected, skipping updateRemoveStatus.");
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeBeresfords();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.message || err}`);
		process.exit(1);
	}
})();
