// Haboodle scraper using Playwright with Crawlee
// Agent ID: 24
// Website: www.haboodle.co.uk
// Usage:
// node backend/scraper-agent-24.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 24;
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

function parsePrice(priceText) {
	if (priceText.includes("SOLD STC")) {
		return null;
	}
	const priceMatch = priceText.match(/[0-9][0-9,\s]*/g);
	if (!priceMatch) return null;

	const priceClean = priceMatch.join("").replace(/[^0-9]/g, "");
	if (!priceClean) return null;

	// Return formated as string with commas for UK style
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($card) {
	try {
		const linkEl = $card.find("h3 a");
		let href = linkEl.attr("href");
		if (!href) return null;

		const title = linkEl.text().trim();
		const link = href.startsWith("http") ? href : "https://www.haboodle.co.uk" + href;

		const priceText = $card.find(".price").text().trim();
		const price = parsePrice(priceText);
		if (!price) return null;

		const bedroomsEl = $card.find(".room-bedrooms .room-count");
		let bedrooms = null;
		if (bedroomsEl.length) {
			const text = bedroomsEl.text().trim();
			const match = text.match(/\d+/);
			bedrooms = match ? match[0] : null;
		}

		return {
			link,
			title,
			price,
			bedrooms,
		};
	} catch (error) {
		logger.error(`Error parsing card: ${error.message}`);
		return null;
	}
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const properties = [];

	$("li.type-property").each((index, element) => {
		const $card = $(element);
		const property = parsePropertyCard($card);
		if (property) {
			properties.push(property);
		}
	});

	return properties;
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
			timeout: 30000,
		});

		// Get HTML content and extract coordinates
		const htmlContent = await detailPage.content();

		// Save property to database
		const dbResult = await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
		);

		stats.totalScraped++;
		stats.totalSaved++;
		if (isRental) stats.savedLettings++;
		else stats.savedSales++;

		return dbResult || { latitude: null, longitude: null };
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
		return { latitude: null, longitude: null };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	// Wait for results to load
	await page.waitForSelector("li.type-property", { timeout: 15000 }).catch(() => {});

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	// Process each property
	for (const property of properties) {
		if (processedUrls.has(property.link)) {
			logger.property(
				pageNum,
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

		// Update price in database (or insert minimal record if new)
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		let action = "UNCHANGED";
		let coords = { latitude: null, longitude: null };

		if (result.updated) {
			action = "UPDATED";
			stats.totalSaved++;
		}

		// If new property, scrape full details immediately
		if (!result.isExisting && !result.error) {
			action = "CREATED";
			coords = await scrapePropertyDetail(page.context(), property, isRental);
		} else if (result.error) {
			action = "ERROR";
		}

		logger.property(
			pageNum,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(property.price, isRental),
			property.link,
			isRental,
			totalPages,
			action,
			coords.latitude,
			coords.longitude,
		);

		if (action === "CREATED") {
			await sleep(1000);
		}
	}

	// Pagination (only if on initial load batch, crawler handles the queue)
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

async function scrapeHaboodle() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Haboodle scraper (Agent ${AGENT_ID})...`);

	const totalSalesPages = 10;
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv";
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let p = Math.max(1, startPage); p <= totalSalesPages; p++) {
		allRequests.push({
			url: `https://www.haboodle.co.uk/find-a-property/?department=residential-sales&address_keyword=&radius=&minimum_bedrooms=&maximum_rent=&maximum_price=${
				p > 1 ? `&paged=${p}` : ""
			}`,
			userData: {
				pageNum: p,
				totalPages: totalSalesPages,
				isRental: false,
				label: "SALES",
			},
		});
	}

	if (allRequests.length === 0) {
		logger.warn("No pages to scrape with current arguments.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Haboodle - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status for properties not seen in this run...");
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
		await scrapeHaboodle();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
