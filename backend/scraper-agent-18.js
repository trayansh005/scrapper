// Moveli scraper using Playwright with Crawlee
// Agent ID: 18
// Website: www.moveli.co.uk
// Usage:
// node backend/scraper-agent-18.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const { isSoldProperty, formatPriceDisplay } = require("./lib/property-helpers.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 18;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedLettings: 0,
};

const processedUrls = new Set();

const SELECTORS = {
	PROPERTY_CARD: "a.property_card",
	PROPERTY_HEADING: "h4",
	PROPERTY_PRICE: "p.format_price",
	STATUS_LABEL: "p.status_label",
	BEDROOMS: "p.inline_text",
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePrice(priceText) {
	const priceMatch = priceText.match(/[0-9][0-9,\s]*/g);
	if (!priceMatch) return null;

	const priceClean = priceMatch.join("").replace(/[^0-9]/g, "");
	if (!priceClean) return null;

	// Return formatted as string with commas for UK style
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($card, $) {
	try {
		// Get link
		let href = $card.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.moveli.co.uk" + href;

		// Get title from h4
		const title = $card.find(SELECTORS.PROPERTY_HEADING).text().trim();
		if (!title) return null;

		// Get status
		const statusText = $card.find(SELECTORS.STATUS_LABEL).text().trim();
		if (isSoldProperty(statusText)) {
			return null;
		}

		// Get price
		const priceText = $card.find(SELECTORS.PROPERTY_PRICE).text().trim();
		const price = parsePrice(priceText);
		if (!price) return null;

		// Get bedrooms
		let bedrooms = null;
		$card.find(SELECTORS.BEDROOMS).each((i, el) => {
			const text = $(el).text().trim();
			if (/^\d+$/.test(text)) {
				bedrooms = text;
			}
		});

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

	$(SELECTORS.PROPERTY_CARD).each((index, element) => {
		const $card = $(element);
		const property = parsePropertyCard($card, $);
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
			timeout: 60000,
		});

		// Wait for dynamic content to load
		await detailPage.waitForTimeout(2000);

		// Get HTML content and extract coordinates
		const htmlContent = await detailPage.content();

		// Save property to database
		await processPropertyWithCoordinates(
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
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNumber, totalPages } = request.userData || {};
	logger.page(pageNumber || 1, label, request.url, totalPages || 1);

	// Wait for properties to load
	await page.waitForSelector(SELECTORS.PROPERTY_CARD, { timeout: 30000 }).catch(() => {
		logger.error(`No properties found on ${request.url}`);
	});

	// Wait for dynamic content
	await page.waitForTimeout(3000);

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	logger.page(pageNumber || 1, label, `Found ${properties.length} properties`, totalPages || 1);

	// Process each property
	for (const property of properties) {
		if (processedUrls.has(property.link)) {
			logger.property(
				pageNumber || 1,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(property.price, isRental),
				property.link,
				isRental,
				totalPages || 1,
				"SKIPPED: ALREADY PROCESSED",
			);
			continue;
		}
		processedUrls.add(property.link);

		// Update price in database
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		let action = "UNCHANGED";
		if (result.updated) action = "UPDATED";

		// If new property, scrape full details
		if (!result.isExisting && !result.error) {
			action = "CREATED";
			await scrapePropertyDetail(page.context(), property, isRental);
		} else if (result.error) {
			action = "ERROR";
		}

		logger.property(
			pageNumber || 1,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(property.price, isRental),
			property.link,
			isRental,
			totalPages || 1,
			action,
		);

		if (action !== "UNCHANGED") {
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

async function scrapeMoveli() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Moveli scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv";
	const crawler = createCrawler(browserWSEndpoint);

	const propertyTypes = [
		{
			url: "https://www.moveli.co.uk/test/properties?category=for-sale&searchKeywords=&status=For%20Sale&maxPrice=any&minBeds=any&sortOrder=price-desc",
			isRental: false,
			label: "SALES",
		},
		{
			url: "https://www.moveli.co.uk/test/properties?category=for-rent&searchKeywords=&status=For%20Rent&maxPrice=any&minBeds=any&sortOrder=price-desc",
			isRental: true,
			label: "LETTINGS",
		},
	];

	const requests = propertyTypes.map((pt) => ({
		url: pt.url,
		userData: {
			isRental: pt.isRental,
			label: pt.label,
			pageNumber: 1,
			totalPages: 1,
		},
	}));

	if (requests.length > 0) {
		logger.step(`Queueing ${requests.length} listing categories...`);
		await crawler.run(requests);
	}

	logger.step(
		`Completed Moveli - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
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
		await scrapeMoveli();
		logger.step("All done!");
		process.exit(0);
	} catch (error) {
		logger.error("Fatal error", error);
		process.exit(1);
	}
})();
