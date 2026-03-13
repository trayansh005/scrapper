// Marriott Vernon scraper using Playwright with Crawlee
// Agent ID: 25
// Website: www.marriottvernon.com
// Usage:
// node backend/scraper-agent-25.js [startPage]

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
require("dotenv").config();

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 25;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedLettings: 0,
};

const processedUrls = new Set();
let browserlessConnectedLogged = false;

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePrice(priceText) {
	if (!priceText) return null;
	const priceMatch = priceText.match(/[0-9][0-9,\s]*/g);
	if (!priceMatch) return null;

	const priceClean = priceMatch.join("").replace(/[^0-9]/g, "");
	if (!priceClean) return null;

	// Return formatted as string with commas for UK style
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($, element) {
	try {
		const $card = $(element);

		// Get the link from .property-grid__image
		const linkEl = $card.find("a.property-grid__image");
		let href = linkEl.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.marriottvernon.com" + href;

		// Extract meta info
		const $meta = $card.find(".property-grid__meta");
		const title = $meta.find("h4").text().trim();
		const infoText = $meta.find("h5").text().trim(); // e.g. "6 Bed Detached house For Sale"

		// Extract bedrooms
		const bedroomsMatch = infoText.match(/(\d+)\s*Bed/i);
		const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

		// Extract price from h6
		const priceText = $meta.find("h6").text().trim();
		const price = parsePrice(priceText);

		if (link && price && title) {
			return {
				link,
				title,
				price,
				bedrooms,
			};
		}
		return null;
	} catch (error) {
		logger.error(`Error parsing card: ${error.message}`);
		return null;
	}
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const properties = [];

	$(".property-grid").each((index, element) => {
		const property = parsePropertyCard($, element);
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

// ===================================
// REQUEST HANDLER
// ===================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;
	logger.page(pageNum, label, request.url);

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	logger.page(pageNum, label, `Found ${properties.length} properties`);

	// Process each property
	for (const property of properties) {
		try {
			if (processedUrls.has(property.link)) {
				logger.property(
					pageNum,
					label,
					property.title.substring(0, 40),
					formatPriceDisplay(property.price, isRental),
					property.link,
					isRental,
					undefined,
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
				undefined,
				action,
				coords.latitude,
				coords.longitude,
			);

			if (action === "CREATED") {
				await sleep(1000);
			}
		} catch (error) {
			logger.error(`Error processing property ${property.link}: ${error.message}`);
		}
	}

	// Dynamic pagination: enqueue next page if properties were found
	if (properties.length > 0) {
		const nextPageNum = pageNum + 1;
		const nextUrl = getPageUrl(nextPageNum, isRental);

		await crawler.addRequests([
			{
				url: nextUrl,
				userData: {
					pageNum: nextPageNum,
					isRental,
					label,
				},
			},
		]);
	}

}

function getPageUrl(pageNum, isRental) {
	const type = isRental ? "letting" : "sale";
	const baseUrl = "https://www.marriottvernon.com/property-search/";
	const query = `?orderby=price_desc&instruction_type=${type}&address_keyword&min_bedrooms&minprice&maxprice&property_type&showstc=off`;

	if (pageNum === 1) {
		return baseUrl + query;
	} else {
		return `${baseUrl}page/${pageNum}/${query}`;
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 5,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 600,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
				if (!browserlessConnectedLogged) {
					const browser = page.context().browser();
					const version = await browser.version();
					const browserType = browser.browserType().name();
					const isRemote = !!process.env.BROWSERLESS_WS_ENDPOINT;

					// Definitive check: if we expect remote but didn't get one (or vice versa), log it clearly
					if (isRemote) {
						logger.step(`Browser info: ${browserType} v${version} (Remote expected)`);
					} else {
						logger.step(`Browser info: ${browserType} v${version} (Local expected)`);
					}

					browserlessConnectedLogged = true;
				}
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
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

async function scrapeMarriottVernon() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Marriott Vernon scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = process.env.BROWSERLESS_WS_ENDPOINT;

	if (browserWSEndpoint) {
		logger.step(`Connecting to remote browser at ${browserWSEndpoint}`);
	} else {
		logger.warn("No BROWSERLESS_WS_ENDPOINT found. Falling back to local browser.");
	}

	const crawler = createCrawler(browserWSEndpoint);

	const initialRequests = [];

	// Initial Sales request
	initialRequests.push({
		url: getPageUrl(startPage, false),
		userData: {
			pageNum: startPage,
			isRental: false,
			label: "SALES",
		},
	});

	// If startPage is 1, also start Lettings
	if (startPage === 1) {
		initialRequests.push({
			url: getPageUrl(1, true),
			userData: {
				pageNum: 1,
				isRental: true,
				label: "LETTINGS",
			},
		});
	}

	logger.step(`Queueing initial pages starting from page ${startPage}...`);
	await crawler.run(initialRequests);

	logger.step(
		`Completed Marriott Vernon - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
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
		await scrapeMarriottVernon();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
