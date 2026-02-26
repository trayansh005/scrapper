// Jackie Quinn scraper using Playwright with Crawlee
// Agent ID: 8
// Usage:
// node backend/scraper-agent-8.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, markAllPropertiesRemovedForAgent } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const {
	extractCoordinatesFromHTML,
	isSoldProperty,
	extractBedroomsFromHTML,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 8;
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

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
}

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	await sleep(700);
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 90000,
		});
		await detailPage.waitForTimeout(800);
		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);
		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
			},
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// PARSING LOGIC (Listing Page)
// ============================================================================

// Listing page parsing now done in browser context (see handleListingPage)

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental } = request.userData;
	logger.page(pageNum, null, request.url);
	await page.waitForTimeout(2000);
	await page.waitForSelector(".propertyBox", { timeout: 30000 }).catch(() => {});
	// Parse listings in browser context for full DOM support
	const properties = await page.evaluate(() => {
		const results = [];
		const propertyBoxes = document.querySelectorAll(".propertyBox");
		for (const el of propertyBoxes) {
			const linkEl = el.querySelector("h2.searchProName a");
			if (!linkEl) continue;
			const rawHref = linkEl.getAttribute("href");
			if (!rawHref) continue;
			const link = rawHref.startsWith("http") ? rawHref : `https://www.jackiequinn.co.uk${rawHref}`;
			const title = linkEl.textContent.trim();
			const priceText = el.querySelector("h3 div")?.textContent.trim() || "";
			// Bedroom extraction and sold check will be done outside for helper reuse
			results.push({ link, title, priceText, allText: el.textContent });
		}
		return results;
	});
	logger.page(pageNum, null, `Found ${properties.length} properties`);
	for (const property of properties) {
		if (!property.link) continue;
		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);
		if (isSoldProperty(property.priceText)) continue;
		const price = formatPriceUk(property.priceText);
		if (!price) continue;
		const bedrooms = extractBedroomsFromHTML(property.allText);
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);
		let propertyAction = "SEEN";
		if (result.updated) {
			stats.totalSaved++;
			propertyAction = "UPDATED";
		}
		if (!result.isExisting && !result.error) {
			const detail = await scrapePropertyDetail(
				page.context(),
				{ ...property, price, bedrooms },
				isRental,
			);
			await updatePriceByPropertyURL(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				detail?.coords?.latitude || null,
				detail?.coords?.longitude || null,
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
			null,
			property.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			property.link,
			isRental,
			null,
			propertyAction,
		);
		await sleep(500);
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

async function scrapeJackieQuinn() {
	logger.step("Starting Jackie Quinn scraper...");
	await markAllPropertiesRemovedForAgent(AGENT_ID);
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const totalPages = 11;
	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);
	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];
	for (let pageNum = Math.max(1, startPage); pageNum <= totalPages; pageNum++) {
		allRequests.push({
			url: `https://www.jackiequinn.co.uk/search?category=1&listingtype=5&statusids=1%2C10%2C4%2C16%2C3&obc=Price&obd=Descending&page=${pageNum}`,
			userData: { pageNum, isRental: false },
		});
	}
	if (allRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}
	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.run(allRequests);
	logger.step(
		`Completed Jackie Quinn - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

(async () => {
	try {
		await scrapeJackieQuinn();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
