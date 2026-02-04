// Winkworth scraper using Playwright with Crawlee
// Agent ID: 36
//
// Usage:
// node backend/scraper-agent-36.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 36;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePrice(priceText) {
	if (!priceText) return null;
	// Extract first price value (e.g., "£7,400 per week" -> "7400")
	const match = priceText.match(/£([\d,]+)/);
	if (!match) return null;

	const priceClean = match[1].replace(/,/g, "");
	if (!priceClean) return null;

	// Return formatted as string with commas
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($, element) {
	try {
		const $card = $(element);

		// Get title and link
		const titleLink = $card.find(".search-result-property__content-card-link");
		if (!titleLink.length) return null;

		let href = titleLink.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.winkworth.co.uk" + href;

		// Get title
		const titleEl = $card.find(".search-result-property__title");
		const title = titleEl.text().trim() || "Unknown";

		// Get price
		const priceEl = $card.find(".search-result-property__price");
		const price = parsePrice(priceEl.text().trim());

		// Get bedrooms
		let bedrooms = null;
		const bedroomsSpecs = $card.find(".specs__item");
		if (bedroomsSpecs.length > 0) {
			// First spec is typically bedrooms
			const bedsText = $(bedroomsSpecs[0]).find(".specs__text");
			if (bedsText.length) {
				const match = bedsText.text().trim().match(/\d+/);
				bedrooms = match ? match[0] : null;
			}
		}

		if (link && title && price) {
			return { link, title, price, bedrooms };
		}
		return null;
	} catch (error) {
		return null;
	}
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const properties = [];

	$("article.search-result-property").each((index, element) => {
		const property = parsePropertyCard($, element);
		if (property) {
			properties.push(property);
		}
	});

	return properties;
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
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		// Block unnecessary resources
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

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
	} catch (error) {
		console.error(`❌ Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(`📋 [${label}] Page ${pageNum} - ${request.url}`);

	// Wait for results to load
	await page.waitForSelector(".search-results-list__inner", { timeout: 30000 }).catch(() => {});
	await page.waitForTimeout(1000);

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

	// Process each property
	for (const property of properties) {
		// Update price in database (or insert minimal record if new)
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			stats.totalSaved++;
		}

		// If new property, scrape full details immediately
		if (!result.isExisting && !result.error) {
			console.log(`🆕 Scraping detail for new property: ${property.title}`);
			await scrapePropertyDetail(page.context(), property, isRental);
		}
	}

	// Pagination - Winkworth uses ?page=N
	const $ = cheerio.load(htmlContent);
	const nextButton = $(".pagination__item--next");
	if (nextButton.length > 0 && pageNum < 10) {
		const currentUrl = new URL(request.url);
		currentUrl.searchParams.set("page", pageNum + 1);

		await crawler.addRequests([
			{
				url: currentUrl.toString(),
				userData: {
					pageNum: pageNum + 1,
					isRental,
					label,
				},
			},
		]);
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
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(`❌ Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeWinkworth() {
	console.log(`\n🚀 Starting Winkworth scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [
		{
			url: "https://www.winkworth.co.uk/london/london/properties-to-let?statusunderoffer=false&propertytype=all",
			userData: {
				pageNum: 1,
				isRental: true,
				label: "LETTINGS",
			},
		},
		{
			url: "https://www.winkworth.co.uk/london/london/properties-for-sale?statusunderoffer=false&propertytype=all",
			userData: {
				pageNum: 1,
				isRental: false,
				label: "SALES",
			},
		},
	];

	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n✅ Completed Winkworth - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeWinkworth();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
