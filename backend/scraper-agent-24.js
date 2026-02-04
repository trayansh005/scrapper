// Haboodle scraper using Playwright with Crawlee
// Agent ID: 24
//
// Usage:
// node backend/scraper-agent-24.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 24;

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
		const bedrooms = bedroomsEl.length ? bedroomsEl.text().trim() : null;

		return {
			link,
			title,
			price,
			bedrooms,
		};
	} catch (error) {
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
	await page.waitForTimeout(2000);

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

	// Pagination
	const $ = cheerio.load(htmlContent);
	const nextLink = $("a.next.page-numbers");
	if (nextLink.length > 0) {
		const nextUrl = nextLink.attr("href");
		if (nextUrl) {
			await crawler.addRequests([
				{
					url: nextUrl,
					userData: {
						pageNum: pageNum + 1,
						isRental,
						label,
					},
				},
			]);
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

async function scrapeHaboodle() {
	console.log(`\n🚀 Starting Haboodle scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [
		{
			url: "https://www.haboodle.co.uk/find-a-property/?department=residential-sales&address_keyword=&radius=&minimum_bedrooms=&maximum_rent=&maximum_price=",
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
		`\n✅ Completed Haboodle - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeHaboodle();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
