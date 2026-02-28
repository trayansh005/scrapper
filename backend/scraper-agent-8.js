// Jackie Quinn scraper using Playwright with Crawlee
// Agent ID: 8
// Usage:
// node backend/scraper-agent-8.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, extractBedroomsFromHTML } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 8;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
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
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
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
			timeout: 60000,
		});

		// Trigger map click if needed, or wait for content
		await detailPage
			.evaluate(() => {
				const mapLink = document.querySelector('a[href*="mapcontainer"]');
				if (mapLink) mapLink.click();
			})
			.catch(() => {});

		await detailPage.waitForTimeout(2000); // Wait for potential map load/transition

		const htmlContent = await detailPage.content();

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
// PARSING LOGIC (Listing Page)
// ============================================================================

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const results = [];

	$(".propertyBox").each((_, el) => {
		const $item = $(el);

		const $linkEl = $item.find("h2.searchProName a");
		const rawHref = $linkEl.attr("href");
		if (!rawHref) return;

		const link = rawHref.startsWith("http") ? rawHref : `https://www.jackiequinn.co.uk${rawHref}`;
		const title = $linkEl.text().trim();

		const priceText = $item.find("h3 div").text().trim();
		if (isSoldProperty(priceText)) return;

		const price = formatPriceUk(priceText);
		if (!price) return;

		const description = $item.find(".featuredDescriptions").text().trim();
		const allText = $item.text();

		// Use centralized helper for bedroom extraction
		const bedrooms = extractBedroomsFromHTML(allText);

		results.push({ link, title, price, bedrooms });
	});

	return results;
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental } = request.userData;
	console.log(`📋 Page ${pageNum}/13 - ${request.url}`);

	await page.waitForTimeout(2000);
	await page.waitForSelector(".propertyBox", { timeout: 30000 }).catch(() => {});

	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

	for (const property of properties) {
		if (!property.link) continue;
		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "UNCHANGED";
		if (result.updated) propertyAction = "UPDATED";
		if (!result.isExisting && !result.error) propertyAction = "CREATED";

		console.log(
			`✅ [${propertyAction}] ${property.title.substring(0, 40)} - ${formatPriceDisplay(
				property.price,
				isRental,
			)} - ${property.link}`,
		);

		if (propertyAction !== "UNCHANGED") {
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
			console.error(`❌ Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeJackieQuinn() {
	console.log(`\n🚀 Starting Jackie Quinn scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const requests = [];
	for (let pageNum = 1; pageNum <= 11; pageNum++) {
		requests.push({
			url: `https://www.jackiequinn.co.uk/search?category=1&listingtype=5&statusids=1%2C10%2C4%2C16%2C3&obc=Price&obd=Descending&page=${pageNum}`,
			userData: { pageNum, isRental: false },
		});
	}

	await crawler.addRequests(requests);
	await crawler.run();

	console.log(
		`\n✅ Completed Jackie Quinn - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

(async () => {
	try {
		await scrapeJackieQuinn();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
