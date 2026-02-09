// Fenn Wright scraper using Playwright with Crawlee
// Agent ID: 242
//
// Usage:
// node backend/scraper-agent-242.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 242;

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
	const match = priceText.match(/[£€]\s*([\d,]+)/);
	if (!match) return null;

	const priceClean = match[1].replace(/,/g, "");
	if (!priceClean) return null;

	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($, element) {
	try {
		const $item = $(element);
		const linkEl = $item.find("a.caption");
		const titleEl = $item.find("h3");
		const priceEl = $item.find(".price");
		const bedroomsEl = $item.find("figure");

		let bedrooms = null;
		if (bedroomsEl.length) {
			const match = bedroomsEl.text().match(/(\d+)/);
			if (match) bedrooms = match[1];
		}

		let price = parsePrice(priceEl.text().trim());
		let link = linkEl.attr("href");
		let title = titleEl.text().trim();

		if (link && title) {
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

	$(".info-item").each((index, element) => {
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

		const htmlContent = await detailPage.content();

		// Coordinate extraction
		const geo = await detailPage.evaluate(() => {
			const html = document.documentElement.innerHTML;
			const latMatch = html.match(/"latitude":\s*(-?\d+\.\d+)/i);
			const lonMatch = html.match(/"longitude":\s*(-?\d+\.\d+)/i);
			return {
				lat: latMatch ? parseFloat(latMatch[1]) : null,
				lon: lonMatch ? parseFloat(lonMatch[1]) : null,
			};
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			geo.lat,
			geo.lon
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
	const { isRental, label, pageNum } = request.userData;
	console.log(`📋 [${label}] Page ${pageNum} - ${request.url}`);

	await page.waitForTimeout(2000);
	await page.waitForSelector(".info-item", { timeout: 30000 }).catch(() => {});

	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

	for (const property of properties) {
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

		if (!result.isExisting && !result.error) {
			console.log(`🆕 Scraping detail for new property: ${property.title}`);
			await scrapePropertyDetail(page.context(), property, isRental);
		}
	}

	// Pagination
	const nextButton = await page.$("a.next.page-numbers");
	if (nextButton) {
		const nextUrl = await nextButton.getAttribute("href");
		if (nextUrl) {
			await crawler.addRequests([
				{
					url: nextUrl,
					userData: { isRental, label, pageNum: pageNum + 1 },
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

async function scrapeFennWright() {
	console.log(`\n🚀 Starting Fenn Wright scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const initialRequests = [
		{
			url: "https://www.fennwright.co.uk/property-search/?address_keyword=&radius=&property_type=&officeID=&minimum_price=&maximum_price=&minimum_bedrooms=0&department=residential-sales",
			userData: { isRental: false, label: "SALES", pageNum: 1 },
		},
		{
			url: "https://www.fennwright.co.uk/property-search/?address_keyword=&radius=&property_type=&officeID=&minimum_rent=&maximum_rent=&minimum_bedrooms=0&department=residential-lettings",
			userData: { isRental: true, label: "RENTALS", pageNum: 1 },
		},
	];

	await crawler.addRequests(initialRequests);
	await crawler.run();

	console.log(`\n✅ Finished - Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`);
}

(async () => {
	try {
		await scrapeFennWright();
		await updateRemoveStatus(AGENT_ID);
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err);
		process.exit(1);
	}
})();
