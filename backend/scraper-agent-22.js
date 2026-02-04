// Allsop scraper using Playwright with Crawlee
// Agent ID: 22
//
// Usage:
// node backend/scraper-agent-22.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 22;

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
	if (
		priceText.includes("Withdrawn") ||
		priceText.includes("Sold Prior") ||
		priceText.includes("Sold After")
	) {
		return null;
	}
	const priceMatch = priceText.match(/£([\d,]+)/);
	if (!priceMatch) return null;

	const priceClean = priceMatch[1].replace(/[^0-9]/g, "");
	if (!priceClean) return null;

	// Return formated as string with commas for UK style
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($card) {
	try {
		const locationEl = $card.find(".__location");
		let title = locationEl.text().trim();

		const priceText = $card.find(".__lot_price_grid").text().trim();
		const price = parsePrice(priceText);
		if (!price) return null;

		const description = $card.find(".__byline span").text().trim();
		const bedroomsMatch = description.match(/(\d+)\s*bed/i);
		const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

		const linkEl = $card.find(".__lot_image a");
		let href = linkEl.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.allsop.co.uk" + href;

		const titleAttr = linkEl.attr("title") || "";
		const lotMatch = titleAttr.match(/LOT\s*(\d+)/i);
		const lotNumber = lotMatch ? lotMatch[1] : null;

		if (lotNumber) {
			title = `LOT ${lotNumber} - ${title}`;
		}

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

	$(".col-sm-6").each((index, element) => {
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

		// Try to click Street View to get coords if possible
		try {
			const streetViewTab = await detailPage.$('a[data-tab="street"]');
			if (streetViewTab) {
				await streetViewTab.click();
				await detailPage.waitForTimeout(2000);
			}
		} catch (e) {}

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

async function handleListingPage({ page, request }) {
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

async function scrapeAllsop() {
	console.log(`\n🚀 Starting Allsop scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	const totalPages = 10;
	// Allsop seems to be mostly Sales (Auctions)
	for (let pg = 1; pg <= totalPages; pg++) {
		allRequests.push({
			url: `https://www.allsop.co.uk/property-search?auction_id=f76e435a-46a5-11f0-ba8f-0242ac110002&page=${pg}`,
			userData: {
				pageNum: pg,
				isRental: false,
				label: "SALES",
			},
		});
	}

	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n✅ Completed Allsop - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeAllsop();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
