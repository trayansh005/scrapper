// Strutt & Parker scraper using Playwright with Crawlee
// Agent ID: 34
//
// Usage:
// node backend/scraper-agent-34.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 34;

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
	const priceMatch = priceText.match(/[£€]\s*([\d,]+)/);
	if (!priceMatch) return null;

	const priceClean = priceMatch[1].replace(/,/g, "");
	return priceClean;
}

function parsePropertyCard($, element) {
	try {
		const $card = $(element);

		// Extract link from anchor tag
		const linkEl = $card.find('a[data-element="property-list-item"]').length
			? $card.find('a[data-element="property-list-item"]')
			: $card.find("a");

		let href = linkEl.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.struttandparker.com" + href;

		// Extract title
		const titleEl = $card.find(".card__heading, .card__title, h3, .card__text-content");
		const title = titleEl.text().trim() || null;

		// Extract bedrooms
		const bedroomsEl = $card.find(".property-features__item--bed, .card__beds");
		let bedrooms = null;
		if (bedroomsEl.length) {
			const bedroomsText = bedroomsEl.text().trim();
			const bedroomsMatch = bedroomsText.match(/\d+/);
			if (bedroomsMatch) bedrooms = bedroomsMatch[0];
		}

		// Extract price
		const priceEl = $card.find(".card__price, .card__price-container .card__price");
		const price = parsePrice(priceEl.text().trim());

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

	$(".grid-columns--2 .grid-columns__item").each((index, element) => {
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

	// Strutt & Parker uses lazy loading / scrolling
	let previousHeightValue = 0;
	let currentHeightValue = await page.evaluate(() => document.body.scrollHeight);
	let scrollAttempts = 0;
	const maxScrollAttempts = 10;

	while (previousHeightValue !== currentHeightValue && scrollAttempts < maxScrollAttempts) {
		previousHeightValue = currentHeightValue;
		await page.evaluate(() => window.scrollBy(0, window.innerHeight));
		await page.waitForTimeout(1500);
		currentHeightValue = await page.evaluate(() => document.body.scrollHeight);
		scrollAttempts++;
	}

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

	// Pagination - Strutt & Parker uses &page=N
	if (pageNum < 2) {
		const nextUrl = request.url + "&page=" + (pageNum + 1);
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

async function scrapeStruttAndParker() {
	console.log(`\n🚀 Starting Strutt & Parker scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [
		{
			url: "https://www.struttandparker.com/properties/residential/for-sale/london?showstc=on",
			userData: {
				pageNum: 1,
				isRental: false,
				label: "SALES",
			},
		},
		{
			url: "https://www.struttandparker.com/properties/residential/to-rent/london?showstc=on",
			userData: {
				pageNum: 1,
				isRental: true,
				label: "LETTINGS",
			},
		},
	];

	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n✅ Completed Strutt & Parker - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeStruttAndParker();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
