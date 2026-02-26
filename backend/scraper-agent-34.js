// Strutt & Parker scraper using Playwright with Crawlee
// Agent ID: 34
//
// Usage:
// node backend/scraper-agent-34.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized, } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty, } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 34;

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

function parsePrice(priceText) {
	if (!priceText) return null;
	const priceMatch = priceText.match(/[£€]\s*([\d,]+)/);
	if (!priceMatch) return null;

	const priceClean = priceMatch[1].replace(/,/g, "");
	if (!priceClean) return null;

	// Return formatted as string with commas
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
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
		const priceRaw = priceEl.text().trim();
		const price = formatPriceUk(priceRaw);

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
	await sleep(800);

	const detailPage = await browserContext.newPage();

	try {
		await detailPage.route("**/*", (route) => {
			const type = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(type)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		await detailPage.waitForTimeout(1200);

		const htmlContent = await detailPage.content();

		const coords = await extractCoordinatesFromHTML(htmlContent);

		await updatePriceByPropertyURL(
			property.link.trim(),
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			coords?.latitude || null,
			coords?.longitude || null
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
		// Skip duplicate URLs
		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		// Skip sold properties
		if (isSoldProperty(property.title || "")) continue;

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
		await sleep(500);
	}

	// Pagination - Strutt & Parker uses &page=N
	// if (pageNum < 2) {
	// 	const nextUrl = request.url + "&page=" + (pageNum + 1);
	// 	await crawler.addRequests([
	// 		{
	// 			url: nextUrl,
	// 			userData: {
	// 				pageNum: pageNum + 1,
	// 				isRental,
	// 				label,
	// 			},
	// 		},
	// 	]);
	// }
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

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const totalSalesPages = 5; // Default for London Sales
	const totalLettingsPages = 2;

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let p = Math.max(1, startPage); p <= totalSalesPages; p++) {
		allRequests.push({
			url: `https://www.struttandparker.com/properties/residential/for-sale/london?showstc=on${p > 1 ? `&page=${p}` : ""
				}`,
			userData: {
				pageNum: p,
				isRental: false,
				label: `SALES_PAGE_${p}`,
			},
		});
	}

	// Build Lettings requests (standard page 1 only if startPage is 1)
	if (startPage === 1) {
		for (let p = 1; p <= totalLettingsPages; p++) {
			allRequests.push({
				url: `https://www.struttandparker.com/properties/residential/to-rent/london?showstc=on${p > 1 ? `&page=${p}` : ""
					}`,
				userData: {
					pageNum: p,
					isRental: true,
					label: `LETTINGS_PAGE_${p}`,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		console.log("⚠️ No pages to scrape with current arguments.");
		return;
	}

	console.log(`📋 Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
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
