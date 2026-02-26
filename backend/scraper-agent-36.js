// Winkworth scraper using Playwright with Crawlee
// Agent ID: 36
//
// Usage:
// node backend/scraper-agent-36.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized, } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty, } = require("./lib/property-helpers.js");

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

// function parsePrice(priceText) {
// 	if (!priceText) return null;
// 		if (priceText.toUpperCase().includes("POA")) return null;
// 	// Extract first price value (e.g., "£7,400 per week" -> "7400")

// 	const match = priceText.match(/£([\d,]+)/);
// 	if (!match) return null;

// 	const priceClean = match[1].replace(/,/g, "");
// 	if (!priceClean) return null;

// 	// Return formatted as string with commas
// 	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
// }

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
		const price = formatPriceUk(priceEl.text().trim());

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
	await sleep(700);

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
		console.error(`❌ Detail scrape error ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
// 	if (properties.length === 0) {
// 	console.log("⚠ No properties found, stopping pagination.");
// 	return;
// }
	const { pageNum, isRental, label } = request.userData;
	console.log(`📋 [${label}] Page ${pageNum} - ${request.url}`);

	// Wait for results to load
	await page.waitForSelector(".search-results-list__inner", { timeout: 30000 }).catch(() => {});
	await page.waitForTimeout(1000);

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);


	console.log(`🔗 Page ${pageNum}: ${properties.length} properties`);

	// Process each property
	for (const property of properties) {

	if (!property.link) continue;

	// Skip sold
	if (isSoldProperty(property.title || "")) continue;

	const price = formatPriceUk(property.price);
	if (!price) continue;

	const result = await updatePriceByPropertyURLOptimized(
		property.link,
		price,
		property.title,
		property.bedrooms,
		AGENT_ID,
		isRental
	);

	if (result.updated) {
		stats.totalSaved++;
	}

	if (!result.isExisting && !result.error) {
		console.log(`🆕 Scraping detail: ${property.title}`);

		await scrapePropertyDetail(page.context(), {
			...property,
			price
		}, isRental);
	}

	await sleep(400); // prevent rate limiting
}

	// Pagination - Winkworth uses ?page=N
	const $ = cheerio.load(htmlContent);
	const nextButton = $(".pagination__item--next");
	if (nextButton.length > 0) {
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
		maxConcurrency: 3,
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

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const totalSalesPages = 440; // ~8,707 properties / 20 per page
	const totalLettingsPages = 100; // Roughly 2,000 properties

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let p = Math.max(1, startPage); p <= totalSalesPages; p++) {
		allRequests.push({
			url: `https://www.winkworth.co.uk/london/london/properties-for-sale?statusunderoffer=false&propertytype=all${
				p > 1 ? `&page=${p}` : ""
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
				url: `https://www.winkworth.co.uk/london/london/properties-to-let?statusunderoffer=false&propertytype=all${
					p > 1 ? `&page=${p}` : ""
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
