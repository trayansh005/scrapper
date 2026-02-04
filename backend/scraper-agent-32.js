// Remax scraper using Playwright with Crawlee
// Agent ID: 32
//
// Usage:
// node backend/scraper-agent-32.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 32;

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
	const m = priceText.match(/[0-9,.]+/);
	if (!m) return null;

	const num = m[0].replace(/,/g, "");
	return parseInt(num).toLocaleString();
}

function parsePropertyCard($, element) {
	try {
		const $card = $(element);

		// Get the detail link
		const linkEl = $card.find("a[href]").first();
		let href = linkEl.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://remax.co.uk" + href;
		const title = $card.find(".p-name a").text().trim();
		const rawPrice = $card.find(".f-price").text().trim();
		const price = parsePrice(rawPrice);

		if (!price) return null;

		// Extract bedrooms, bathrooms from property-attr
		const attrText = $card.find(".property-attr").text().trim();
		const bedroomsMatch = attrText.match(/(\d+)\s*Bed/);
		const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

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

	$("div[class*='grid'] .property-item").each((index, element) => {
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

	// Pagination - check for next link
	const $ = cheerio.load(htmlContent);
	const nextLink = $("a.page-link[aria-label='Next']");
	if (nextLink.length > 0) {
		const nextUrl = nextLink.attr("href");
		if (nextUrl) {
			const fullNextUrl = nextUrl.startsWith("http") ? nextUrl : "https://remax.co.uk" + nextUrl;
			await crawler.addRequests([
				{
					url: fullNextUrl,
					userData: {
						pageNum: pageNum + 1,
						isRental,
						label,
					},
				},
			]);
		}
	} else if (pageNum < 5) {
		// Fallback for some page types
		const currentUrl = request.url;
		let nextUrl;
		if (currentUrl.includes("?page=")) {
			nextUrl = currentUrl.replace(/page=\d+/, `page=${pageNum + 1}`);
		} else {
			nextUrl = currentUrl + (currentUrl.includes("?") ? "&" : "?") + `page=${pageNum + 1}`;
		}
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

async function scrapeRemax() {
	console.log(`\n🚀 Starting Remax scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const totalSalesPages = 5; // Default for Remax Sales
	const totalLettingsPages = 2; // Default for Remax Lettings

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let p = Math.max(1, startPage); p <= totalSalesPages; p++) {
		allRequests.push({
			url: `https://remax.co.uk/properties-for-sale/${p > 1 ? `page/${p}/` : ""}`,
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
				url: `https://remax.co.uk/properties-for-rent/${p > 1 ? `page/${p}/` : ""}`,
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
		`\n✅ Completed Remax - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeRemax();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
