// Marriott Vernon scraper using Playwright with Crawlee
// Agent ID: 25
//
// Usage:
// node backend/scraper-agent-25.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 25;

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
	const priceMatch = priceText.match(/[0-9][0-9,\s]*/g);
	if (!priceMatch) return null;

	const priceClean = priceMatch.join("").replace(/[^0-9]/g, "");
	if (!priceClean) return null;

	// Return formatted as string with commas for UK style
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($, element) {
	try {
		const $card = $(element);

		// Get the link
		const linkEl = $card.find("a.cards--property");
		let href = linkEl.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.marriottvernon.com" + href;

		// Get title and price from h4 and h5
		const h4Text = $card.find("h4").text().trim();
		const h5Text = $card.find("h5").text().trim();

		// Extract bedrooms from h4 (e.g., "6 Beds House - Detached - For Sale")
		const bedroomsMatch = h4Text.match(/(\d+)\s*Bed/i);
		const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

		// Extract title and price from h5
		const lines = h5Text
			.split("\n")
			.map((l) => l.trim())
			.filter((l) => l);
		const title = lines[0] || null;

		// Extract and format price - keep only digits and commas
		const price = parsePrice(h5Text);

		if (link && price && title) {
			return {
				link,
				title,
				price,
				bedrooms,
			};
		}
		return null;
	} catch (error) {
		return null;
	}
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const properties = [];

	$(".col-xl-6.mb-4.property").each((index, element) => {
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

	// Marriott Vernon uses lazy loading / scrolling
	let previousHeightValue = 0;
	let currentHeightValue = await page.evaluate(() => document.body.scrollHeight);
	let scrollAttempts = 0;
	const maxScrollAttempts = 10;

	while (previousHeightValue !== currentHeightValue && scrollAttempts < maxScrollAttempts) {
		previousHeightValue = currentHeightValue;
		await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
		await page.waitForTimeout(2000);
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

	// Pagination
	const $ = cheerio.load(htmlContent);
	const nextLink = $('a.page-link[rel="next"]');
	if (nextLink.length > 0) {
		const nextUrl = nextLink.attr("href");
		if (nextUrl) {
			await crawler.addRequests([
				{
					url: nextUrl.startsWith("http") ? nextUrl : "https://www.marriottvernon.com" + nextUrl,
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

async function scrapeMarriottVernon() {
	console.log(`\n🚀 Starting Marriott Vernon scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const totalPages = 2; // Fixed as per AGENTS config in combined-scraper

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let p = Math.max(1, startPage); p <= totalPages; p++) {
		allRequests.push({
			url: `https://www.marriottvernon.com/search/?showstc=off&instruction_type=Sale&address_keyword=&minprice=&maxprice=&property_type=${
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
		allRequests.push({
			url: "https://www.marriottvernon.com/search/?showstc=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=",
			userData: {
				pageNum: 1,
				isRental: true,
				label: "LETTINGS",
			},
		});
	}

	if (allRequests.length === 0) {
		console.log("⚠️ No pages to scrape with current arguments.");
		return;
	}

	console.log(`📋 Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n✅ Completed Marriott Vernon - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeMarriottVernon();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
