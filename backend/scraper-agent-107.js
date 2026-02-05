// Belvoir scraper using Playwright with Crawlee
// Agent ID: 107
//
// Usage:
// node backend/scraper-agent-107.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 107;

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
	// Extract digits and format with commas
	const digits = priceText.replace(/[^0-9]/g, "");
	if (!digits) return null;

	return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
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
		console.error(` Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(` [${label}] Page ${pageNum} - ${request.url}`);

	// Wait for results to load
	await page.waitForTimeout(2000);
	await page.waitForSelector(".tease-property", { timeout: 30000 }).catch(() => {
		console.log(` No properties found on page ${pageNum}`);
	});

	// Extract all properties from the page
	const properties = await page.$$eval(".tease-property", (elements) => {
		return elements
			.map((element) => {
				try {
					const linkEl = element.querySelector(".text-link");
					let link = linkEl ? linkEl.getAttribute("href") : null;
					if (link && !link.startsWith("http")) {
						link = "https://www.belvoir.co.uk" + link;
					}

					const addr1 = element.querySelector(".addr1")?.textContent || "";
					const addr2 = element.querySelector(".addr2")?.textContent || "";
					const title = [addr1, addr2]
						.map((t) => t.replace(/\s+/g, " ").trim())
						.filter(Boolean)
						.join(", ");

					const bedroomsText =
						element.querySelector(".bedroom-icon")?.nextElementSibling?.textContent?.trim() || "";
					const bedroomsMatch = bedroomsText.match(/\d+/);
					const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

					const priceText = element.querySelector(".amount")?.textContent?.trim() || "";
					const digits = priceText.replace(/[^0-9]/g, "");
					const price = digits ? digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",") : null;

					if (link && title && price) {
						return { link, title, price, bedrooms };
					}
					return null;
				} catch (err) {
					return null;
				}
			})
			.filter(Boolean);
	});

	console.log(` Found ${properties.length} properties on page ${pageNum}`);

	// Process each property
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
			console.log(` Scraping detail for new property: ${property.title}`);
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
			console.error(` Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeBelvoir() {
	console.log(`\n Starting Belvoir scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	// Config
	const totalSalesPages = 334; // 3672 / 11
	const totalRentalsPages = 118; // 1300 / 11

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let p = Math.max(1, startPage); p <= totalSalesPages; p++) {
		allRequests.push({
			url: `https://www.belvoir.co.uk/properties/for-sale/?per_page=11&pg=${p}`,
			userData: {
				pageNum: p,
				isRental: false,
				label: `SALES_PAGE_${p}`,
			},
		});
	}

	// Build Rentals requests
	if (startPage === 1) {
		for (let p = 1; p <= totalRentalsPages; p++) {
			allRequests.push({
				url: `https://www.belvoir.co.uk/properties/for-rent/?per_page=11&pg=${p}`,
				userData: {
					pageNum: p,
					isRental: true,
					label: `RENTALS_PAGE_${p}`,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		console.log(" No pages to scrape with current arguments.");
		return;
	}

	console.log(` Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n Completed Belvoir - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeBelvoir();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
