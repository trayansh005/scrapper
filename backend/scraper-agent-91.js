// Estates East scraper using PlaywrightCrawler
// Agent ID: 91
// Site: https://www.estateseast.co.uk
// Usage:
//   node backend/scraper-agent-91.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 91;
const BASE_URL = "https://www.estateseast.co.uk";
const SALES_PAGES = 5; // Estates east is a local agency, 5 pages is safe
const RENTAL_PAGES = 3;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
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
	await sleep(1000); // Politeness delay

	const detailPage = await browserContext.newPage();

	try {
		await detailPage.setExtraHTTPHeaders({
			"Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
			"User-Agent":
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
		});

		await detailPage.goto(property.url, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		const htmlContent = await detailPage.content();
		const $ = cheerio.load(htmlContent);

		let bedrooms = property.bedrooms;
		if (!bedrooms) {
			// Try to find bedrooms in the detail page text
			const detailText = $("body").text();
			const bedMatch = detailText.match(/(\d+)\s*Bed/i);
			if (bedMatch) {
				bedrooms = parseInt(bedMatch[1], 10);
			}
		}

		await processPropertyWithCoordinates(
			property.url,
			property.price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
			htmlContent,
			null,
			null,
		);

		counts.totalSaved++;
		counts.totalScraped++;
		if (isRental) counts.savedRentals++;
		else counts.savedSales++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.url}:`, error);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForTimeout(2000); // Allow JS rendering and potential CF

		await page.waitForSelector("a.property-grid", { timeout: 15000 }).catch(() => {});

		const htmlContent = await page.content();
		const $ = cheerio.load(htmlContent);

		const cards = $("a.property-grid");
		logger.page(pageNum, label, `Found ${cards.length} property cards`, totalPages);

		if (cards.length === 0) {
			logger.page(pageNum, label, `No cards found — stopping early`, totalPages);
			return;
		}

		for (let i = 0; i < cards.length; i++) {
			const $card = $(cards[i]);

			let href = $card.attr("href");
			if (!href) continue;

			let link = href.startsWith("http") ? href : `${BASE_URL}${href}`;
			if (!link.includes("estateseast.co.uk")) link = `https://estateseast.co.uk${href}`;

			// Skip sold/SSTC properties
			if (isSoldProperty($card.text())) {
				logger.page(pageNum, label, `Skipped [SOLD]: ${link}`, totalPages);
				continue;
			}

			// Title
			const title = $card.find("h3.grid-address").text().trim() || "Estates East Property";

			// Price
			const priceText =
				$card.find("h4.property__price span").text().trim() ||
				$card.find("h4.property__price").text().trim();
			const price = parsePrice(priceText);

			if (!price) {
				logger.page(pageNum, label, `Skipping (no price): ${link}`, totalPages);
				continue;
			}

			// Bedrooms (Not on listing grid anymore)
			const bedrooms = null;

			// Persist property
			const result = await updatePriceByPropertyURLOptimized(
				link,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";
			if (result.updated) propertyAction = "UPDATED";

			if (!result.isExisting || result.updated) {
				propertyAction = result.isExisting ? "UPDATED" : "CREATED";
				await scrapePropertyDetail(page.context(), { url: link, price, title, bedrooms }, isRental);
			} else if (result.isExisting) {
				counts.totalScraped++;
			}

			logger.property(
				pageNum,
				label,
				title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				link,
				isRental,
				totalPages,
				propertyAction,
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500);
			}
		}
	} catch (error) {
		logger.error(`Error processing page ${pageNum} for ${label}`, error);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 60,
		requestHandlerTimeoutSecs: 300,
		sessionPoolOptions: { blockedStatusCodes: [] },
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await page.setExtraHTTPHeaders({
					"User-Agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
				});
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeEstatesEast() {
	logger.step(`Starting Estates East scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	const PROPERTY_TYPES = [
		{
			urlPath: "buy/?orderby=price_desc&instruction_type=sale",
			totalPages: SALES_PAGES,
			isRental: false,
			label: "SALES",
		},
		{
			urlPath:
				"to-let/?orderby=price_desc&instruction_type=Letting&instruction_type=Letting&address_keyword=",
			totalPages: RENTAL_PAGES,
			isRental: true,
			label: "RENTALS",
		},
	];

	for (const type of PROPERTY_TYPES) {
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			allRequests.push({
				url: `${BASE_URL}/${type.urlPath}&page=${pg}`,
				userData: {
					pageNum: pg,
					totalPages: type.totalPages,
					isRental: type.isRental,
					label: type.label,
				},
			});
		}
	}

	logger.step(`Queueing ${allRequests.length} pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Estates East - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeEstatesEast();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
