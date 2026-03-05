// Douglas and Gordon scraper using PlaywrightCrawler
// Agent ID: 48
// Site: https://www.douglasandgordon.com
// Usage:
//   node backend/scraper-agent-48.js [startPage]

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

const AGENT_ID = 48;
const BASE_URL = "https://www.douglasandgordon.com";
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

		// Extract coordinates from initStreetView script
		// initStreetView=function(){var pos={lat:51.553668,lng:-0.245176};
		let lat = null;
		let lon = null;

		const latMatch = htmlContent.match(/pos=\{lat:([\d.-]+),lng:([\d.-]+)\}/);
		if (latMatch) {
			lat = parseFloat(latMatch[1]);
			lon = parseFloat(latMatch[2]);
		}

		await processPropertyWithCoordinates(
			property.url,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			htmlContent,
			lat,
			lon,
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

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		// Wait for property listings container
		await page.waitForSelector(".properties-list", { timeout: 15000 }).catch(() => {});

		const htmlContent = await page.content();
		const $ = cheerio.load(htmlContent);

		const cards = $("li.type-lbl");
		logger.page(pageNum, label, `Found ${cards.length} property cards`, totalPages);

		if (cards.length === 0) {
			logger.page(
				pageNum,
				label,
				`No property cards found — page may exceed total, skipping`,
				totalPages,
			);
			return;
		}

		for (let i = 0; i < cards.length; i++) {
			const $card = $(cards[i]);

			const href =
				$card.find(".property-caption h4 a.cta-link").attr("href") ||
				$card.find("a").first().attr("href");
			if (!href) continue;

			const link = href.startsWith("http") ? href : `${BASE_URL}${href}`;

			// Skip sold/SSTC/Let properties
			if (isSoldProperty($card.text())) {
				logger.page(pageNum, label, `Skipped [SOLD]: ${link}`, totalPages);
				continue;
			}

			// Title
			const title =
				$card.find(".property-caption h4.text-truncate").text().trim() ||
				"Douglas and Gordon Property";

			// Price
			const priceText = $card.find(".property-caption h5").first().text().trim();
			const price = parsePrice(priceText);

			if (!price) {
				logger.page(pageNum, label, `Skipping (no price): ${link}`, totalPages);
				continue;
			}

			// Bedrooms
			const bedroomsText = $card.find(".list-inline .ico-bedroom").text().trim();
			const bedroomsMatch = bedroomsText.match(/\d+/);
			const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[0], 10) : null;

			// Check if property exists / update price
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
		navigationTimeoutSecs: 90,
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

async function scrapeDouglasAndGordon() {
	logger.step(`Starting Douglas and Gordon scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const PROPERTY_TYPES = [
		{
			urlPath: "buy/list/anywhere/houses-and-flats/",
			totalPages: 15, // Approximate; gracefully exits if fewer pages exist
			isRental: false,
			label: "SALES",
		},
		{
			urlPath: "rent/list/anywhere/houses-and-flats/",
			totalPages: 10,
			isRental: true,
			label: "RENTALS",
		},
	];

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		const effectiveStartPage = Math.max(1, startPage);

		for (let pg = effectiveStartPage; pg <= type.totalPages; pg++) {
			const pageUrl = `${BASE_URL}/${type.urlPath}?filter=exclude-under-offer&usersearch=true&page=${pg}`;

			allRequests.push({
				url: pageUrl,
				userData: {
					pageNum: pg,
					totalPages: type.totalPages,
					isRental: type.isRental,
					label: type.label,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Douglas and Gordon - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
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
		await scrapeDouglasAndGordon();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
