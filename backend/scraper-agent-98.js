// Alex Crown scraper using Playwright + Cheerio
// Agent ID: 98
// Site: https://www.alexcrown.co.uk/buy/property-for-sale?page=N
// Usage:
//   node backend/scraper-agent-98.js [startPage]

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

const AGENT_ID = 98;
const BASE_URL = "https://www.alexcrown.co.uk";
const SALES_PAGES = 5; // old code had 2; set to 5 safely, exits early if no cards
const RENTAL_PAGES = 5; // adding rentals at identical pattern to be safe
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

		let lat = null;
		let lon = null;

		// Extract coordinates from #maps data-cords
		const dataCords = $("#maps").attr("data-cords");
		if (dataCords) {
			try {
				const parsed = JSON.parse(dataCords);
				if (parsed.lat) lat = parseFloat(parsed.lat);
				if (parsed.lng) lon = parseFloat(parsed.lng);
			} catch (e) {
				logger.error(`Failed to parse data-cords on ${property.url}`, e);
			}
		}

		if (!lat || !lon) {
			// Fallback: check #schoolResultsData
			const schoolDataStr = $("#schoolResultsData").val();
			if (schoolDataStr) {
				try {
					const parsedArr = JSON.parse(schoolDataStr);
					if (parsedArr && parsedArr.length > 0) {
						if (parsedArr[0].lat) lat = parseFloat(parsedArr[0].lat);
						if (parsedArr[0].lng) lon = parseFloat(parsedArr[0].lng);
					}
				} catch (e) {}
			}
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

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForTimeout(3000); // small delay to clear any potential challenges

		// Wait for property cards to render
		await page.waitForSelector(".property", { timeout: 15000 }).catch(() => {});

		const htmlContent = await page.content();
		const $ = cheerio.load(htmlContent);

		const cards = $(".property");
		logger.page(pageNum, label, `Found ${cards.length} property cards`, totalPages);

		if (cards.length === 0) {
			logger.page(pageNum, label, `No cards found — stopping early`, totalPages);
			return;
		}

		for (let i = 0; i < cards.length; i++) {
			const $card = $(cards[i]);

			const href = $card.find(".property-description-link").attr("href");
			if (!href) continue;

			const link = href.startsWith("http") ? href : `${BASE_URL}${href}`;

			// Skip sold/SSTC properties
			if (isSoldProperty($card.text())) {
				logger.page(pageNum, label, `Skipped [SOLD]: ${link}`, totalPages);
				continue;
			}

			// Title
			const title = $card.find(".grid-address").text().trim() || "Alex Crown Property";

			// Price
			const priceText = $card.find(".nativecurrencyvalue").text().trim();
			const price = parsePrice(priceText);

			if (!price) {
				logger.page(pageNum, label, `Skipping (no price): ${link}`, totalPages);
				continue;
			}

			// Bedrooms
			const bedroomsText = $card
				.find(".FeaturedProperty__list-stats-item--bedrooms span")
				.text()
				.trim();
			const bedroomsMatch = bedroomsText.match(/\d+/);
			const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[0], 10) : null;

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

			// Only delay on write operations (BASELINE_RULES: conditional sleep)
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

async function scrapeAlexCrown() {
	logger.step(`Starting Alex Crown scraper (Agent ${AGENT_ID})...`);

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
			urlPath: "buy/property-for-sale",
			totalPages: SALES_PAGES,
			isRental: false,
			label: "SALES",
		},
		// Extrapolating rentals URL path based on typical generic estate agency platforms, if it works, cool. Otherwise it will likely 404 and exit gracefully.
		{
			urlPath: "let/property-to-let",
			totalPages: RENTAL_PAGES,
			isRental: true,
			label: "RENTALS",
		},
	];

	for (const type of PROPERTY_TYPES) {
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			allRequests.push({
				url: `${BASE_URL}/${type.urlPath}?page=${pg}`,
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
		`Completed Alex Crown - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
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
		await scrapeAlexCrown();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
