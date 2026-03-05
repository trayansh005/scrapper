// Nicolas Van Patrick scraper using Playwright to bypass Cloudflare + HTML scraping
// Agent ID: 87
// Site: https://nicolasvanpatrick.com/sales/
// Usage:
//   node backend/scraper-agent-87.js [startPage]

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

const AGENT_ID = 87;
const BASE_URL = "https://nicolasvanpatrick.com";
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
// PROCESS ONE PAGE OF PROPERTY CARDS
// ============================================================================

async function processPageCards(page, pageNum, totalPages, isRental, label) {
	const htmlContent = await page.content();
	const $ = cheerio.load(htmlContent);

	const cards = $(".cnt_frame");
	logger.page(pageNum, label, `Found ${cards.length} property cards`, totalPages);

	if (cards.length === 0) return false; // signal: no more pages

	for (let i = 0; i < cards.length; i++) {
		const $card = $(cards[i]);

		const href = $card.find("a").first().attr("href");
		if (!href) continue;

		const link = href.startsWith("http") ? href : `${BASE_URL}${href}`;

		// Skip sold/SSTC/let properties
		if (isSoldProperty($card.text())) {
			logger.page(pageNum, label, `Skipped [SOLD]: ${link}`, totalPages);
			continue;
		}

		// Title / address
		const title = $card.find("h2").first().text().trim() || "Nicolas Van Patrick Property";

		// Price — the old code used `#price` selector
		const priceText =
			$card.find("#price").text().trim() ||
			$card.find(".price").text().trim() ||
			$card.find("[class*='price']").first().text().trim();
		const price = parsePrice(priceText);

		if (!price) {
			logger.page(pageNum, label, `Skipping (no price): ${link}`, totalPages);
			continue;
		}

		// Bedrooms — old code used `.group prty_items` eq(0)
		const bedroomsText =
			$card.find(".group .prty_items").eq(0).text().trim() ||
			$card.find(".prty_items").eq(0).text().trim();
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

		if (!result.isExisting && !result.error) {
			propertyAction = "CREATED";
			await processPropertyWithCoordinates(
				link,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
				null,
				null,
				null,
			);
			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
		} else if (result.updated) {
			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
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

	return true; // signal: had cards, continue
}

// ============================================================================
// REQUEST HANDLER — mega-handler: one request, all pages via page.goto()
// Cloudflare is only challenged once on the first navigation; subsequent
// page.goto() calls reuse the same cleared browser session.
// ============================================================================

async function handleAllPages({ page, request }) {
	const { startPage, totalPages, isRental, label } = request.userData;

	try {
		// CF challenge clears on first load — wait generously
		logger.step(`Waiting for Cloudflare to clear on page ${startPage}...`);
		await page.waitForTimeout(8000);
		await page.waitForSelector(".cnt_frame, .property-listing", { timeout: 25000 }).catch(() => {});

		// Process page 1 (already loaded by Crawlee)
		const hadCards = await processPageCards(page, startPage, totalPages, isRental, label);
		if (!hadCards) return;

		// Loop through remaining pages within the same browser session
		for (let pg = startPage + 1; pg <= totalPages; pg++) {
			const pageUrl = `${BASE_URL}/sales/?option=Hide&pg=${pg}`;
			logger.page(pg, label, pageUrl, totalPages);

			try {
				await page.goto(pageUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
				await page.waitForTimeout(2000);
				await page
					.waitForSelector(".cnt_frame, .property-listing", { timeout: 15000 })
					.catch(() => {});

				const more = await processPageCards(page, pg, totalPages, isRental, label);
				if (!more) {
					logger.page(pg, label, `No cards on page ${pg} — stopping early`, totalPages);
					break;
				}
			} catch (err) {
				logger.error(`Error navigating to page ${pg}`, err);
				break;
			}
		}
	} catch (error) {
		logger.error(`Fatal error in mega-handler`, error);
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
		// Prevent Crawlee from treating Cloudflare's 403 challenge as a hard block.
		// Without this, _throwOnBlockedRequest fires before the handler runs.
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
		requestHandler: handleAllPages,
		failedRequestHandler({ request }) {
			logger.error(`Failed entry request: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeNicolasVanPatrick() {
	logger.step(`Starting Nicolas Van Patrick scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	// Nicolas Van Patrick: sales only, approx 4 pages
	// URL pattern: /sales/?option=Hide&pg=N (page 1 = /sales/?option=Hide&pg=1)
	const PROPERTY_TYPES = [
		{
			urlPath: "sales",
			totalPages: 10, // Approximate; gracefully exits if fewer pages exist
			isRental: false,
			label: "SALES",
		},
	];

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	// ONE entry request per property type — the mega-handler loops all pages
	// internally via page.goto() to avoid per-request Cloudflare 403s.
	const entryRequests = PROPERTY_TYPES.map((type) => ({
		url: `${BASE_URL}/${type.urlPath}/?option=Hide&pg=${Math.max(1, startPage)}`,
		userData: {
			startPage: Math.max(1, startPage),
			totalPages: type.totalPages,
			isRental: type.isRental,
			label: type.label,
		},
	}));

	logger.step(`Starting from page ${startPage}, up to ${PROPERTY_TYPES[0].totalPages} pages...`);
	await crawler.run(entryRequests);

	logger.step(
		`Completed Nicolas Van Patrick - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
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
		await scrapeNicolasVanPatrick();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
