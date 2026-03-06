// Humberts scraper using Playwright with Crawlee
// Agent ID: 230
// Website: humberts.com
// Usage:
// node backend/scraper-agent-230.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 230;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();
let scrapeStartTime = null;

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
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
// PROPERTY TYPE CONFIGURATION
// ============================================================================

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.humberts.com/search/",
		totalPages: 3,
		isRental: true,
		label: "LETTINGS",
		suffix:
			"?country=GB&department=residential-lettings&tenure=&address_keyword=&radius=25&commercial_for_sale_to_rent=&property_type=&minimum_bedrooms=&minimum_price=&maximum_price=&lat=&lng=",
	},
];

// ============================================================================
// CRAWLER CONFIGURATION
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
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async (crawlingContext) => {
				await blockNonEssentialResources(crawlingContext.page);
			},
		],
		async requestHandler({ page, request }) {
			const { pageNum, totalPages, isRental, label } = request.userData;

			logger.page(pageNum, label, "Starting", totalPages);

			await page.waitForTimeout(1500);

			// Wait for property list items
			await page
				.waitForSelector("li.type-property", { timeout: 15000 })
				.catch(() => null);

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll("li.type-property"));

					return items
						.map((el) => {
							try {
								const titleAnchor = el.querySelector("h3 a");
								const link = titleAnchor ? titleAnchor.href : null;
								const title = titleAnchor ? titleAnchor.textContent.trim() : "";

								const rawPrice = el.querySelector(".price")?.textContent?.trim() || "";
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}

								const rooms = Array.from(el.querySelectorAll(".room-count")).map((s) =>
									s.textContent.trim(),
								);
								const bedrooms = rooms[0] || null;

								if (!link) return null;

								return { link, title, price, bedrooms };
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					return [];
				}
			});

			counts.totalFound += properties.length;

			// Process properties sequentially with detail page extraction
			for (const property of properties) {
				if (processedUrls.has(property.link.trim())) {
					continue;
				}
				processedUrls.add(property.link.trim());
				counts.totalScraped++;

				try {
					const priceNum = parsePrice(property.price);
					if (priceNum === null) {
						counts.totalSkipped++;
						continue;
					}

					// Scrape detail page to get HTML content for coordinate extraction
					const detailPage = await page.context().newPage();
					let htmlContent = null;

					try {
						await blockNonEssentialResources(detailPage);
						await detailPage.goto(property.link, {
							waitUntil: "domcontentloaded",
							timeout: 40000,
						});
						await detailPage.waitForTimeout(500);
						htmlContent = await detailPage.content();
					} catch (err) {
						logger.error(`Error fetching detail page ${property.link}`, err);
					} finally {
						await detailPage.close();
					}

					const result = await updatePriceByPropertyURLOptimized(
						property.link.trim(),
						priceNum,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
					);

					let action = "UNCHANGED";

					if (result.updated) {
						action = "UPDATED";
						counts.totalSaved++;
						if (isRental) counts.savedRentals++;
						else counts.savedSales++;
					} else if (!result.isExisting && !result.error) {
						action = "CREATED";
						await processPropertyWithCoordinates(
							property.link.trim(),
							priceNum,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
							htmlContent,
						);
						counts.totalSaved++;
						if (isRental) counts.savedRentals++;
						else counts.savedSales++;
					} else if (result.error) {
						action = "ERROR";
						counts.totalSkipped++;
					}

					logger.property(
						property.title.substring(0, 50),
						formatPriceDisplay(priceNum, isRental),
						property.link,
						isRental ? "LETTINGS" : "SALES",
						action,
					);

					if (action !== "UNCHANGED") {
						await sleep(100);
					}
				} catch (err) {
					logger.error(`Error saving property ${property.link}`, err);
					counts.totalSkipped++;
				}
			}

			logger.page(pageNum, label, "Complete", totalPages);
		},
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		scrapeStartTime = new Date();
		const startPage = getStartPage();
		const isPartialRun = startPage > 1;

		logger.step(`Starting Humberts scraper (Agent ${AGENT_ID})`);

		const browserWSEndpoint = getBrowserlessEndpoint();

		for (const propertyType of PROPERTY_TYPES) {
			const { urlBase, totalPages, isRental, label, suffix } = propertyType;

			const crawler = createCrawler(browserWSEndpoint);

			const requests = [];
			for (let pg = startPage; pg <= totalPages; pg++) {
				const url = `${urlBase}${suffix}`;
				requests.push({
					url,
					userData: { pageNum: pg, totalPages, isRental, label },
				});
			}

			await crawler.addRequests(requests);
			await crawler.run();
		}

		// Partial run protection: only mark as removed if full scrape from page 1
		if (!isPartialRun) {
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		}

		logger.step(
			`Humberts scraper complete - Found: ${counts.totalFound} | Scraped: ${counts.totalScraped} | Saved: ${counts.totalSaved} (Lettings: ${counts.savedRentals})`,
		);

		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
