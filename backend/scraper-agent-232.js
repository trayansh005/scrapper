// Richard James scraper using Playwright with Crawlee
// Agent ID: 232
// Usage:
// node backend/scraper-agent-232.js [startPage]

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

const AGENT_ID = 232;
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
// PROPERTY TYPE CONFIGURATION
// ============================================================================

// Two searches:
// - For Sale: 408 properties, 18 per page => 23 pages
// - To Let: 28 properties, 18 per page => 2 pages
const PROPERTY_TYPES = [
	{
		urlBase: "https://richardjames.uk/search-results/page",
		totalPages: 23,
		isRental: false,
		label: "FOR SALE",
		suffix: "/?keyword&status%5B0%5D=for-sale",
	},
	{
		urlBase: "https://richardjames.uk/search-results/page",
		totalPages: 2,
		isRental: true,
		label: "TO LET",
		suffix:
			"/?keyword&status%5B0%5D=to-let&min-price=0&max-price=2500000&bathrooms&bedrooms&property_id",
	},
];

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
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		async requestHandler({ page, request }) {
			const { pageNum, isRental, label, totalPages } = request.userData;

			logger.page(pageNum, label, request.url, totalPages);

			await page.waitForTimeout(1500);

			// Wait for listing cards
			await page
				.waitForSelector(".item-listing-wrap, .item-listing-wrap-v6, .item-listing-wrap-v6.card", {
					timeout: 15000,
				})
				.catch(() => null);

			// Extract properties
			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(
							".item-listing-wrap, .item-listing-wrap-v6, .item-listing-wrap-v6.card",
						),
					);

					return items
						.map((el) => {
							try {
								const linkEl =
									el.querySelector("h2.item-title a") ||
									el.querySelector(".item-title a") ||
									el.querySelector(".listing-image-wrap a") ||
									el.querySelector(".rh_list_card__thumbnail a");
								const link = linkEl ? linkEl.href : null;
								const title =
									el.querySelector("h2.item-title a, .item-title a")?.textContent?.trim() || "";
								const rawPrice =
									el.querySelector(".item-price, .item-price .price")?.textContent?.trim() || "";

								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}

								const beds =
									el.querySelector(".hz-figure, .figure, .hz-figure")?.textContent?.trim() || null;

								if (!link) return null;

								return { link, title, price, bedrooms: beds };
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

			// Process properties sequentially (not in batches)
			for (const property of properties) {
				if (!property.link) continue;

				if (processedUrls.has(property.link.trim())) {
					continue;
				}
				processedUrls.add(property.link.trim());

				try {
					const priceNum = parsePrice(property.price);

					if (priceNum === null) {
						counts.totalSkipped++;
						continue;
					}

					// Check if property exists first (before loading detail page)
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
						// Only load detail page for NEW properties
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

						await processPropertyWithCoordinates(
							property.link.trim(),
							priceNum,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
							htmlContent,
						);

						action = "CREATED";
						counts.totalSaved++;
						counts.totalScraped++;
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
						isRental ? "TO LET" : "FOR SALE",
						action,
					);

					// Only sleep for CREATED properties
					if (action === "CREATED") {
						await sleep(500);
					}
				} catch (err) {
					logger.error(`Error processing property ${property.link}`, err);
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

		logger.step(`Starting Richard James scraper (Agent ${AGENT_ID})`);

		const browserWSEndpoint = getBrowserlessEndpoint();

		for (const propertyType of PROPERTY_TYPES) {
			const { urlBase, totalPages, isRental, label, suffix } = propertyType;

			const crawler = createCrawler(browserWSEndpoint);

			const initialRequests = [];
			for (let pg = startPage; pg <= totalPages; pg++) {
				const url = `${urlBase}/${pg}/${suffix}`;
				initialRequests.push({
					url,
					userData: { pageNum: pg, totalPages, isRental, label },
				});
			}

			await crawler.run(initialRequests);
		}

		// Partial run protection: only mark as removed if full scrape from page 1
		if (!isPartialRun) {
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		}

		logger.step(
			`Richard James scraper complete - Found: ${counts.totalFound} | Scraped: ${counts.totalScraped} | Saved: ${counts.totalSaved} (Sales: ${counts.savedSales}, Lettings: ${counts.savedRentals})`,
		);

		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
