// Dixons Estate Agents scraper using Playwright with Crawlee
// Agent ID: 243
// Usage: node backend/scraper-agent-243.js

const { PlaywrightCrawler, log } = require("crawlee");

const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
} = require("./lib/db-helpers.js");

const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Inline sleep
function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
}

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 243;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING (for logging & future use, no DB update)
// ============================================================================

async function scrapePropertyDetail(context, property) {
	await sleep(3000 + Math.random() * 2000);

	const detailPage = await context.newPage();

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

		const coords = await detailPage.evaluate(() => {
			const html = document.documentElement.innerHTML;
			const lat = html.match(/"latitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
			const lon = html.match(/"longitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
			return {
				lat: lat ? parseFloat(lat[1]) : null,
				lon: lon ? parseFloat(lon[1]) : null,
			};
		});

		logger.step(`Coords → lat=${coords.lat ?? 'null'}, lon=${coords.lon ?? 'null'}`);
	} catch (err) {
		logger.error(`Detail failed → ${property.link}`, err.message || err);
	} finally {
		await detailPage.close().catch(() => { });
	}
}

// ============================================================================
// PROPERTY TYPES
// ============================================================================

const PROPERTY_TYPES = [
	{
		url: "https://www.dixonsestateagents.co.uk/properties/sales/status-available/most-recent-first/page-1#/",
		isRental: false,
		label: "SALES",
	},
	{
		url: "https://www.dixonsestateagents.co.uk/properties/lettings/status-available/most-recent-first/page-1#/",
		isRental: true,
		label: "LETTINGS",
	},
];

// ============================================================================
// MAIN SCRAPER
// ============================================================================

async function scrapeDixons() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Dixons scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless...`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 4,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 600,

		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ['--disable-blink-features=AutomationControlled'],
			},
		},

		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],

		async requestHandler({ page, request }) {
			const { pageNum = 1, isRental, label } = request.userData;

			logger.page(pageNum, label, "Processing listing page...");

			await sleep(2500 + Math.random() * 1500);

			await page.waitForSelector(".card", { timeout: 30000 })
				.catch(() => logger.warn("No .card elements found", pageNum, label));

			const properties = await page.evaluate(() => {
				const cards = document.querySelectorAll(".card");
				const results = [];
				const baseUrl = window.location.origin;

				cards.forEach((card) => {
					try {
						const linkEl = card.querySelector("a.card__link");
						if (!linkEl) return;
						const href = linkEl.getAttribute("href");
						if (!href) return;
						const link = href.startsWith("http") ? href : `${baseUrl}${href.startsWith("/") ? "" : "/"}${href}`;

						const priceText = card.querySelector(".card__heading")?.innerText.trim() || "";
						const title = card.querySelector(".card__text-content")?.innerText.trim() || "Property";

						let bedrooms = null;
						const specs = card.querySelectorAll(".card-content__spec-list-item");
						specs.forEach((spec) => {
							if (spec.querySelector(".icon-bedroom")) {
								const val = spec.querySelector(".card-content__spec-list-number")?.innerText.trim();
								if (val) bedrooms = parseInt(val, 10);
							}
						});

						const statusText = card.innerText.toLowerCase();

						if (link) {
							results.push({ link, title, priceText, bedrooms, statusText });
						}
					} catch { }
				});

				return results;
			});

			logger.step(`Found ${properties.length} properties`, pageNum, label);

			const batchSize = 3;

			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						if (isSoldProperty(property.statusText || "")) {
							logger.warn(`Skipping sold/let agreed`, pageNum, label);
							return;
						}

						if (processedUrls.has(property.link)) {
							logger.warn(`Skipping duplicate`, pageNum, label);
							return;
						}

						processedUrls.add(property.link);

						try {
							let actionTaken = "UNCHANGED";

							const priceNum = parsePrice(property.priceText);

							if (!priceNum || isNaN(priceNum)) {
								logger.warn(`No valid price found`, pageNum, label);
								return;
							}

							const result = await updatePriceByPropertyURLOptimized(
								property.link.trim(),
								priceNum,
								property.title,
								property.bedrooms || null,
								AGENT_ID,
								isRental
							);

							if (!result.isExisting && !result.error) {

								logger.step(`New detail → ${property.title}`, pageNum, label);

								await scrapePropertyDetail(page.context(), property, isRental);

								stats.totalScraped++;
								stats.totalSaved++;
								if (isRental) stats.savedRentals++;
								else stats.savedSales++;

								actionTaken = "CREATED";

							} else if (result.updated) {

								actionTaken = "UPDATED";
								stats.totalSaved++;

							} else {

								actionTaken = "UNCHANGED";

							}

							if (!result.isExisting && !result.error) {
								logger.step(`New detail → ${property.title}`, pageNum, label);
								await scrapePropertyDetail(page.context(), property, isRental);

								stats.totalScraped++;
								stats.totalSaved++;
								if (isRental) stats.savedRentals++;
								else stats.savedSales++;

								actionTaken = "CREATED";
							}

							const priceDisplay = formatPriceUk(priceNum);

							logger.property(
								pageNum,
								label,
								property.title,
								priceDisplay,
								property.link,
								isRental,
								null,
								actionTaken
							);

							if (actionTaken === "CREATED") {
								await sleep(5000 + Math.random() * 3000); // 5–8 sec delay
							}
						} catch (err) {
							logger.error(`Property processing failed → ${property.link}`, err.message || err, pageNum, label);
						}
					})
				);

				await sleep(2500 + Math.random() * 1500);
			}

			// Pagination
			if (properties.length > 0) {
				const nextPage = pageNum + 1;
				const type = isRental ? "lettings" : "sales";
				const nextUrl = `https://www.dixonsestateagents.co.uk/properties/${type}/status-available/most-recent-first/page-${nextPage}#/`;

				logger.step(`Enqueuing page ${nextPage}`, pageNum, label);
				await crawler.addRequests([{
					url: nextUrl,
					userData: { pageNum: nextPage, isRental, label },
				}]);
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Request permanently failed → ${request.url}`);
		},
	});

	logger.step(`Queueing SALES and LETTINGS`);

	await crawler.addRequests(
		PROPERTY_TYPES.map(type => ({
			url: type.url,
			userData: {
				pageNum: 1,
				isRental: type.isRental,
				label: type.label,
			},
		}))
	);

	await crawler.run();

	logger.step(`Completed Dixons scraper`);
	logger.step(`Total scraped: ${stats.totalScraped} | Total saved: ${stats.totalSaved}`);
	logger.step(`Breakdown → SALES: ${stats.savedSales} | LETTINGS: ${stats.savedRentals}`);

	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
	try {
		await scrapeDixons();
		logger.step("\nAll done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error:", err?.message || err);
		process.exit(1);
	}
})();