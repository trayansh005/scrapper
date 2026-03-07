// Fenn Wright scraper using Playwright with Crawlee
// Agent ID: 242

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");

const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
	formatPriceUk,
} = require("./lib/db-helpers.js");

const { parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources, sleep } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 242;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// PARSING
// ============================================================================

function parseListingPage(html) {
	const $ = cheerio.load(html);
	const properties = [];

	$(".info-item").each((_, el) => {
		const link = $(el).find("a.caption").attr("href");
		const title = $(el).find("h3").text().trim();
		const priceText = $(el).find(".price").text().trim();
		const price = parsePrice(priceText);

		let bedrooms = null;
		const bedMatch = $(el).find("figure").text().match(/(\d+)/);
		if (bedMatch) bedrooms = bedMatch[1];

		if (link && title) {
			properties.push({ link, title, price, bedrooms });
		}
	});

	return properties;
}

// ============================================================================
// DETAIL SCRAPER
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	await sleep(500);

	const detailPage = await browserContext.newPage();

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
			timeout: 30000,
		});

		const htmlContent = await detailPage.content();

		const geo = await detailPage.evaluate(() => {
			const html = document.documentElement.innerHTML;
			const lat = html.match(/"latitude":\s*(-?\d+\.\d+)/i);
			const lon = html.match(/"longitude":\s*(-?\d+\.\d+)/i);

			return {
				lat: lat ? parseFloat(lat[1]) : null,
				lon: lon ? parseFloat(lon[1]) : null,
			};
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			geo.lat,
			geo.lon
		);

		stats.totalScraped++;
		stats.totalSaved++;
		if (isRental) stats.savedRentals++;
		else stats.savedSales++;
	} catch (err) {
		logger.error(`Detail scrape failed: ${property.link}`, err.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNum } = request.userData;

	logger.page(pageNum, label, "Processing listing page...");

	await page.waitForTimeout(1200);

	await page.waitForSelector(".info-item", { timeout: 20000 }).catch(() => {});

	const html = await page.content();
	const properties = parseListingPage(html);

	logger.step(`Found ${properties.length} properties`, pageNum, label);

	const batchSize = 5;

	for (let i = 0; i < properties.length; i += batchSize) {
		const batch = properties.slice(i, i + batchSize);

		await Promise.all(
			batch.map(async (property) => {
				if (!property.link) return;

				if (processedUrls.has(property.link)) {
					logger.warn("Skipping duplicate", pageNum, label);
					return;
				}

				processedUrls.add(property.link);

				try {
					let actionTaken = "UNCHANGED";

					const priceNum = parsePrice(property.price);

					if (priceNum === null) {
						logger.warn("No price found", pageNum, label);
						return;
					}

					const result = await updatePriceByPropertyURLOptimized(
						property.link.trim(),
						priceNum,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental
					);

					if (result.updated) {
						stats.totalSaved++;
						actionTaken = "UPDATED";
					}

					if (!result.isExisting && !result.error) {
						await scrapePropertyDetail(
							page.context(),
							{
								...property,
								price: priceNum,
							},
							isRental
						);

						actionTaken = "CREATED";
					}

					const priceDisplay = isNaN(priceNum)
						? "N/A"
						: formatPriceUk(priceNum);

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

					// Step 7 Conditional Sleep
					if (actionTaken === "CREATED") {
						await sleep(500);
					}
				} catch (err) {
					logger.error("DB error", err, pageNum, label);
				}
			})
		);

		await sleep(200);
	}

	// Pagination
	const nextButton = await page.$("a.next.page-numbers");

	if (nextButton) {
		const nextUrl = await nextButton.getAttribute("href");

		if (nextUrl) {
			await crawler.addRequests([
				{
					url: nextUrl,
					userData: {
						isRental,
						label,
						pageNum: pageNum + 1,
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
		maxConcurrency: 2,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],

		launchContext: {
			launchOptions: {
				browserWSEndpoint,
			},
		},

		requestHandler: handleListingPage,

		failedRequestHandler({ request }) {
			logger.error(`Failed request: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeFennWright() {
	const scrapeStartTime = new Date();

	logger.step(`Starting Fenn Wright scraper`);

	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000";

	const crawler = createCrawler(browserWSEndpoint);

	await crawler.addRequests([
		{
			url: "https://www.fennwright.co.uk/property-search/?department=residential-sales",
			userData: { isRental: false, label: "SALES", pageNum: 1 },
		},
		{
			url: "https://www.fennwright.co.uk/property-search/?department=residential-lettings",
			userData: { isRental: true, label: "RENTALS", pageNum: 1 },
		},
	]);

	await crawler.run();

	logger.step(
		`Completed - Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`
	);

	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
	try {
		await scrapeFennWright();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error:", err);
		process.exit(1);
	}
})();