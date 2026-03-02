// Gascoigne Pees scraper using Playwright with Crawlee
// Agent ID: 116
// Website: www.gpees.co.uk
// Usage:
// node backend/scraper-agent-116.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	extractCoordinatesFromHTML,
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 116;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlPath: "properties/sales/status-available/most-recent-first",
		totalRecords: 512,
		recordsPerPage: 10,
		isRental: false,
		label: "SALES",
	},
	{
		urlPath: "properties/lettings/status-available/most-recent-first",
		totalRecords: 70,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;

	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector(".hf-property-results .card", { timeout: 30000 }).catch(() => {
			logger.warn(`No listing container found on page ${pageNum}`);
		});

		const properties = await page.evaluate(() => {
			const results = [];
			const cards = Array.from(document.querySelectorAll(".hf-property-results .card"));
			for (const card of cards) {
				try {
					let linkEl = card.querySelector("a");
					let link = linkEl ? linkEl.getAttribute("href") : null;
					if (link && !link.startsWith("http")) {
						link = "https://www.gpees.co.uk" + link;
					}

					const titleEl = card.querySelector(".card__text-content");
					const title = titleEl ? titleEl.textContent.trim() : "";

					let bedrooms = null;
					const bedroomsEl = card.querySelector(".card-content__spec-list-number");
					if (bedroomsEl) {
						const bedsText = bedroomsEl.textContent.trim();
						const m = bedsText.match(/\d+/);
						if (m) bedrooms = parseInt(m[0], 10);
					}

					let priceRaw = "";
					const priceEl = card.querySelector(".card__heading");
					if (priceEl) {
						priceRaw = priceEl.textContent.trim();
					}

					if (link && priceRaw && title) {
						results.push({ link, title, priceRaw, bedrooms, statusText: card.innerText || "" });
					}
				} catch (err) {
					// ignore
				}
			}
			return results;
		});

		logger.page(pageNum, label, `Found ${properties.length} properties`);

		for (const property of properties) {
			if (!property.link) continue;
			if (isSoldProperty(property.statusText || property.priceRaw)) continue;

			// Parse price using shared helper
			const price = parsePrice(property.priceRaw);
			if (!price) {
				logger.warn(`Skipping (no price): ${property.link}`);
				continue;
			}

			// --- Agent 39 base pattern: check existing → update or create ---
			const result = await updatePriceByPropertyURLOptimized(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			if (result.updated) {
				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (result.isExisting) {
				counts.totalScraped++;
			}

			let propertyAction = "UNCHANGED";
			if (result.updated) propertyAction = "UPDATED";

			if (!result.isExisting && !result.error) {
				propertyAction = "CREATED";

				// Fetch detail page ONLY for new properties to extract coords
				const detailPage = await page.context().newPage();
				let html = null;
				let latitude = null;
				let longitude = null;

				try {
					await blockNonEssentialResources(detailPage);
					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});
					await detailPage.waitForTimeout(500);

					html = await detailPage.content();

					// Extract coords using central helper
					const coords = await extractCoordinatesFromHTML(html);
					latitude = coords?.latitude || null;
					longitude = coords?.longitude || null;

					// Gascoigne-specific comment regex fallback (already integrated in some versions of helper, but keeping here for legacy certainty)
					if (!latitude || !longitude) {
						const latMatch = html.match(/<!--property-latitude:"([0-9.\-]+)"-->/);
						const lngMatch = html.match(/<!--property-longitude:"([0-9.\-]+)"-->/);
						if (latMatch && lngMatch) {
							latitude = parseFloat(latMatch[1]);
							longitude = parseFloat(lngMatch[1]); // Note: Fix potential copy-paste bug from original file
						}
					}

					logger.step(`Coords: ${latitude || "No Lat"}, ${longitude || "No Lng"}`);
				} catch (err) {
					logger.error(`Detail page failed: ${property.link}`);
				} finally {
					await detailPage.close();
				}

				await processPropertyWithCoordinates(
					property.link,
					price,
					property.title,
					property.bedrooms,
					AGENT_ID,
					isRental,
					html,
					latitude,
					longitude,
				);

				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			}

			logger.property(
				pageNum,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				totalPages,
				propertyAction,
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500); // DB politeness delay for writes
			}
		}
	} catch (error) {
		logger.error(`Error in ${label} page ${pageNum}: ${error.message}`);
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
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeGascoignePees() {
	logger.step(`Starting Gascoigne Pees scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	for (const propertyType of PROPERTY_TYPES) {
		const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
		logger.step(`Processing ${propertyType.label} (${totalPages} pages)`);

		const crawler = createCrawler(browserWSEndpoint);
		const requests = [];

		for (let pg = 1; pg <= totalPages; pg++) {
			requests.push({
				url: `https://www.gpees.co.uk/${propertyType.urlPath}/page-${pg}#/`,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label, totalPages },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	logger.step(
		`Completed Gascoigne Pees - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
}

(async () => {
	try {
		const scrapeStartTime = new Date();
		await scrapeGascoignePees();
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
