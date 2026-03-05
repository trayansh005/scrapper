// Taylor Wimpey scraper using PlaywrightCrawler
// Agent ID: 82
// Site: https://www.taylorwimpey.co.uk/new-homes/england
// Usage:
//   node backend/scraper-agent-82.js [startPage]

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

const AGENT_ID = 82;
const BASE_URL = "https://www.taylorwimpey.co.uk";
const TOTAL_PAGES = 10; // Taylor wimpey has many developments, let's process up to 10 pages of search results
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0, // Taylor Wimpey focus only on sales, but keeping standard structure
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// Extract JSON safely from script tags
function extractMapData(htmlContent) {
	try {
		// Look for window.referenceData assignment
		const regex = /window\.referenceData\s*=\s*({.*?});/s;
		const match = htmlContent.match(regex);
		if (match && match[1]) {
			const data = JSON.parse(match[1]);
			if (data && data.staticDynamicMapData && data.staticDynamicMapData.markers) {
				return data.staticDynamicMapData.markers;
			}
		}
	} catch (e) {
		logger.warn("Failed to parse window.referenceData script for coordinates", e);
	}
	return [];
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
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		// Wait for development segments to render
		await page.waitForSelector(".hf-dev-segment", { timeout: 15000 }).catch(() => {});

		const htmlContent = await page.content();
		const $ = cheerio.load(htmlContent);

		const developments = $(".hf-dev-segment");
		logger.page(pageNum, label, `Found ${developments.length} development blocks`, totalPages);

		if (developments.length === 0) {
			logger.page(pageNum, label, `No development blocks found — stopping early`, totalPages);
			return;
		}

		// Extract coordinates map from the page script
		const markers = extractMapData(htmlContent);
		const coordsByDevId = {};
		markers.forEach((marker) => {
			if (marker.id && marker.coordinates) {
				coordsByDevId[marker.id] = {
					lat: marker.coordinates.lat,
					lon: marker.coordinates.lng, // Note: they use 'lng' but we use 'lon'
				};
			}
		});

		// Iterate through each development
		for (let d = 0; d < developments.length; d++) {
			const $dev = $(developments[d]);
			const devId = $dev.attr("data-property-id");
			const devName =
				$dev.find(".hf-dev-segment-content__heading-title h2 a").text().trim() ||
				"Taylor Wimpey Development";

			const coords = coordsByDevId[devId] || { lat: null, lon: null };

			// Find all plots within this development
			const plots = $dev.find(".home-finder-plot-segment");

			for (let p = 0; p < plots.length; p++) {
				const $plot = $(plots[p]);

				const $link = $plot.find("h4.home-finder-plot-segment__title a");
				const href = $link.attr("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : `${BASE_URL}${href}`;
				const plotTitle = $link.text().trim() || "Taylor Wimpey Plot";

				// Full title: "Plot Title - Development Name"
				const title = `${plotTitle} - ${devName}`;

				// Skip sold properties
				if (isSoldProperty($plot.text())) {
					logger.page(pageNum, label, `Skipped [SOLD]: ${link}`, totalPages);
					continue;
				}

				// Price
				const priceText = $plot.find(".plot-card-price__text").text().trim();
				const price = parsePrice(priceText);

				if (!price) {
					logger.page(pageNum, label, `Skipping (no price): ${link}`, totalPages);
					continue;
				}

				// Bedrooms
				const bedroomsText = $plot.find(".plot-key-stats__stat--bedrooms").text().trim();
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

				if (!result.isExisting && !result.error) {
					propertyAction = "CREATED";
					await processPropertyWithCoordinates(
						link,
						price,
						title,
						bedrooms,
						AGENT_ID,
						isRental,
						htmlContent,
						coords.lat,
						coords.lon,
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
					title.substring(0, 50), // slightly larger to fit dev name
					formatPriceDisplay(price, isRental),
					link,
					isRental,
					totalPages,
					propertyAction,
				);

				// Only delay on write operations
				if (propertyAction !== "UNCHANGED") {
					await sleep(500);
				}
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

async function scrapeTaylorWimpey() {
	logger.step(`Starting Taylor Wimpey scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	for (let pg = Math.max(1, startPage); pg <= TOTAL_PAGES; pg++) {
		// Taylor Wimpey pagination seems to use standard ?page=N for the primary listing
		allRequests.push({
			url: `${BASE_URL}/new-homes/england?page=${pg}`,
			userData: {
				pageNum: pg,
				totalPages: TOTAL_PAGES,
				isRental: false,
				label: "SALES",
			},
		});
	}

	logger.step(`Queueing ${allRequests.length} pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Taylor Wimpey - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}`,
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
		await scrapeTaylorWimpey();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
