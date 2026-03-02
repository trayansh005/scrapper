// CJ Hole scraper using Playwright with Crawlee
// Agent ID: 216
// Usage:
// node backend/scraper-agent-216.js

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

const AGENT_ID = 216;
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

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.cjhole.co.uk/search-results/for-sale/in-united-kingdom",
		totalPages: Math.ceil(592 / 9),
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.cjhole.co.uk/search-results/for-letting/in-united-kingdom",
		totalPages: Math.ceil(123 / 9),
		isRental: true,
		label: "RENTALS",
	},
];

// ============================================================================
// COORDINATE EXTRACTION — CJ Hole JSON-LD GeoCoordinates
// Pattern in HTML: "GeoCoordinates","latitude":51.4567,"longitude":-2.5432
// extractCoordinatesFromHTML covers standard patterns; fallback handles CJ Hole format.
// ============================================================================

function extractCJHoleCoords(html) {
	// Try shared extractor first
	const coords = extractCoordinatesFromHTML(html);
	if (coords?.latitude && coords?.longitude) return coords;

	// CJ Hole-specific fallback: inline JSON-LD without quoted numbers
	const geoMatch = html.match(
		/"GeoCoordinates","latitude"\s*:\s*([0-9.-]+)\s*,\s*"longitude"\s*:\s*([0-9.-]+)/,
	);
	if (geoMatch) {
		return {
			latitude: parseFloat(geoMatch[1]),
			longitude: parseFloat(geoMatch[2]),
		};
	}

	return { latitude: null, longitude: null };
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;

	logger.page(pageNum, label, request.url);

	await page
		.waitForSelector(".property--card__results", { timeout: 20000 })
		.catch(() => null);

	const properties = await page.evaluate(() => {
		const items = Array.from(
			document.querySelectorAll(".property--card__results"),
		);
		return items
			.map((el) => {
				const linkEl = el.querySelector("a.property--card__image-wrapper");
				const href = linkEl?.href || null;

				const title =
					el.querySelector(".property-title a")?.textContent?.trim() || "";

				const price =
					el.querySelector(".property-price")?.textContent?.trim() || "";

				let bedrooms = null;
				const typeText = el.querySelector(".property-type")?.textContent || "";
				const bedMatch = typeText.match(/(\d+)\s*bedroom/i);
				if (bedMatch) bedrooms = bedMatch[1];

				return { link: href, title, priceRaw: price, bedrooms };
			})
			.filter((p) => p.link);
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`);

	for (const property of properties) {
		if (!property.link) continue;
		if (isSoldProperty(property.priceRaw || "")) continue;

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

				html = await detailPage.content();
				const coords = extractCJHoleCoords(html);
				latitude = coords.latitude;
				longitude = coords.longitude;

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
			null,
			propertyAction,
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(500);
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
		requestHandlerTimeoutSecs: 120,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeCJHole() {
	logger.step(`Starting CJ Hole scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			requests.push({
				url: `${propertyType.urlBase}/page-${pg}/?orderby=price_desc&radius=0.1`,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	logger.step(
		`Completed CJ Hole - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
}

(async () => {
	try {
		const scrapeStartTime = new Date();
		await scrapeCJHole();
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();