// Martyn Gerrard property scraper using PlaywrightCrawler
// Agent ID: 76
// Usage:
//   node backend/scraper-agent-76.js [startPage]
//
// Coordinate strategy: Gatsby site — coordinates are fetched from Gatsby page-data JSON
// at /page-data/<property-path>/page-data.json (no browser visit needed for detail pages).
// Architecture: PlaywrightCrawler for listing pages; axios for detail data.

const { PlaywrightCrawler, log } = require("crawlee");
const axios = require("axios");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 76;
const logger = createAgentLogger(AGENT_ID);

const BASE_URL = "https://www.martyngerrard.co.uk";

const PROPERTY_TYPES = [
	{
		label: "SALES",
		isRental: false,
		totalPages: 15,
		buildUrl: (page) => `${BASE_URL}/property/for-sale/in-london/available/page-${page}/`,
	},
	{
		label: "RENTALS",
		isRental: true,
		totalPages: 3,
		buildUrl: (page) => `${BASE_URL}/property/to-rent/in-london/available/page-${page}/`,
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
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
// DETAIL DATA FETCHING (Gatsby page-data JSON — no browser needed)
// ============================================================================

async function scrapePropertyDetail(propertyUrl) {
	await sleep(300);

	try {
		// Derive Gatsby page-data URL from property URL path
		// e.g. https://www.martyngerrard.co.uk/property-for-sale/n2/bdc190339/
		//   -> https://www.martyngerrard.co.uk/page-data/property-for-sale/n2/bdc190339/page-data.json
		const path = new URL(propertyUrl).pathname; // "/property-for-sale/n2/bdc190339/"
		const pageDataUrl = `${BASE_URL}/page-data${path}page-data.json`;

		const response = await axios.get(pageDataUrl, { timeout: 15000 });
		const prop = response.data?.result?.data?.strapidata?.property;

		if (!prop) {
			logger.error(`No property data in page-data JSON: ${pageDataUrl}`);
			return { latitude: null, longitude: null };
		}

		return {
			latitude: prop.latitude ?? null,
			longitude: prop.longitude ?? null,
		};
	} catch (err) {
		logger.error(`Error fetching page-data for ${propertyUrl}: ${err.message}`);
		return { latitude: null, longitude: null };
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector(".property-card", { timeout: 15000 });
	} catch (e) {
		logger.error("Listing container not found", e, pageNum, label);
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();

			const cards = Array.from(document.querySelectorAll(".property-card"));

			for (const card of cards) {
				const anchor = card.querySelector("a");
				if (!anchor) continue;

				const href = anchor.getAttribute("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : `${window.location.origin}${href}`;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				const title = card.querySelector(".address-title")?.textContent?.trim() || "Property";

				// ".prop-title" contains e.g. "3 bed house" — extract first number as bedrooms
				const bedText = card.querySelector(".prop-title")?.textContent?.trim() || "";

				const priceRaw = card.querySelector(".price_qua_price")?.textContent?.trim() || "";
				const statusText = card.innerText || "";

				results.push({ link, title, bedText, priceRaw, statusText });
			}
			return results;
		} catch (e) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link) continue;

		if (isSoldProperty(property.statusText || "")) continue;

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.page(pageNum, label, `Skipping update (no price found): ${property.link}`, totalPages);
			continue;
		}

		const bedMatch = property.bedText.match(/(\d+)/);
		const bedrooms = bedMatch ? parseInt(bedMatch[1]) : null;

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "UNCHANGED";

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const { latitude, longitude } = await scrapePropertyDetail(property.link);

			await processPropertyWithCoordinates(
				property.link,
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				null,
				latitude,
				longitude,
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
			propertyAction = "CREATED";
		} else if (result.error) {
			propertyAction = "ERROR";
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
			await sleep(300);
		}
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
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: async (context) => {
			await handleListingPage(context);
		},
		failedRequestHandler({ request }) {
			logger.error(`Failed request: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeMartynGerrard() {
	logger.step("Starting Martyn Gerrard scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} (${type.totalPages} pages)`);
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			allRequests.push({
				url: type.buildUrl(pg),
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: type.label,
					totalPages: type.totalPages,
				},
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	} else {
		logger.warn("No requests to process.");
	}

	logger.step(
		`Completed Martyn Gerrard - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, RENTALS: ${counts.savedRentals}`);

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
		await scrapeMartynGerrard();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
