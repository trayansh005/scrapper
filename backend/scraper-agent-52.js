// Haart property scraper using Playwright with Crawlee
// Agent ID: 52
// Usage:
// node backend/scraper-agent-52.js [startPage]
//
// Coordinate strategy: pre-fetches all property data (incl. Location: {lat, lon})
// from the Haart FindNearestSRProperties API in one call at startup, then looks up
// coordinates by URL during listing page processing — no detail page visits needed.

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 52;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl:
			"https://www.haart.co.uk/property-results/?IsPurchase=true&Location=London,%20Greater%20London&SearchDistance=50&Latitude=51.51437&Longitude=-0.09229&MinPrice=0&MaxPrice=100000000&MinimumBeds=0&SortBy=HighestPrice&NumberOfResults=6",
		totalPages: 27,
		isRental: false,
		label: "SALES",
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
};

const processedUrls = new Set();

// Keyed by normalised path (no domain, no trailing slash, lowercase)
// e.g. "/buying/4-bedroom-house-for-sale/london-se19/hrt013111416"
const coordsMap = new Map();

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

function normaliseSeoUrl(urlOrPath) {
	try {
		const path = urlOrPath.startsWith("http") ? new URL(urlOrPath).pathname : urlOrPath;
		return path.toLowerCase().replace(/\/+$/, "");
	} catch {
		return urlOrPath.toLowerCase().replace(/\/+$/, "");
	}
}

// ============================================================================
// API PRE-FETCH — builds coordsMap from FindNearestSRProperties
// ============================================================================

async function buildCoordsMap(isPurchase) {
	const label = isPurchase ? "SALES" : "RENTALS";
	logger.step(`Pre-fetching ${label} property data from Haart API...`);

	try {
		const response = await fetch(
			"https://www.haart.co.uk/umbraco/api/PropertySearch/FindNearestSRProperties",
			{
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({
					Lat: 51.51437,
					Lon: -0.09229,
					Brand: "HRT",
					IsPurchase: isPurchase,
					Distance: "50miles",
					Sort: "distance",
				}),
			},
		);

		if (!response.ok) {
			logger.step(`API pre-fetch failed with status ${response.status} — coords will be null`);
			return;
		}

		const data = await response.json();
		const results = data.Results || [];

		let mapped = 0;
		for (const item of results) {
			if (!item.SeoUrl) continue;
			const key = normaliseSeoUrl(item.SeoUrl);
			let lat = null;
			let lon = null;
			try {
				const loc = typeof item.Location === "string" ? JSON.parse(item.Location) : item.Location;
				if (loc && loc.lat != null && loc.lon != null) {
					lat = parseFloat(loc.lat);
					lon = parseFloat(loc.lon);
				}
			} catch (e) {
				// skip malformed location
			}
			coordsMap.set(key, { latitude: lat, longitude: lon });
			mapped++;
		}

		logger.step(`API pre-fetch complete — ${mapped} properties mapped (${label})`);
	} catch (err) {
		logger.step(`API pre-fetch error — coords will be null: ${err.message}`);
	}
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
		await page.waitForSelector(".property-box", { timeout: 15000 });
	} catch (e) {
		logger.error("Property box container not found", e, pageNum, label);
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();

			// Find all property boxes - they contain property info
			const propertyBoxes = Array.from(document.querySelectorAll(".property-box"));

			for (const box of propertyBoxes) {
				const anchor = box.querySelector("a");
				if (!anchor) continue;

				let href = anchor.getAttribute("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				// Extract title
				const title = box.querySelector(".propAida")?.textContent?.trim() || "Property";

				// Extract price
				let priceRaw = "";
				const priceEl = box.querySelector(".propPrice");
				if (priceEl) {
					priceRaw = priceEl.textContent.trim();
				}

				// Extract bedrooms
				let bedText = "";
				const bedEl = box.querySelector(".propBeds");
				if (bedEl) {
					bedText = bedEl.textContent.trim();
				}

				const statusText = box.innerText || "";

				results.push({ link, title, priceRaw, bedText, statusText });
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
		let bedrooms = null;
		const bedMatch = property.bedText.match(/(\d+)\s*bedroom/i);
		if (bedMatch) bedrooms = parseInt(bedMatch[1]);

		if (!price) {
			logger.page(pageNum, label, `Skipping update (no price found): ${property.link}`, totalPages);
			continue;
		}

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
			// Coords pre-fetched from FindNearestSRProperties API — no detail page visit needed
			const key = normaliseSeoUrl(property.link);
			const coords = coordsMap.get(key) || { latitude: null, longitude: null };

			await processPropertyWithCoordinates(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				null,
				coords.latitude,
				coords.longitude,
			);

			counts.totalSaved++;
			counts.totalScraped++;
			counts.savedSales++;
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
			await sleep(500);
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
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeHaart() {
	logger.step("Starting Haart scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	// Pre-fetch all property coords from Haart API (one call, no browser needed)
	await buildCoordsMap(true); // SALES

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} (${type.totalPages} pages)`);
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			allRequests.push({
				url: `${type.baseUrl}&Page=${pg}&Stc=False&OnMkt=True&PropertyTypes=0`,
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
		`Completed Haart - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}`);

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
		await scrapeHaart();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
