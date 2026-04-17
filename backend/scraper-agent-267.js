// Oakley Property Scraper
// Agent ID: 267
// Agent Name: Oakley Property
// Updated: 17 April 2026 - Fixed Lettings Price Extraction

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

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 267;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://oakleyproperty.com/residential-properties?section=residential&isLet=0&isUnavailableSale=0&isUnavailableLet=1",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://oakleyproperty.com/residential-properties?section=residential&isLet=1&isUnavailableSale=0&isUnavailableLet=0",
		isRental: true,
		label: "LETTINGS",
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

function getBrowserlessEndpoint() {
	return process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;
}

function cleanPropertyUrl(url) {
	return url.split('?')[0];
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	await sleep(1200);
	const detailPage = await browserContext.newPage();
	const cleanUrl = cleanPropertyUrl(property.link);

	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(cleanUrl, { waitUntil: "domcontentloaded", timeout: 60000 });

		await detailPage.waitForTimeout(2500);

		const detailInfo = await detailPage.evaluate(() => {
			let latitude = null, longitude = null, bedroomsCount = null;

			const html = document.documentElement.innerHTML;

			// Coordinates
			const coordMatch = html.match(/["']latLng["']:\s*\[\s*["']?([\d.-]+)["']?,\s*["']?([\d.-]+)["']?\s*\]/);
			if (coordMatch) {
				latitude = parseFloat(coordMatch[1]);
				longitude = parseFloat(coordMatch[2]);
			}

			// Bedrooms from specs
			const specs = document.querySelectorAll('.property-specs .property-specs_value');
			if (specs.length > 0) {
				const first = specs[0].textContent.trim();
				if (/^\d+$/.test(first)) bedroomsCount = parseInt(first, 10);
			}

			// Text fallback for bedrooms
			if (!bedroomsCount) {
				const text = document.body.textContent;
				const match = text.match(/(\d+)\s*(?:bedroom|bed)s?/i);
				if (match) bedroomsCount = parseInt(match[1], 10);
			}

			return { latitude, longitude, bedroomsCount };
		});

		return detailInfo;

	} catch (error) {
		logger.error(`Detail page error ${cleanUrl}: ${error.message}`);
		return { latitude: null, longitude: null, bedroomsCount: null };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// LISTING PAGE HANDLER - FIXED FOR LETTINGS
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label } = request.userData;
	logger.step(`Processing ${label} page: ${request.url}`);

	try {
		await page.waitForLoadState("networkidle", { timeout: 45000 }).catch(() => {});
		await page.waitForTimeout(3000);

		await page.waitForSelector('ul.property-results li', { timeout: 25000 }).catch(() => {});
	} catch (e) {
		logger.error(`Load timeout on ${label}`);
	}

	const properties = await page.evaluate((isRental) => {
		const results = [];
		const items = document.querySelectorAll('ul.property-results li');

		items.forEach((item) => {
			const linkEl = item.querySelector('a.property-card, a[href*="/residential-properties/"]');
			if (!linkEl) return;

			const link = linkEl.href;
			const title = item.querySelector('h2.property-card_heading')?.textContent.trim() || "Property";

			// === IMPROVED PRICE EXTRACTION (Critical for Lettings) ===
			let priceRaw = "";

			// Sales price selector
			if (!isRental) {
				priceRaw = item.querySelector('.property-info_price strong')?.textContent.trim() || "";
			} 
			// Lettings price selectors (most common on lettings page)
			else {
				priceRaw = 
					item.querySelector('.property-info_price strong')?.textContent.trim() ||
					item.querySelector('.price, .property-price, .rent-price')?.textContent.trim() ||
					item.querySelector('.property-card_price')?.textContent.trim() ||
					"";
			}

			// Extract bedrooms
			let bedroomsFromSnippet = null;
			const specValue = item.querySelector('.property-specs_value');
			if (specValue) {
				const val = parseInt(specValue.textContent.trim(), 10);
				if (!isNaN(val) && val > 0) bedroomsFromSnippet = val;
			}

			if (!bedroomsFromSnippet) {
				const descText = item.textContent || "";
				const bedMatch = descText.match(/(\d+)\s*bed/i);
				if (bedMatch) bedroomsFromSnippet = parseInt(bedMatch[1], 10);
			}

			const flag = item.querySelector('.property-flag')?.textContent.trim() || "";

			results.push({ link, title, priceRaw, bedroomsFromSnippet, flag });
		});

		return results;
	}, isRental);   // ← Pass isRental to evaluate

	logger.step(`Found ${properties.length} ${label} properties`);

	for (const property of properties) {
		if (!property.link || processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		if (isSoldProperty(property.flag) || property.flag.toLowerCase().includes("sold") || property.flag.toLowerCase().includes("let")) {
			continue;
		}

		const price = parsePrice(property.priceRaw);

		if (!price) {
			logger.property(1, label, property.title.substring(0, 40), "N/A", property.link, isRental, 1, "SKIPPED (No Price)");
			continue;
		}

		let bedrooms = property.bedroomsFromSnippet;

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental
		);

		let action = "UNCHANGED";
		let lat = null, lng = null;

		if (result.updated) {
			counts.totalSaved++;
			action = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const details = await scrapePropertyDetail(page.context(), property);
			lat = details.latitude;
			lng = details.longitude;
			if (details.bedroomsCount) bedrooms = details.bedroomsCount;

			await processPropertyWithCoordinates(
				property.link, price, property.title, bedrooms, AGENT_ID, isRental, null, lat, lng
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++; else counts.savedSales++;
			action = "CREATED";
		}

		logger.property(
			1, label, property.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			property.link, isRental, 1, action, lat, lng
		);

		if (action !== "UNCHANGED") await sleep(700);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 120,
		requestHandlerTimeoutSecs: 900,
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: handleListingPage,
	});
}

async function scrapeOakleyProperty() {
	logger.step("Starting Oakley Property scraper...");

	const scrapeStartTime = new Date();
	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = PROPERTY_TYPES.map(type => ({
		url: type.baseUrl,
		userData: { isRental: type.isRental, label: type.label }
	}));

	await crawler.run(allRequests);

	logger.step(`Completed - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, LETTINGS: ${counts.savedRentals}`);

	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
	try {
		await scrapeOakleyProperty();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();