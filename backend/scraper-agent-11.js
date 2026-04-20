// VHHomes scraper using Playwright with Crawlee
// Agent ID: 11
// FINAL FIXED VERSION - Title + Bedrooms + Better Coordinates

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

const AGENT_ID = 11;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const PROPERTY_TYPES = [
	{
		baseUrl: "https://vhhomes.co.uk/property-search/?type=buy&status=available&per-page=10&sort=price-high&view=list",
		totalPages: 5,
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://vhhomes.co.uk/property-search/?type=rent&status=available&per-page=10&sort=price-high&view=list",
		totalPages: 2,
		isRental: true,
		label: "LETTINGS",
	},
];

const processedUrls = new Set();

// ============================================================================
// UTILITY
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return process.env.BROWSERLESS_WS_ENDPOINT || 
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;
}

// ============================================================================
// DETAIL PAGE - Coordinates (enhanced fallbacks)
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	await sleep(1000);
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, { waitUntil: "networkidle", timeout: 90000 });
		await detailPage.waitForTimeout(1500);

		const htmlContent = await detailPage.content();
		let coords = await extractCoordinatesFromHTML(htmlContent);

		// Stronger fallback if still null
		if (!coords.latitude || !coords.longitude) {
			coords = await detailPage.evaluate(() => {
				// 1. Data attributes
				let lat = document.querySelector('[data-lat], [data-latitude], [lat]')?.getAttribute('data-lat') ||
				          document.querySelector('[data-latitude]')?.getAttribute('data-latitude');
				let lng = document.querySelector('[data-lng], [data-longitude], [lng]')?.getAttribute('data-lng') ||
				          document.querySelector('[data-longitude]')?.getAttribute('data-longitude');

				// 2. Meta tags
				if (!lat) lat = document.querySelector('meta[property*="latitude"], meta[name*="latitude"]')?.content;
				if (!lng) lng = document.querySelector('meta[property*="longitude"], meta[name*="longitude"]')?.content;

				// 3. JSON in any script tag (most common on estate sites)
				const scripts = Array.from(document.scripts);
				for (const script of scripts) {
					const text = script.textContent || '';
					const latMatch = text.match(/"latitude"?\s*[:=]\s*([0-9.-]+)/i);
					const lngMatch = text.match(/"longitude"?\s*[:=]\s*([0-9.-]+)/i);
					if (latMatch && lngMatch) {
						return {
							latitude: parseFloat(latMatch[1]),
							longitude: parseFloat(lngMatch[1])
						};
					}
				}
				return { latitude: lat ? parseFloat(lat) : null, longitude: lng ? parseFloat(lng) : null };
			});
		}

		return { coords };
	} catch (error) {
		logger.error(`Detail page error: ${property.link}`, error.message);
		return { coords: { latitude: null, longitude: null } };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// LISTING PAGE - FIXED Title + Bedrooms
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector("._expertweb-property", { timeout: 25000 });
	} catch (e) {
		logger.warn(`No properties found on page ${pageNum}`, label);
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const cards = Array.from(document.querySelectorAll("._expertweb-property"));

		for (const card of cards) {
			// === TITLE (Fixed) ===
			const nameContainer = card.querySelector("._expertweb-property-name");
			const linkElem = nameContainer 
				? nameContainer.querySelector('a[href*="/properties/"]') 
				: card.querySelector('a[href*="/properties/"]');
			
			const title = linkElem ? linkElem.textContent.trim() : "Unknown Property";

			// === PRICE ===
			const priceElem = card.querySelector("._expertweb-property-price");
			const priceRaw = priceElem ? priceElem.textContent.trim() : "";
			if (!priceRaw) continue;

			// === STATUS ===
			const statusElem = card.querySelector("._expertweb-property-status");
			const statusText = statusElem ? statusElem.textContent.trim() : "";

			// === BEDROOMS (Fixed) ===
			let bedrooms = null;
			const roomsContainer = card.querySelector("._expertweb-property-rooms");
			if (roomsContainer) {
				// Look for the span that contains the bed icon
				const bedIcon = roomsContainer.querySelector('i.fa-bed, .fas.fa-bed');
				if (bedIcon) {
					const parentSpan = bedIcon.parentElement;
					const numMatch = parentSpan.textContent.match(/\d+/);
					if (numMatch) bedrooms = parseInt(numMatch[0], 10);
				} else {
					// Fallback text match
					const text = roomsContainer.textContent || "";
					const match = text.match(/(\d+)\s*(?:bed|bedroom|🛏)/i);
					if (match) bedrooms = parseInt(match[1], 10);
				}
			}

			results.push({
				link: linkElem ? linkElem.href : null,
				title,
				bedrooms,
				statusText,
				priceRaw,
			});
		}
		return results;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link || processedUrls.has(property.link)) continue;
		if (isSoldProperty(property.statusText || "")) continue;

		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		if (!price) continue;

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental
		);

		let propertyAction = "UNCHANGED";

		if (result.updated) propertyAction = "UPDATED";

		if (!result.isExisting && !result.error) {
			const detail = await scrapePropertyDetail(page.context(), property);

			await processPropertyWithCoordinates(
				property.link.trim(),
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
				null,
				detail.coords.latitude || null,
				detail.coords.longitude || null
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
			propertyAction = "CREATED";
		} else if (result.isExisting && result.updated) {
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
		}

		logger.property(
			pageNum,
			label,
			property.title.substring(0, 45),
			formatPriceDisplay(price, isRental),
			property.link,
			isRental,
			totalPages,
			propertyAction
		);

		if (propertyAction !== "UNCHANGED") await sleep(600);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		launchContext: {
			launchOptions: { browserWSEndpoint, args: ["--no-sandbox", "--disable-setuid-sandbox"] }
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeVHHomes() {
	logger.step("Starting VHHomes scraper - FINAL FIXED VERSION...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless...`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} (${type.totalPages} pages)`);
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			allRequests.push({
				
				url: `${type.baseUrl}&ppage=${pg}`,
				userData: { pageNum: pg, isRental: type.isRental, label: type.label, totalPages: type.totalPages }
			});
		}
	}

	await crawler.run(allRequests);

	logger.step(`Completed! Scraped: ${counts.totalScraped} | Saved: ${counts.totalSaved} | Sales: ${counts.savedSales} | Rentals: ${counts.savedRentals}`);

	if (!isPartialRun) await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

scrapeVHHomes()
	.then(() => { logger.step("All done!"); process.exit(0); })
	.catch((error) => { logger.error("Unhandled error", error); process.exit(1); });