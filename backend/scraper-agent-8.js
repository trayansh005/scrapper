// Jackie Quinn scraper using Playwright with Crawlee
// Agent ID: 8
// Website: www.jackiequinn.co.uk
// Updated: Fixed title, bedrooms, and coordinates extraction

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 8;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedLettings: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================
function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

// ============================================================================
// DETAIL PAGE - IMPROVED COORDINATES EXTRACTION
// ============================================================================
async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// Wait for property content
		await detailPage.waitForSelector('.card__content, .property-description, #map', {
			timeout: 15000,
		}).catch(() => { });

		// Scroll to map area if exists
		await detailPage.evaluate(() => {
			const mapEl = document.querySelector('#map, .map, [class*="map"]');
			if (mapEl) mapEl.scrollIntoView({ behavior: "smooth" });
		});

		await detailPage.waitForTimeout(1500); // Give map time to load

		const coords = await detailPage.evaluate(() => {
			// Strategy 1: Window global objects
			const dataSources = [
				window.mapData,
				window.propertyData,
				window.__PRELOADED_STATE__,
				window.__INITIAL_STATE__,
				window.__APP_STATE__,
				window.__REACT_QUERY_STATE__
			];

			for (const data of dataSources) {
				if (data && typeof data === 'object') {
					const lat = data.latitude || data.lat || data.coords?.latitude || data.coords?.lat || data.location?.lat;
					const lng = data.longitude || data.lng || data.coords?.longitude || data.coords?.lng || data.location?.lng;
					if (lat && lng) {
						return { lat: parseFloat(lat), lng: parseFloat(lng) };
					}
				}
			}

			// Strategy 2: Search all scripts for JSON containing lat/lng (most reliable for this site)
			const scripts = Array.from(document.querySelectorAll('script'));
			for (const script of scripts) {
				const text = script.textContent || '';
				if (!text.includes('latitude') && !text.includes('lat') && !text.includes('lng')) continue;

				const latMatch = text.match(/"latitude"\s*:\s*(-?\d+\.?\d*)/i) ||
					text.match(/"lat"\s*:\s*(-?\d+\.?\d*)/i) ||
					text.match(/lat["']\s*:\s*(-?\d+\.?\d*)/i);

				const lngMatch = text.match(/"longitude"\s*:\s*(-?\d+\.?\d*)/i) ||
					text.match(/"lng"\s*:\s*(-?\d+\.?\d*)/i) ||
					text.match(/lng["']\s*:\s*(-?\d+\.?\d*)/i);

				if (latMatch && lngMatch) {
					return {
						lat: parseFloat(latMatch[1]),
						lng: parseFloat(lngMatch[1])
					};
				}
			}

			// Strategy 3: Google Maps iframe
			const iframe = document.querySelector('iframe[src*="google.com/maps"], iframe[src*="maps.googleapis"]');
			if (iframe) {
				const src = iframe.src;
				const match = src.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/) ||
					src.match(/q=(-?\d+\.\d+),(-?\d+\.\d+)/);
				if (match) {
					return { lat: parseFloat(match[1]), lng: parseFloat(match[2]) };
				}
			}

			// Strategy 4: Data attributes
			const mapDiv = document.querySelector('#map, .map-container, [class*="map"]');
			if (mapDiv) {
				const lat = mapDiv.dataset.lat || mapDiv.dataset.latitude || mapDiv.getAttribute('data-lat');
				const lng = mapDiv.dataset.lng || mapDiv.dataset.longitude || mapDiv.getAttribute('data-lng');
				if (lat && lng) {
					return { lat: parseFloat(lat), lng: parseFloat(lng) };
				}
			}

			return null;
		});

		const htmlContent = await detailPage.content();

		const dbResult = await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			coords ? coords.lat : null,
			coords ? coords.lng : null
		);

		return coords || { latitude: null, longitude: null };

	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
		return { latitude: null, longitude: null };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// LISTING PAGE HANDLER - FIXED PARSING
// ============================================================================
async function handleListingPage({ page, request }) {
	const { pageNumber, totalPages, isRental, label } = request.userData;

	logger.page(pageNumber, label, request.url, totalPages);

	// Wait for cards to load
	await page.waitForSelector('.card.card--grid', { timeout: 30000 }).catch(() => { });

	// Scroll to load all cards
	await page.evaluate(async () => {
		await new Promise((resolve) => {
			let totalHeight = 0;
			const distance = 800;
			const timer = setInterval(() => {
				window.scrollBy(0, distance);
				totalHeight += distance;
				if (totalHeight >= document.body.scrollHeight - 1000) {
					clearInterval(timer);
					resolve();
				}
			}, 150);
		});
	});


	// Extract properties with improved logic
	const properties = await page.evaluate(() => {
		const results = [];
		const cards = document.querySelectorAll('.card.card--grid:not(.card--property-worth)');

		cards.forEach(card => {
			const linkEl = card.querySelector('a[href*="/property/"]');
			if (!linkEl) return;

			const link = linkEl.href;

			// Clean title - remove "New" badge and extra text
			let title = '';
			const titleEl = card.querySelector('.card__title');
			if (titleEl) {
				title = titleEl.textContent.trim();
			} else {
				title = linkEl.textContent.trim();
			}
			// Remove "New" if it appears at the end
			title = title.replace(/^New\s+/i, '').replace(/\s+New\s*$/i, '').trim();

			// Price
			const priceEl = card.querySelector('.card__price .price-value');
			let priceText = priceEl ? priceEl.textContent.trim() : '';
			if (!priceText) {
				const priceMatch = card.textContent.match(/£[\d,]+/);
				priceText = priceMatch ? priceMatch[0] : '';
			}

			// Bedrooms
			let bedrooms = null;
			const bedEl = card.querySelector('.fa-bed-front + span.number, .card__info__item span.number');
			if (bedEl) {
				const num = parseInt(bedEl.textContent.trim(), 10);
				if (!isNaN(num)) bedrooms = num;
			}

			if (link && priceText) {
				results.push({
					link,
					title,
					price: priceText,
					bedrooms
				});
			}
		});

		return results;
	});

	logger.page(
		pageNumber,
		label,
		`Found ${properties.length} properties on page ${pageNumber}`,
		totalPages
	);

	for (const property of properties) {
		stats.totalScraped++;

		if (!property.link) continue;
		if (processedUrls.has(property.link)) {
			logger.property(pageNumber, label, property.title.substring(0, 40),
				formatPriceDisplay(property.price, isRental), property.link, isRental, totalPages, "SKIPPED");
			continue;
		}

		processedUrls.add(property.link);

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,           // ← Now properly extracted
			AGENT_ID,
			isRental
		);

		let propertyAction = "UNCHANGED";
		let coords = { latitude: null, longitude: null };

		if (result.updated) propertyAction = "UPDATED";
		if (!result.isExisting && !result.error) {
			propertyAction = "CREATED";
			coords = await scrapePropertyDetail(page.context(), property, isRental);
		}

		if (propertyAction === "CREATED") {
			stats.totalSaved++;
			if (isRental) stats.savedLettings++;
			else stats.savedSales++;
		}

		logger.property(
			pageNumber,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(property.price, isRental),
			property.link,
			isRental,
			totalPages,
			propertyAction,
			coords.latitude,
			coords.longitude
		);

		if (propertyAction === "CREATED") {
			await sleep(300);// Be gentle
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
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN FUNCTION
// ============================================================================
async function scrapeJackieQuinn() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Jackie Quinn scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const requests = [];

	// SALES (2 pages)
	const totalSalesPages = 2;

	for (let i = 1; i <= totalSalesPages; i++) {
		requests.push({
			url: `https://www.jackiequinn.co.uk/properties-for-sale/property/any-bed/all-location?exclude=1&page=${i}`,
			userData: {
				pageNumber: i,
				totalPages: totalSalesPages,
				isRental: false,
				label: "SALES",
			},
		});
	}

	// LETTINGS (only 1 page)
	requests.push({
		url: "https://www.jackiequinn.co.uk/property-to-rent",
		userData: {
			pageNumber: 1,
			totalPages: 1,
			isRental: true,
			label: "LETTINGS",
		},
	});

	logger.step(`Queueing ${requests.length} listing pages...`);
	await crawler.run(requests);

	logger.step(
		`Completed Jackie Quinn - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`
	);

	if (!isPartialRun) {
		logger.step("Updating remove status for properties not seen...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run - skipping updateRemoveStatus");
	}
}

// Run the scraper
(async () => {
	try {
		await scrapeJackieQuinn();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();