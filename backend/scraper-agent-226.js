const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 226;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
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

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
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
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental, pageNum, label) {
	await sleep(2000 + Math.random() * 1500); // Give map time to load

	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "networkidle0", // stricter: wait until no network activity
			timeout: 60000,
		});

		// Extra wait for Google Maps to initialize
		await sleep(5000);

		const detailData = await detailPage.evaluate(() => {
			const result = {
				lat: null,
				lng: null,
				bedrooms: null,
			};

			// === COORDINATES ===

			// 1. Broad search in all scripts for any lat/lng pair
			const allScripts = document.documentElement.innerHTML;
			const coordPatterns = [
				/lat(?:itude)?\s*[:=]\s*([-+]?[\d.]+).*?lng(?:itude)?\s*[:=]\s*([-+]?[\d.]+)/is,
				/lng(?:itude)?\s*[:=]\s*([-+]?[\d.]+).*?lat(?:itude)?\s*[:=]\s*([-+]?[\d.]+)/is,
				/new\s+LatLng\s*\(\s*([-+]?[\d.]+)\s*,\s*([-+]?[\d.]+)\s*\)/i,
				/center\s*:\s*new\s+LatLng\s*\(\s*([-+]?[\d.]+)\s*,\s*([-+]?[\d.]+)\s*\)/i,
				/position\s*:\s*{\s*lat\s*:\s*([-+]?[\d.]+).*?lng\s*:\s*([-+]?[\d.]+)/i,
				/marker\.setPosition\({lat:\s*([-+]?[\d.]+).*?lng:\s*([-+]?[\d.]+)/i,
			];

			for (const regex of coordPatterns) {
				const match = allScripts.match(regex);
				if (match) {
					result.lat = parseFloat(match[1]);
					result.lng = parseFloat(match[2]);
					if (result.lat && result.lng) break;
				}
			}

			// 2. Google Maps marker or map init fallback
			if (!result.lat || !result.lng) {
				const markerMatch = allScripts.match(/marker\s*=\s*new\s+google\.maps\.Marker\s*\({.*?position\s*:\s*([-+]?[\d.]+).*?([-+]?[\d.]+)/is);
				if (markerMatch) {
					result.lat = parseFloat(markerMatch[1]);
					result.lng = parseFloat(markerMatch[2]);
				}
			}

			// === BEDROOMS ===

			// Search entire page text for bedroom count
			const bodyText = document.body.innerText.toLowerCase();
			const bedRegexes = [
				/(\d+)\s*(?:bed|bedroom|bedrooms)\b/i,
				/bed(?:room)?s?\s*[:\-]\s*(\d+)/i,
				/(\d+)\s*bed(?:room)?\b/i,
				/number of bedrooms\s*(\d+)/i,
			];

			for (const regex of bedRegexes) {
				const match = bodyText.match(regex);
				if (match) {
					result.bedrooms = parseInt(match[1], 10);
					break;
				}
			}

			return result;
		});

		const htmlContent = await detailPage.content();

		// Debug log – keep this for now
		logger.step(
			`[DETAIL ${pageNum}/${label}] "${property.title}": ` +
			`lat=${detailData.lat ?? 'NULL'}, lng=${detailData.lng ?? 'NULL'}, beds=${detailData.bedrooms ?? 'NULL'}`
		);

		// Safety check: prevent null price crash
		if (!property.price || isNaN(property.price) || property.price <= 0) {
			logger.error(`Skipping DB insert - invalid price (${property.price}) for ${property.link}`, null, pageNum, label);
			return null;
		}

		await processPropertyWithCoordinates(
			property.link.trim(),
			property.price,
			property.title,
			detailData.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			detailData.lat,
			detailData.lng
		);

		return detailData;
	} catch (error) {
		logger.error(`Detail scrape failed → ${property.link}`, error.message || error, pageNum || 'unknown', label);
		return null;
	} finally {
		await detailPage.close().catch(() => { });
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNum = 1, totalPages = 50 } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	await page.waitForTimeout(2000);
	await page.waitForSelector('.property, .property-card, a[href*="/property/"]', { timeout: 30000 }).catch(() => { });

	// Extract properties
	const properties = await page.evaluate(() => {
		try {
			const items = Array.from(document.querySelectorAll(".property-card, .search-items li, .property, .row.property"));
			return items.map((el) => {
				const linkTag = el.querySelector('a[href*="/property/"]');
				const priceText = el.querySelector(".price, .property-price, .price-display, .list-price")?.innerText || "";
				const title = el.querySelector(".address, .property-address, .address-display, .list-address")?.innerText || "Property";
				const statusText = el.querySelector(".property-status, .status, .label")?.innerText?.trim() || "";
				return { link: linkTag?.href, priceText, title, status: statusText };
			}).filter((p) => p.link);
		} catch (err) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);
	stats.totalFound += properties.length;

	for (const property of properties) {
		try {
			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			stats.totalScraped++;

			if (isSoldProperty(property.status)) {
				stats.totalSkipped++;
				logger.property(
					pageNum,
					label,
					property.title.substring(0, 40),
					formatPriceDisplay(null, isRental),
					property.link,
					isRental,
					totalPages,
					"SKIPPED",
				);
				continue;
			}

			const price = parsePrice(property.priceText);
			if (price === null || price === 0) {
				stats.totalSkipped++;
				continue;
			}

			const result = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				price,
				property.title,
				null, // bedrooms extracted later or from detail
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";
			if (result.updated) {
				stats.totalSaved++;
				propertyAction = "UPDATED";
				if (isRental) stats.savedRentals++;
				else stats.savedSales++;
			}

			let lat = null;
			let lng = null;
			let bedrooms = null;

			if (!result.isExisting && !result.error) {
				logger.step(`New property → scraping detail: ${property.title}`, pageNum, label);

				// Try to get bedrooms from listing if not already set
				let bedroomsFromListing = null;
				// If you can extract it earlier in page.evaluate, add it to property object

				const detail = await scrapePropertyDetail(
					page.context(),
					{ ...property, bedrooms: bedroomsFromListing },
					isRental,
					pageNum,
					label
				);

				if (detail) {
					lat = detail.lat;
					lng = detail.lng;
					bedrooms = detail.bedrooms || bedroomsFromListing;

					await processPropertyWithCoordinates(
						property.link.trim(),
						price,
						property.title,
						bedrooms,
						AGENT_ID,
						isRental,
						detail.html,
						lat,
						lng
					);

					stats.totalSaved++;
					propertyAction = "CREATED";
					if (isRental) stats.savedRentals++;
					else stats.savedSales++;
				}
			} else if (result.error) {
				propertyAction = "ERROR";
				stats.totalSkipped++;
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
				lat,
				lng,
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500);
			}
		} catch (err) {
			logger.error(`Error processing property ${property.link}: ${err.message}`, err, pageNum, label);
		}
	}

	// Handle pagination (only on the first page to avoid re-queueing)
	if (pageNum === 1) {
		const maxPageNum = await page.evaluate(() => {
			const paginationLinks = Array.from(document.querySelectorAll(".pagination a"));
			let highest = 1;
			paginationLinks.forEach((a) => {
				const val = parseInt(a.innerText.trim());
				if (!isNaN(val) && val > highest) highest = val;
			});
			return highest;
		});

		if (maxPageNum > 1) {
			logger.step(`Found ${maxPageNum} pages for ${label}. Queueing remaining pages...`);
			for (let p = 2; p <= maxPageNum; p++) {
				const pageUrl = `${request.url}${request.url.includes("?") ? "&" : "?"}page=${p}`;
				await crawler.addRequests([
					{
						url: pageUrl,
						userData: { isRental, label, pageNum: p, totalPages: maxPageNum },
					},
				]);
			}
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
		requestHandlerTimeoutSecs: 600,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			const { pageNum, label } = request.userData || {};
			logger.error(`Failed listing page: ${request.url}`, null, pageNum, label);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapePalmerPartners() {
	const scrapeStartTime = new Date();
	const startPage = getStartPage();
	const isPartialRun = startPage > 1;

	logger.step(`Starting Palmer Partners scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const PROPERTY_TYPES = [
		{
			urlBase: "https://www.palmerpartners.com/buy/property-for-sale/",
			isRental: false,
			label: "SALES",
		},
		{
			urlBase: "https://www.palmerpartners.com/let/property-to-let/",
			isRental: true,
			label: "RENTALS",
		},
	];

	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label}...`);
		const crawler = createCrawler(browserWSEndpoint);
		await crawler.addRequests([
			{
				url: propertyType.urlBase,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
					pageNum: startPage,
					totalPages: startPage // Will be updated during pagination detection
				},
			},
		]);
		await crawler.run();
	}

	logger.step(
		`Completed Palmer Partners Agent ${AGENT_ID}`,
		`found=${stats.totalFound}, scraped=${stats.totalScraped}, saved=${stats.totalSaved}, skipped=${stats.totalSkipped}, sales=${stats.savedSales}, rentals=${stats.savedRentals}`,
	);

	if (!isPartialRun) {
		logger.step(`Updating remove status...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

(async () => {
	try {
		await scrapePalmerPartners();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
