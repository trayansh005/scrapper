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
// DETAIL PAGE SCRAPING (UPDATED FOR RELIABILITY)
// ============================================================================
async function scrapePropertyDetail(browserContext, property, isRental) {
	await sleep(1000);
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, {
			waitUntil: "networkidle",
			timeout: 60000,
		});

		const detailData = await detailPage.evaluate(() => {
			const data = { lat: null, lng: null, bedrooms: null };

			// ────────────────────────────────────────────────
			// 1. Try original hidden input logic first (for legacy pages where it works)
			// ────────────────────────────────────────────────
			try {
				const bedsInput = document.querySelector('input[name="beds"]');
				if (bedsInput) data.bedrooms = bedsInput.value;

				const allHiddenInputs = Array.from(document.querySelectorAll('input[type="hidden"]'));
				for (const input of allHiddenInputs) {
					const val = input.value?.trim() || "";
					if (val.startsWith("[") && val.includes('"lat"') && val.includes('"lng"')) {
						try {
							const coords = JSON.parse(val);
							if (coords?.[0]) {
								data.lat = coords[0].lat;
								data.lng = coords[0].lng;
								if (!data.bedrooms && coords[0].beds) data.bedrooms = coords[0].beds;
							}
						} catch (e) {
							// Ignore parse errors
						}
					}
				}
			} catch (e) {
				// Ignore evaluation errors
			}

			// ────────────────────────────────────────────────
			// 2. Extract coordinates from inline script: var properties = [...]
			// ────────────────────────────────────────────────
			if (!data.lat || !data.lng) {

				const scripts = Array.from(document.querySelectorAll("script"));

				for (const script of scripts) {

					const text = script.textContent || "";

					if (text.includes("var properties")) {

						try {

							const match = text.match(/var properties\s*=\s*(\[[\s\S]*?\]);/);

							if (match) {

								const json = JSON.parse(match[1]);

								if (json[0]) {

									const prop = json[0];

									data.lat = parseFloat(prop.latitude || prop.coordinates?.lat);
									data.lng = parseFloat(prop.longitude || prop.coordinates?.lng);

									if (prop.NumberBedrooms) {
										data.bedrooms = String(prop.NumberBedrooms);
									} else if (prop.beds) {
										data.bedrooms = String(prop.beds);
									}

									break;
								}

							}

						} catch (e) {
							// ignore parsing error
						}

					}

				}

			}

			// ────────────────────────────────────────────────
			// 3. Fallback: Regex scan scripts for coords/beds (robust against minor changes)
			// ────────────────────────────────────────────────
			if (!data.lat || !data.lng || !data.bedrooms) {
				const scripts = Array.from(document.querySelectorAll('script'));
				for (const script of scripts) {
					const text = script.textContent || '';
					if (text.includes('latitude') || text.includes('longitude') || text.includes('beds')) {
						// Extract lat/lng with regex
						const latMatch = text.match(/"?latitude"?\s*:\s*["']?(-?\d+\.?\d+)["']?/i);
						const lngMatch = text.match(/"?longitude"?\s*:\s*["']?(-?\d+\.?\d+)["']?/i);
						const bedsMatch = text.match(/"?(?:NumberBedrooms|beds)"?\s*:\s*["']?(\d+)["']?/i);

						if (latMatch && !data.lat) data.lat = parseFloat(latMatch[1]);
						if (lngMatch && !data.lng) data.lng = parseFloat(lngMatch[1]);
						if (bedsMatch && !data.bedrooms) data.bedrooms = bedsMatch[1];
					}
				}
			}

			// ────────────────────────────────────────────────
			// 4. Last resort: Visible text for bedrooms (e.g., from stats icons or title)
			// ────────────────────────────────────────────────
			if (!data.bedrooms) {
				const bedsSelectors = [
					'.FeaturedProperty__list-stats-item--bedrooms span',
					'.list-stats .bedroom span',
					'[title="Bedrooms"] span',
					'.property-stats .beds'
				];
				for (const selector of bedsSelectors) {
					const bedsEl = document.querySelector(selector);
					if (bedsEl) {
						const text = bedsEl.textContent.trim();
						const match = text.match(/(\d+)\s*(?:bed|Bedroom|bedroom)/i);
						if (match) {
							data.bedrooms = match[1];
							break;
						}
					}
				}
			}

			return data;
		});

		const htmlContent = await detailPage.content();
		return {
			coords: {
				latitude: detailData?.lat || null,
				longitude: detailData?.lng || null,
			},
			bedrooms: detailData?.bedrooms || null,
			html: htmlContent,
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
		return null;
	} finally {
		await detailPage.close();
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

			// Skip sold properties
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
					"SKIPPED"
				);

				continue;
			}

			// Parse price
			const price = parsePrice(property.priceText);
			if (price === null || price === 0) {
				stats.totalSkipped++;
				continue;
			}

			let lat = null;
			let lng = null;
			let bedrooms = null;

			// Check if property exists and update price if needed
			const result = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				price,
				property.title,
				null,
				AGENT_ID,
				isRental
			);

			let propertyAction = "UNCHANGED";

			// Handle DB error
			if (result.error) {
				propertyAction = "ERROR";
				stats.totalSkipped++;
			}

			// Existing property → only update price
			else if (result.isExisting) {

				if (result.updated) {
					stats.totalSaved++;
					propertyAction = "UPDATED";

					if (isRental) stats.savedRentals++;
					else stats.savedSales++;
				}

				// NEW FIX → fetch coordinates if missing
				const detail = await scrapePropertyDetail(
					page.context(),
					{ ...property, price },
					isRental
				);

				if (detail && (detail.coords.latitude || detail.coords.longitude || detail.bedrooms)) {

					lat = detail.coords.latitude;
					lng = detail.coords.longitude;
					bedrooms = detail.bedrooms;

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
				}
			}

			// New property → scrape detail page and save coordinates
			else {
				const detail = await scrapePropertyDetail(
					page.context(),
					{ ...property, price },
					isRental
				);

				if (detail) {
					lat = detail.coords.latitude;
					lng = detail.coords.longitude;
					bedrooms = detail.bedrooms;

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
			}

			// Logging
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
				lng
			);

			// Small delay to reduce load
			if (propertyAction !== "UNCHANGED") {
				await sleep(500);
			}

		} catch (err) {
			logger.error(
				`Error processing property ${property.link}: ${err.message}`,
				err,
				pageNum,
				label
			);
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