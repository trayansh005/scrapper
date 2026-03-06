// Robert Holmes scraper using Playwright with Crawlee
// Agent ID: 78
// Usage:
// node backend/scraper-agent-78.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 78;
const logger = createAgentLogger(AGENT_ID);

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// PROPERTY TYPES CONFIGURATION
// ============================================================================

const PROPERTY_TYPES = [
	{
		urlBase: "https://robertholmes.co.uk/search/",
		params: "address_keyword=&department=residential-sales&availability=2",
		totalPages: 10,
		recordsPerPage: 12,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://robertholmes.co.uk/search/",
		params: "address_keyword=&department=residential-lettings",
		totalPages: 10,
		recordsPerPage: 12,
		isRental: true,
		label: "LETTINGS",
	}

];

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
// REQUEST HANDLER FOR LISTING PAGES
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		// Wait for page content to populate
		await page.waitForTimeout(2000);
		await page
			.waitForSelector('a[href*="/property/"]', { timeout: 20000 })
			.catch(() => {
				logger.warn(`No listing container found on page ${pageNum}`);
			});

		// Extract properties from listing page
		const properties = await page.evaluate(() => {
			try {
				const items = Array.from(document.querySelectorAll('a[href*="/property/"]'));
				return items
					.map((el) => {
						try {
							const link = el.getAttribute("href");
							if (!link) return null;

							const fullLink = link.startsWith("http") ? link : "https://robertholmes.co.uk" + link;

							const title = el.querySelector("h4")?.textContent?.trim() || "";

							const bedroomsText = el.querySelector("ul li:nth-child(1) span:nth-child(1)")?.textContent?.trim() || "";
							const bedroomsMatch = bedroomsText.match(/\d+/);
							const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[0], 10) : null;

							const priceText = el.querySelector("h5")?.textContent?.trim() || "";
							// Extract price: match £ followed by digits and commas only
							const priceMatch = priceText.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							return { link: fullLink, price, title, bedrooms, lat: null, lng: null };
						} catch (e) {
							return null;
						}
					})
					.filter((p) => p && p.link);
			} catch (e) {
				return [];
			}
		});

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

		// Process properties with detail scraping
		for (const property of properties) {
			if (!property.link) continue;

			// Skip sold properties
			if (isSoldProperty(property.title)) {
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

			let lat = null;
			let lon = null;

			// Visit detail page to extract coordinates from GeoCoordinates JSON
			const detailPage = await page.context().newPage();
			try {
				await detailPage.goto(property.link, {
					waitUntil: "load",
					timeout: 30000,
				});
				await detailPage.waitForTimeout(500);

				// Extract coordinates from GeoCoordinates JSON
				const detailCoords = await detailPage.evaluate(() => {
					try {
						// Look for GeoCoordinates JSON-LD data
						const scripts = Array.from(
							document.querySelectorAll('script[type="application/ld+json"]')
						);
						for (const s of scripts) {
							try {
								const data = JSON.parse(s.textContent);
								if (
									data &&
									data["@type"] === "GeoCoordinates" &&
									data.latitude &&
									data.longitude
								) {
									// Check for valid coordinates (not 0.000013, 0.000013 dummy values)
									if (Math.abs(data.latitude) > 0.1 && Math.abs(data.longitude) > 0.1) {
										return { lat: data.latitude, lng: data.longitude };
									}
								}
							} catch (e) {
								// continue
							}
						}

						// Regex search for GeoCoordinates pattern
						const allScripts = Array.from(document.querySelectorAll("script"))
							.map((s) => s.textContent)
							.join("\n");

						// Try multiple regex patterns for coordinates
						const geoMatch = allScripts.match(
							/"@type":"GeoCoordinates","latitude":([0-9e.-]+),"longitude":([0-9e.-]+)/
						);
						if (geoMatch) {
							const lat = parseFloat(geoMatch[1]);
							const lng = parseFloat(geoMatch[2]);
							// Check for valid coordinates
							if (Math.abs(lat) > 0.1 && Math.abs(lng) > 0.1) {
								return { lat, lng };
							}
						}

						// Try alternative pattern with spaces
						const geoMatch2 = allScripts.match(
							/"latitude"\s*:\s*([0-9e.-]+)\s*,\s*"longitude"\s*:\s*([0-9e.-]+)/
						);
						if (geoMatch2) {
							const lat = parseFloat(geoMatch2[1]);
							const lng = parseFloat(geoMatch2[2]);
							if (Math.abs(lat) > 0.1 && Math.abs(lng) > 0.1) {
								return { lat, lng };
							}
						}

						return null;
					} catch (e) {
						return null;
					}
				});

				if (detailCoords) {
					lat = detailCoords.lat;
					lon = detailCoords.lng;
				}
			} catch (err) {
				logger.warn(`Could not fetch detail page for ${property.link}: ${err.message}`);
			} finally {
				await detailPage.close();
			}

			// Clean price: extract only numbers (e.g., "£47,666pcm" → "47666")
			const priceClean = property.price ? parseInt(property.price.replace(/[^0-9]/g, ""), 10) : null;
			const price = priceClean ? priceClean.toString() : null;

			try {
				// Check if property exists first
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
					// Insert new property with coordinates
					await processPropertyWithCoordinates(
						property.link,
						price,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
						null, // HTML config not needed
						lat,
						lon,
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
			} catch (dbErr) {
				logger.error(`DB error for ${property.link}: ${dbErr.message}`);
				counts.totalScraped++;
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
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);

				await page.setExtraHTTPHeaders({
					"user-agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
				});
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
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeRobertHolmes() {
	logger.step(`Starting Robert Holmes scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		const effectiveStartPage = Math.max(1, startPage);

		for (let pg = effectiveStartPage; pg <= type.totalPages; pg++) {
			const url = pg === 1
				? `${type.urlBase}?${type.params}`
				: `${type.urlBase}page/${pg}/?${type.params}`;

			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					totalPages: type.totalPages,
					isRental: type.isRental,
					label: type.label,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Robert Holmes - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New lettings: ${counts.savedRentals}`,
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
		await scrapeRobertHolmes();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
