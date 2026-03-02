// JLL Residential scraper using Playwright with Crawlee
// Agent ID: 72
// Website: residential.jll.co.uk
// Usage:
// node backend/scraper-agent-72.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 72;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

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
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// Small delay to ensure JS execution if needed
		await detailPage.waitForTimeout(1000);

		// Extract coordinates from window.__NEXT_DATA__
		const detailData = await detailPage.evaluate(() => {
			try {
				const nextData = window.__NEXT_DATA__;
				if (
					nextData &&
					nextData.props &&
					nextData.props.pageProps &&
					nextData.props.pageProps.property
				) {
					const p = nextData.props.pageProps.property;
					return {
						lat: p.lat || null,
						lng: p.lng || null,
						html: document.documentElement.innerHTML,
					};
				}
			} catch (e) {
				// Fallback or error
			}
			return { lat: null, lng: null, html: document.documentElement.innerHTML };
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			detailData.html,
			detailData.lat,
			detailData.lng,
		);

		stats.totalScraped++;
		stats.totalSaved++;
		if (isRental) stats.savedRentals++;
		else stats.savedSales++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNumber, totalPages } = request.userData;
	logger.page(pageNumber, label, request.url, totalPages);

	try {
		// Extract coordinates and other data from window.__NEXT_DATA__
		const properties = await page.evaluate(() => {
			try {
				const nextData = window.__NEXT_DATA__;
				if (!nextData?.props?.pageProps?.properties) return [];

				return nextData.props.pageProps.properties.map((p) => {
					// Price extraction
					let priceText = "";
					if (p.price) {
						if (p.price.minAmount) {
							priceText = `£${p.price.minAmount.toLocaleString()}`;
						} else if (p.price.amount) {
							priceText = `£${p.price.amount.toLocaleString()}`;
						}
					}

					// Bedroom extraction
					let bedrooms = null;
					if (p.rooms?.bedrooms?.[0]) {
						const bedMatch = p.rooms.bedrooms[0].match(/(\d+)/);
						if (bedMatch) bedrooms = parseInt(bedMatch[1]);
					}

					return {
						link: p.pageUrl ? `https://residential.jll.co.uk${p.pageUrl}` : null,
						title: p.title || "JLL Property",
						priceText,
						bedrooms,
						statusText: p.saleLabel || p.rentLabel || "",
						lat: p.lat || null,
						lng: p.lng || null,
					};
				});
			} catch (e) {
				return [];
			}
		});

		if (properties.length === 0) {
			logger.error(
				`No properties found in __NEXT_DATA__ on page ${pageNumber}`,
				null,
				pageNumber,
				label,
			);
		}

		logger.page(pageNumber, label, `Found ${properties.length} properties`, totalPages);

		for (const property of properties) {
			if (!property.link || !property.priceText) continue;

			if (isSoldProperty(property.statusText)) {
				continue;
			}

			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			const price = parsePrice(property.priceText);
			if (!price) continue;

			// Use processPropertyWithCoordinates directly if we have lat/lng
			let action = "UNCHANGED";
			try {
				// Optimization: Check if it exists and price is different first
				const updateResult = await updatePriceByPropertyURLOptimized(
					property.link,
					price,
					property.title,
					property.bedrooms,
					AGENT_ID,
					isRental,
				);

				if (updateResult.updated) {
					stats.totalSaved++;
					action = "UPDATED";
				}

				if (!updateResult.isExisting && !updateResult.error) {
					// New property, use processPropertyWithCoordinates with the coords we have
					await processPropertyWithCoordinates(
						property.link,
						price,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
						"", // No HTML needed
						property.lat,
						property.lng,
					);
					stats.totalSaved++;
					stats.totalScraped++;
					if (isRental) stats.savedRentals++;
					else stats.savedSales++;
					action = "CREATED";
				} else if (updateResult.error) {
					action = "ERROR";
				}
			} catch (error) {
				logger.error(`Error processing property ${property.link}: ${error.message}`);
				action = "ERROR";
			}

			logger.property(
				pageNumber,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				totalPages,
				action,
			);
		}
	} catch (error) {
		logger.error(`Error in handleListingPage: ${error.message}`, error, pageNumber, label);
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
		requestHandlerTimeoutSecs: 600,
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
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeJLL() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting JLL Residential Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const totalPages = 20; // Reasonable limit for JLL sales
	const requests = [];

	for (let p = Math.max(1, startPage); p <= totalPages; p++) {
		const url = `https://residential.jll.co.uk/search?tenureType=sale&sortBy=price&sortDirection=desc&page=${p}`;
		requests.push({
			url,
			userData: {
				pageNumber: p,
				totalPages,
				isRental: false,
				label: "SALES",
			},
		});
	}

	if (requests.length > 0) {
		logger.step(`Queueing ${requests.length} listing pages starting from page ${startPage}...`);
		await crawler.run(requests);
	}

	logger.step(
		`Finished JLL - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);

	// Only update remove status if we did a full run
	if (!isPartialRun) {
		logger.step("Updating remove status for properties not seen in this run...");
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
		await scrapeJLL();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
