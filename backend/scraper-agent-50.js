// Foxtons scraper using Playwright with Crawlee
// Agent ID: 50
//
// Usage:
// node backend/scraper-agent-50.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 50;

const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
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

async function scrapePropertyDetail(browserContext, property, isRental) {
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		// Block unnecessary resources using shared helper
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		// Extract details from detail page (Coordinates, Bedrooms, and Monthly Rent if applicable)
		const detailInfo = await detailPage.evaluate((isRental) => {
			let result = {
				coords: { latitude: null, longitude: null },
				bedrooms: null,
				monthlyRent: null,
				htmlContent: document.documentElement.outerHTML,
			};

			try {
				// 1. Extract coordinates from structured data
				const content = result.htmlContent;
				const geoMatch = content.match(
					/"@type":"GeoCoordinates","latitude":([\d.-]+),"longitude":([\d.-]+)/,
				);
				if (geoMatch) {
					result.coords.latitude = parseFloat(geoMatch[1]);
					result.coords.longitude = parseFloat(geoMatch[2]);
				}

				// 2. Extract bedrooms
				const bedElements = document.querySelectorAll(".MuiTypography-body1.iconText");
				for (const bedEl of bedElements) {
					const bedText = bedEl.textContent?.trim() || "";
					if (bedText.includes("Bed")) {
						const bedMatch = bedText.match(/(\d+)/);
						if (bedMatch) {
							result.bedrooms = parseInt(bedMatch[1]);
							break;
						}
					}
				}

				// 3. Extract monthly rent for rentals
				if (isRental) {
					const monthlyRentEl = document.querySelector(".monthly-rent");
					if (monthlyRentEl) {
						result.monthlyRent = monthlyRentEl.textContent?.trim() || null;
					}
				}
			} catch (e) {
				// Silently fail within browser context
			}
			return result;
		}, isRental);

		if (detailInfo.bedrooms !== null) {
			property.bedrooms = detailInfo.bedrooms;
		}

		// If we found a monthly rent on the detail page, use it over the listing page price
		if (isRental && detailInfo.monthlyRent) {
			const rentMatch = detailInfo.monthlyRent.match(/[\d,]+/);
			if (rentMatch) {
				property.price = rentMatch[0];
			}
		}

		// Save property to database
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			detailInfo.htmlContent,
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}:`, error?.message || error);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;
	logger.page(pageNum, label, request.url);

	// Wait for page content to populate
	await page.waitForTimeout(2000);
	await page.waitForSelector("[data-id]", { timeout: 20000 }).catch(() => {
		logger.step(`No property container found on page ${pageNum}`);
	});

	// Extract all properties from the page
	const properties = await page.evaluate((isRental) => {
		const containers = Array.from(document.querySelectorAll("[data-id]"));
		return containers
			.map((container) => {
				// Get property link
				const linkEl = container.querySelector("a[href*='/properties-']");
				const link = linkEl ? linkEl.href : null;

				// Get address
				const address = container.querySelector(".addressText")?.textContent?.trim() || "";

				// Get price
				let priceText = "";
				if (isRental) {
					// For rentals, try to get monthly rent first
					const monthlyRentEl = container.querySelector(".monthly-rent");
					if (monthlyRentEl) {
						priceText = monthlyRentEl.textContent?.trim() || "";
					} else {
						priceText = container.querySelector(".MuiTypography-h4")?.textContent?.trim() || "";
					}
				} else {
					priceText = container.querySelector(".MuiTypography-h4")?.textContent?.trim() || "";
				}

				// Format price: extract first numeric value with commas only
				let priceClean = "";
				if (priceText) {
					const priceMatch = priceText.match(/[\d,]+/);
					priceClean = priceMatch ? priceMatch[0] : "";
				}

				if (link && address && priceClean) {
					return {
						link,
						title: address,
						price: priceClean,
						bedrooms: null, // Will extract from detail page if needed
					};
				}
				return null;
			})
			.filter(Boolean);
	}, isRental);

	logger.page(pageNum, label, `Found ${properties.length} properties`);

	// Process each property
	for (const property of properties) {
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			stats.totalSaved++;
		}

		if (!result.isExisting && !result.error) {
			logger.page(pageNum, label, `Scraping detail for new property: ${property.title}`);
			await scrapePropertyDetail(page.context(), property, isRental);
		}
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		navigationTimeoutSecs: 60,
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeFoxtons() {
	logger.step(`Starting Foxtons scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	// Config
	const totalSalesPages = 69;
	const totalRentalsPages = 22;

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	// for (let p = Math.max(1, startPage); p <= totalSalesPages; p++) {
	// 	const url =
	// 		p === 1
	// 			? "https://www.foxtons.co.uk/properties-for-sale/south-east-england?order_by=price_desc&radius=5&available_for_auction=0&sold=0"
	// 			: `https://www.foxtons.co.uk/properties-for-sale/south-east-england?order_by=price_desc&radius=5&available_for_auction=0&sold=0&page=${p}`;

	// 	allRequests.push({
	// 		url,
	// 		userData: {
	// 			pageNum: p,
	// 			isRental: false,
	// 			label: `SALES_PAGE_${p}`,
	// 		},
	// 	});
	// }

	// Build Rentals requests
	if (startPage === 1) {
		for (let p = 1; p <= totalRentalsPages; p++) {
			const url =
				p === 1
					? "https://www.foxtons.co.uk/properties-to-rent/south-east-england?order_by=price_desc&expand=5&sold=0"
					: `https://www.foxtons.co.uk/properties-to-rent/south-east-england?order_by=price_desc&expand=5&sold=0&page=${p}`;

			allRequests.push({
				url,
				userData: {
					pageNum: p,
					isRental: true,
					label: `RENTALS_PAGE_${p}`,
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
		`Completed Foxtons - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeFoxtons();
		await updateRemoveStatus(AGENT_ID);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
