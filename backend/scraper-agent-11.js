// VHHomes scraper using Playwright with Crawlee
// Agent ID: 11
// Usage:
// node backend/scraper-agent-11.js

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
		baseUrl:
			"https://vhhomes.co.uk/search?type=buy&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394",
		totalPages: 5,
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl:
			"https://vhhomes.co.uk/search?type=rent&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394",
		totalPages: 1,
		isRental: true,
		label: "LETTINGS",
	},
];

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// using shared blockNonEssentialResources from lib/scraper-utils.js

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
// DETAIL PAGE SCRAPING (refactored to use extractCoordinatesFromHTML)
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	await sleep(700);
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 90000,
		});
		await detailPage.waitForTimeout(800);
		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);
		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
			},
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER (no change, but detail scraping now uses extractCoordinatesFromHTML)
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector("._property", { timeout: 15000 });
	} catch (e) {
		logger.warn("No properties found with current selectors", pageNum, label);
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const cards = Array.from(document.querySelectorAll("._property"));
			for (const card of cards) {
				// Link
				const linkElem = card.querySelector(
					'._property-address a, a[href*="/buy/"], a[href*="/rent/"]',
				);
				const href = linkElem ? linkElem.getAttribute("href") : null;
				if (!href) continue;
				const fullUrl = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				// Title/Address
				const titleElem = card.querySelector("._property-address a");
				const title = titleElem ? titleElem.textContent.trim() : "Property";
				// Price
				const priceElem = card.querySelector("._property-price");
				const priceRaw = priceElem ? priceElem.textContent.trim() : "";
				if (!priceRaw) continue; // skip if price is not valid
				// Status (e.g. For Sale, Let)
				const statusElem = card.querySelector("span._property-availability");
				const statusText = statusElem ? statusElem.textContent.trim() : "";
				// Bedrooms (from rooms container)
				let bedrooms = null;
				const roomsContainer = card.querySelector("._property-rooms-container");
				if (roomsContainer) {
					const spans = Array.from(roomsContainer.querySelectorAll("span"));
					for (const span of spans) {
						const svgTitle = span.querySelector("svg title")?.textContent?.toLowerCase() || "";
						if (
							svgTitle.includes("room") ||
							span
								.querySelector("svg[viewBox='0 0 100 100'] title")
								?.textContent?.toLowerCase()
								?.includes("room")
						) {
							const numMatch = (span.textContent || "").match(/\d+/);
							if (numMatch) {
								bedrooms = parseInt(numMatch[0], 10);
								break;
							}
						}
					}
				}
				// Summary
				const summaryElem = card.querySelector("._property-summary-container");
				const summary = summaryElem ? summaryElem.textContent.trim() : "";
				results.push({
					link: fullUrl,
					title,
					bedrooms,
					statusText,
					priceRaw,
					summary,
				});
			}
			return results;
		} catch (e) {
			console.log("Error extracting properties:", e.message);
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link) continue;
		if (isSoldProperty(property.statusText || "")) continue;
		if (processedUrls.has(property.link)) {
			logger.page(
				pageNum,
				label,
				`Skipping duplicate URL: ${property.link.substring(0, 60)}...`,
				totalPages,
			);
			continue;
		}
		processedUrls.add(property.link);

		// Extract price and bedrooms from listing
		const price = parsePrice(property.priceRaw);
		let bedrooms = property.bedrooms;
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
			const detail = await scrapePropertyDetail(page.context(), property);
			await processPropertyWithCoordinates(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				null, // HTML not needed if we have coords
				detail?.coords?.latitude || null,
				detail?.coords?.longitude || null,
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

async function scrapeVHHomes() {
	logger.step("Starting VHHomes scraper...");

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
				url: `${type.baseUrl}&page=${pg}`,
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
		`Completed VHHomes - Total scraped: ${counts.totalScraped}, Total updated: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

// Run the scraper
scrapeVHHomes()
	.then(() => {
		logger.step("All done!");
		process.exit(0);
	})
	.catch((error) => {
		logger.error("Unhandled scraper error", error);
		process.exit(1);
	});
