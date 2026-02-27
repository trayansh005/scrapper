// VHHomes scraper using Playwright with Crawlee
// Agent ID: 11
// Usage:
// node backend/scraper-agent-11.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 11;
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

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatPriceDisplay(price, isRental) {
	if (!price) return isRental ? "£0 pcm" : "£0";
	return `£${price}${isRental ? " pcm" : ""}`;
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
		await page.waitForSelector("a", { timeout: 15000 });
	} catch (e) {
		logger.error("Page load issue", e, pageNum, label);
	}

	const properties = await page.evaluate(() => {
		function cleanPrice(raw) {
			if (!raw) return null;
			// Extract the first number with commas (UK style)
			const match = raw.match(/£?\s*([\d,]+)/);
			if (!match) return null;
			return match[1];
		}
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
				const priceElem = card.querySelector("span._property-price");
				const priceTextRaw = priceElem ? priceElem.textContent : null;
				const priceText = cleanPrice(priceTextRaw);
				if (!priceText) continue; // skip if price is not valid
				// Status (e.g. For Sale, Let)
				const statusElem = card.querySelector("span._property-availability");
				const statusText = statusElem ? statusElem.textContent.trim() : "";
				// Bedrooms (from rooms container)
				let bedrooms = null;
				const roomsContainer = card.querySelector("._property-rooms-container");
				if (roomsContainer) {
					const spans = Array.from(roomsContainer.querySelectorAll("span"));
					for (const span of spans) {
						const svgTitle = span.querySelector("svg title");
						if (svgTitle && svgTitle.textContent.toLowerCase().includes("room")) {
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
					price: priceText,
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
		const formattedPrice = formatPriceUk(property.price);
		let bedrooms = property.bedrooms;
		if (!formattedPrice) {
			logger.page(pageNum, label, `Skipping update (no price found): ${property.link}`, totalPages);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			formattedPrice,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "SEEN";

		if (result.updated) {
			stats.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const detail = await scrapePropertyDetail(page.context(), property);
			await updatePriceByPropertyURL(
				property.link.trim(),
				formattedPrice,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				detail?.coords?.latitude || null,
				detail?.coords?.longitude || null,
			);
			stats.totalSaved++;
			stats.totalScraped++;
			if (isRental) stats.savedRentals++;
			else stats.savedSales++;
			propertyAction = "CREATED";
		} else if (result.isExisting && result.updated) {
			stats.totalScraped++;
			if (isRental) stats.savedRentals++;
			else stats.savedSales++;
		} else if (result.error) {
			propertyAction = "ERROR";
		}

		logger.property(
			pageNum,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(formattedPrice, isRental),
			property.link,
			isRental,
			totalPages,
			propertyAction,
		);

		await sleep(500);
	}

	// Check for next page and queue it
	const nextPageNum = pageNum + 1;
	const nextPageUrl = isRental
		? `https://vhhomes.co.uk/search?type=rent&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394&page=${nextPageNum}`
		: `https://vhhomes.co.uk/search?type=buy&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394&page=${nextPageNum}`;
	if (properties.length >= 10 && pageNum < 50) {
		logger.page(pageNum, label, `Queuing next page: ${nextPageNum}`, totalPages);
		await crawler.addRequests([
			{
				url: nextPageUrl,
				userData: { pageNum: nextPageNum, totalPages: 50, isRental, label },
			},
		]);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

let crawler; // Global crawler instance for recursion

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
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);
	// Scrape sales
	logger.step("SALES");
	crawler = createCrawler(browserWSEndpoint);
	try {
		await crawler.run([
			{
				url: `https://vhhomes.co.uk/search?type=buy&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394&page=${startPage}`,
				userData: { pageNum: startPage, totalPages: 50, isRental: false, label: "SALES" },
			},
		]);
	} catch (error) {
		logger.error("Error during sales scraping", error);
	} finally {
		await crawler.teardown();
	}
	// Clear processed URLs for rentals
	processedUrls.clear();
	// Scrape rentals
	logger.step("LETTINGS");
	crawler = createCrawler(browserWSEndpoint);
	try {
		await crawler.run([
			{
				url: `https://vhhomes.co.uk/search?type=rent&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394&page=${startPage}`,
				userData: { pageNum: startPage, totalPages: 50, isRental: true, label: "LETTINGS" },
			},
		]);
	} catch (error) {
		logger.error("Error during rentals scraping", error);
	} finally {
		await crawler.teardown();
	}
	// Print summary
	logger.step(
		`Summary - Total scraped: ${stats.totalScraped}, Total updated: ${stats.totalSaved}, New sales: ${stats.savedSales}, New rentals: ${stats.savedRentals}`,
	);
}

// Run the scraper
scrapeVHHomes().catch((error) => logger.error("Unhandled scraper error", error));
