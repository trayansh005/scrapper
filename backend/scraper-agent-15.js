// Sequence Home scraper using Playwright with Crawlee
// Agent ID: 15
// Usage:
// node backend/scraper-agent-15.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 15;
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
// DETAIL PAGE SCRAPING (Fallback if API lacks coordinates)
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	logger.step(`[Detail] Scraping coordinates for: ${property.title}`);

	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 90000,
		});

		await detailPage.waitForTimeout(3000);

		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);

		if (coords.latitude && coords.longitude) {
			logger.step(`[Detail] Found coordinates: ${coords.latitude}, ${coords.longitude}`);
		} else {
			logger.step(`[Detail] No coordinates found in HTML`);
		}

		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
			},
		};
	} catch (error) {
		logger.error(`[Detail] Error scraping detail page ${property.link}`, error);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;
	logger.page(pageNum, label, request.url);

	const finalUrl = page.url();
	const isExpectedFirstPageRedirect =
		pageNum === 1 && /\/properties\/(sales|lettings)\/?$/.test(finalUrl);
	if (!finalUrl.includes(`page-${pageNum}`) && !isExpectedFirstPageRedirect) {
		logger.page(pageNum, label, `Pagination mismatch: landed on ${finalUrl}`);
	}

	try {
		await page.waitForLoadState("networkidle");
	} catch (e) {
		logger.error(`Network loading failed on page ${pageNum}`, e);
	}

	// Fetch JSON API from within browser context to bypass Cloudflare
	const properties = await page.evaluate(
		async ({ channel, pageNum }) => {
			try {
				const fragment = pageNum > 1 ? `/page-${pageNum}` : "";
				const apiUrl = `https://www.sequencehome.co.uk/search.ljson?channel=${channel}&fragment=${encodeURIComponent(fragment)}`;
				const response = await fetch(apiUrl, {
					headers: {
						Accept: "application/json",
						"User-Agent": navigator.userAgent,
					},
					credentials: "include", // Include cookies/auth from browser session
				});

				if (!response.ok) {
					console.error(`API error: ${response.status}`);
					return [];
				}

				const data = await response.json();
				const results = [];

				if (Array.isArray(data.properties)) {
					for (const prop of data.properties) {
						const rawLink = prop.property_url || null;
						if (!rawLink) continue;

						const link = rawLink.startsWith("http")
							? rawLink
							: `https://www.sequencehome.co.uk${rawLink}`;
						const title = prop.display_address || prop.short_description || "Property";
						const priceRaw = Number.isFinite(prop.price_value) ? String(prop.price_value) : "";
						const bedrooms = Number.isFinite(prop.bedrooms) ? prop.bedrooms : null;
						const statusText = prop.status || "";
						const latitude = Number.isFinite(prop.lat) ? prop.lat : null;
						const longitude = Number.isFinite(prop.lng) ? prop.lng : null;

						results.push({
							link,
							title,
							priceRaw,
							bedrooms,
							statusText,
							latitude,
							longitude,
						});
					}
				}

				return results;
			} catch (e) {
				console.error(`API fetch error: ${e.message}`);
				return [];
			}
		},
		{ channel: isRental ? "lettings" : "sales", pageNum },
	);

	logger.page(pageNum, label, `Processing ${properties.length} properties from API`);

	for (const property of properties) {
		if (!property.link) {
			logger.page(pageNum, label, `Skipped: Missing link for property`);
			continue;
		}

		if (isSoldProperty(property.statusText || "")) {
			logger.page(pageNum, label, `Skipped: Property is Sold/Under Offer (${property.link})`);
			continue;
		}

		if (processedUrls.has(property.link)) {
			logger.page(pageNum, label, `Skipped: Already processed (${property.link})`);
			continue;
		}
		processedUrls.add(property.link);

		const price = property.priceRaw ? parseInt(property.priceRaw) : null;
		const bedrooms = property.bedrooms || null;

		if (!price) {
			logger.page(pageNum, label, `Skipped: No price found for ${property.link}`);
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

		if (result.updated) {
			stats.totalSaved++;
		}

		let latitude = property.latitude;
		let longitude = property.longitude;

		// If API provides coordinates, use them directly; otherwise scrape detail page
		if (!result.isExisting && !result.error) {
			if (!latitude || !longitude) {
				const detail = await scrapePropertyDetail(page.context(), property);
				latitude = detail?.coords?.latitude || null;
				longitude = detail?.coords?.longitude || null;
			}

			// Save new property with coordinates
			await processPropertyWithCoordinates(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				null,
				latitude,
				longitude,
			);

			stats.totalSaved++;
			stats.totalScraped++;
			if (isRental) stats.savedRentals++;
			else stats.savedSales++;
		}

		let propertyAction = "UNCHANGED";
		if (result.updated) propertyAction = "UPDATED";
		if (!result.isExisting && !result.error) propertyAction = "CREATED";
		logger.property(
			pageNum,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			property.link,
			isRental,
			0,
			propertyAction,
		);
	}

	// Delay before the next page/API call to avoid rate-limiting
	const pageJitter = Math.floor(Math.random() * 2000) + 2000;
	logger.step(`Waiting ${pageJitter}ms before next API call...`);
	await sleep(pageJitter);
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3, // Increased retries for 403 handling
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [
			async ({ page, request }) => {
				await blockNonEssentialResources(page);

				// Rotate User-Agent to a common desktop one
				const userAgents = [
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
					"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
					"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
				];
				const randomUA = userAgents[Math.floor(Math.random() * userAgents.length)];
				await page.setExtraHTTPHeaders({ "User-Agent": randomUA });
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: [
					"--no-sandbox",
					"--disable-setuid-sandbox",
					"--disable-blink-features=AutomationControlled", // Help bypass detection
				],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(` Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeSequenceHome() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Sequence Home scraper at ${scrapeStartTime.toISOString()}...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;

	if (isPartialRun) {
		logger.step(
			`CRITICAL: Partial run detected (startPage: ${startPage}). Automatic cleanup will be disabled.`,
		);
	}

	const PROPERTY_TYPES = [
		{
			urlBase: "https://www.sequencehome.co.uk/properties/sales",
			isRental: false,
			label: "SALES",
			totalRecords: 16362,
			recordsPerPage: 10,
		},
		{
			urlBase: "https://www.sequencehome.co.uk/properties/lettings",
			isRental: true,
			label: "LETTINGS",
			totalRecords: 1907,
			recordsPerPage: 10,
		},
	];

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
		const effectiveStartPage = Math.max(1, startPage);

		for (let pg = effectiveStartPage; pg <= totalPages; pg++) {
			allRequests.push({
				url: `${type.urlBase}/page-${pg}`,
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: `${type.label}_PAGE_${pg}`,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	logger.step(
		`Completed Sequence Home - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);

	return { scrapeStartTime, isPartialRun };
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		const { scrapeStartTime, isPartialRun } = await scrapeSequenceHome();

		if (!isPartialRun) {
			logger.step("Full run completed. Starting cleanup of stale properties...");
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
			logger.step("Cleanup finished successfully.");
		} else {
			logger.step(
				"Partial run completed. Skipping cleanup of stale properties to prevent accidental removal.",
			);
		}

		logger.step("Summary of Scraper Run:");
		logger.step(`- Total Collected: ${stats.totalScraped}`);
		logger.step(`- Total Saved to DB: ${stats.totalSaved}`);
		logger.step(`- Sales Saved: ${stats.savedSales}`);
		logger.step(`- Rentals Saved: ${stats.savedRentals}`);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
