// Sally Hatcher Estates scraper using Playwright with Crawlee
// Agent ID: 263
// Agent Name: Sally Hatcher Estates
// Updated: 27 March 2026

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

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 263;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.sallyhatcher.co.uk/properties-to-buy",
		totalPages: 1,        // Single page - no pagination
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.sallyhatcher.co.uk/properties-to-let?address=&bedrooms=Min+bed",
		totalPages: 1,        // Single page
		isRental: true,
		label: "LETTINGS",
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS (same as 264)
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
}

// ============================================================================
// BROWSERLESS SETUP (same)
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING (same as 264)
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 90000,
		});

		await detailPage.waitForTimeout(2000);

		const coords = await detailPage.evaluate(() => {
			try {
				const html = document.documentElement.innerHTML;
				const match = html.match(/L\.marker\(\[([\d.-]+),\s*([\d.-]+)\]/);
				if (match) {
					return {
						latitude: parseFloat(match[1]),
						longitude: parseFloat(match[2]),
					};
				}
				return null;
			} catch (e) {
				return null;
			}
		});

		if (coords) return { coords };

		const htmlContent = await detailPage.content();
		const fallback = await extractCoordinatesFromHTML(htmlContent);

		return {
			coords: {
				latitude: fallback.latitude || null,
				longitude: fallback.longitude || null,
			},
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
		return { coords: { latitude: null, longitude: null } };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

// ============================================================================
// REQUEST HANDLER - IMPROVED FOR SALLY HATCHER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForLoadState("networkidle", { timeout: 20000 }).catch(() => {});
		await page.waitForTimeout(4000); // Extra time for text content to render
	} catch (e) {
		logger.error("Initial load timeout", e);
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const seenUrls = new Set();

		// Find all detail links ending with .php
		const detailLinks = Array.from(document.querySelectorAll('a[href$=".php"]'));

		for (const linkEl of detailLinks) {
			const href = linkEl.getAttribute("href");
			if (!href) continue;

			const fullLink = new URL(href, window.location.origin).href;
			if (seenUrls.has(fullLink)) continue;
			seenUrls.add(fullLink);

			// Find correct container (the full li or closest overarching div)
			let container = linkEl.closest('li') || linkEl.closest('.details')?.parentElement || document.body;

			const title = linkEl.textContent.trim() || "Property";

			// Price extraction
			let priceRaw = "";
			const priceEl = container.querySelector('.price');
			if (priceEl) {
				priceRaw = priceEl.textContent.trim();
			} else {
				const priceMatch = container.textContent.match(/£\s*[\d,]+/);
				if (priceMatch) priceRaw = priceMatch[0];
			}

			// Bedrooms
			let bedText = "";
			const sizeEl = container.querySelector('.size');
			if (sizeEl) {
				bedText = sizeEl.textContent.trim();
			} else {
				const bedMatch = container.textContent.match(/(\d+)\s*Bed/i);
				if (bedMatch) bedText = bedMatch[0];
			}
            
            // Status text checking for "SOLD STC" (usually in .tolet element)
            const toletEl = container.querySelector('.tolet, .sold, .status');
            const statusText = toletEl ? toletEl.textContent.trim() : container.textContent || "";

			results.push({
				link: fullLink,
				title,
				priceRaw,
				bedText,
				statusText
			});
		}

		return results;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link) continue;

		// Strong exclusion for Sold STC
		if (property.statusText.toLowerCase().includes("sold stc") || isSoldProperty(property.statusText)) {
			logger.property(pageNum, label, property.title.substring(0, 40), "N/A", property.link, isRental, totalPages, "SKIPPED");
			continue;
		}

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		let bedrooms = null;
		if (property.bedText) {
			const bedMatch = property.bedText.match(/\d+/);
			if (bedMatch) bedrooms = parseInt(bedMatch[0]);
		}

		if (!price) {
			logger.page(pageNum, label, `Skipping (no price found): ${property.link}`, totalPages);
			logger.property(pageNum, label, property.title.substring(0, 40), "N/A", property.link, isRental, totalPages, "SKIPPED");
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
		let coords = { latitude: null, longitude: null };

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const detail = await scrapePropertyDetail(page.context(), property);
			coords = detail?.coords || { latitude: null, longitude: null };

			await processPropertyWithCoordinates(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				null,
				coords.latitude,
				coords.longitude,
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
			propertyAction = "CREATED";
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
			coords.latitude,
			coords.longitude
		);

		if (propertyAction !== "UNCHANGED") await sleep(600);
	}
}

// ============================================================================
// CRAWLER SETUP (same as 264)
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 120,
		requestHandlerTimeoutSecs: 360,
		preNavigationHooks: [async ({ page }) => { await blockNonEssentialResources(page); }],
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1920, height: 1080 },
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

async function scrapeSallyHatcherEstates() {
	logger.step("Starting Sally Hatcher Estates scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label}`);
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			const url = type.baseUrl; // No pagination support

			allRequests.push({
				url,
				userData: { pageNum: pg, isRental: type.isRental, label: type.label, totalPages: type.totalPages }
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	}

	logger.step(`Completed - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, LETTINGS: ${counts.savedRentals}`);

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

(async () => {
	try {
		await scrapeSallyHatcherEstates();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();