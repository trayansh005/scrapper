// Sequence Home scraper using Playwright with Crawlee
// Agent ID: 15
// Usage:
// node backend/scraper-agent-15.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
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
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	logger.step(`[Detail] Scraping coordinates for: ${property.title}`);
	await sleep(1200);

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
	if (!finalUrl.includes(`page=${pageNum}`)) {
		logger.page(pageNum, label, `Pagination mismatch: landed on ${finalUrl}`);
	}

	try {
		await page.waitForLoadState("networkidle");
		await page.waitForTimeout(4000);
	} catch (e) {
		logger.error(`Listing container not found on page ${pageNum}`, e);
	}

	const properties = await page.evaluate((rentalMode) => {
		try {
			const results = [];
			const seenLinks = new Set();

			// const cards = Array.from(
			// 	document.querySelectorAll(".property-item-card, .property-card, .listing-item"),
			// );

			const cards = Array.from(
				document.querySelectorAll("#search-results-container a[href*='/properties/']"),
			).map((a) => a.closest("div"));

			for (const card of cards) {
				const linkEl = card.querySelector("a[href*='/properties/']");
				let href = linkEl?.getAttribute("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				const expectedPathPart = rentalMode ? "/lettings/" : "/sales/";
				if (!link.includes(expectedPathPart)) continue;

				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				const title = card.querySelector(".property-title, h3")?.textContent?.trim() || "Property";
				// const priceRaw = card.querySelector(".property-price, .price")?.textContent?.trim() || "";

				let priceRaw = "";

				const allTextElements = Array.from(card.querySelectorAll("*"));

				for (const el of allTextElements) {
					const text = el.textContent?.trim();
					if (text && text.includes("£")) {
						priceRaw = text;
						break;
					}
				}
				const bedText = card.querySelector(".property-beds, .beds")?.textContent?.trim() || "";
				const statusText = card.innerText || "";

				results.push({ link, title, priceRaw, bedText, statusText });
			}
			return results;
		} catch (e) {
			return [];
		}
	}, isRental);

	logger.page(pageNum, label, `Processing ${properties.length} properties found on page`);

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

		const price = formatPriceUk(property.priceRaw);
		let bedrooms = null;
		const bedMatch = property.bedText.match(/\d+/);
		if (bedMatch) bedrooms = parseInt(bedMatch[0]);

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

		if (!result.isExisting && !result.error) {
			const detail = await scrapePropertyDetail(page.context(), property);

			await updatePriceByPropertyURL(
				property.link.trim(),
				price,
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
		}

		let propertyAction = "SEEN";
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

		await sleep(2000);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
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
				url: `${type.urlBase}?page=${pg}`,
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
