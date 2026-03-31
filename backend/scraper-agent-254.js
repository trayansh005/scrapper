// Balgores Property scraper - HTML extraction only (no Strapi)
// Agent ID: 254
// Usage: node backend/scraper-agent-254.js

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
	extractBedroomsFromHTML,
	extractCoordinatesFromHTML,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 254;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.balgoresproperty.co.uk/properties-for-sale/essex-and-kent/",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.balgoresproperty.co.uk/properties-to-rent/essex-and-kent/",
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
// UTILITIES
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
}

// ============================================================================
// COORDINATE EXTRACTION - from locrating iframe src on detail page
// ============================================================================

function extractLocratingCoords(html) {
	if (!html) return null;
	// Match the locrating iframe src and pull lat/lng query params
	const srcMatch = html.match(/schools\.locrating\.com[^"']*?lat=([\d.\-]+)[^"']*?lng=([\d.\-]+)/i);
	if (srcMatch) {
		return {
			latitude: parseFloat(srcMatch[1]),
			longitude: parseFloat(srcMatch[2]),
		};
	}
	return null;
}

// ============================================================================
// DETAIL PAGE - coordinates only, for new properties
// ============================================================================

async function fetchDetailPageHtml(browserContext, propertyUrl) {
	const detailPage = await browserContext.newPage();
	try {
		// Allow iframes through — locrating src is set by JS after page load
		await detailPage.route("**/*", (route) => {
			const type = route.request().resourceType();
			if (["image", "font", "media", "stylesheet"].includes(type)) return route.abort();
			return route.continue();
		});

		await detailPage.goto(propertyUrl, { waitUntil: "domcontentloaded", timeout: 30000 });

		// Wait for the locrating iframe src to be populated (injected by JS)
		try {
			await detailPage.waitForFunction(
				() => {
					const iframe = document.querySelector("iframe#location-map, iframe[src*='locrating']");
					return iframe && iframe.getAttribute("src") && iframe.getAttribute("src").includes("lat=");
				},
				{ timeout: 8000 }
			);
		} catch (_) {
			// iframe may not exist on all detail pages — fall through
		}

		return await detailPage.content();
	} catch (err) {
		logger.error(`Detail page error for ${propertyUrl}: ${err.message}`);
		return null;
	} finally {
		await detailPage.close().catch(() => null);
	}
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, baseUrl } = request.userData;
	logger.page(pageNum, label, request.url);

	// Wait for property cards inside search-list-block
	try {
		await page.waitForSelector(".search-list-block .container[id^='myRes']", { timeout: 15000 });
	} catch (e) {
		logger.warn("No property cards found on page", pageNum, label);
	}

	// Extract all property cards from the search-list-block
	const properties = await page.evaluate(() => {
		const results = [];

		// Target only real property containers (those with an id starting with "myRes")
		const cards = Array.from(
			document.querySelectorAll(".search-list-block .container[id^='myRes']")
		);

		for (const card of cards) {
			// Link - from the main property anchor
			const linkEl = card.querySelector("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']");
			if (!linkEl) continue;
			const href = linkEl.getAttribute("href");
			if (!href) continue;
			const fullLink = href.startsWith("http")
				? href
				: new URL(href, window.location.origin).href;

			// Title - h2 inside search-property-details
			const titleEl = card.querySelector(".search-property-details h2 a, .search-property-details h2");
			const title = titleEl ? titleEl.textContent.trim() : "Property";

			// Property type text (e.g. "1 bedroom apartment for sale")
			const typeEl = card.querySelector(".search-property-text");
			const typeText = typeEl ? typeEl.textContent.trim() : "";

			// Price
			const priceEl = card.querySelector(".property-price");
			const priceRaw = priceEl ? priceEl.textContent.trim() : "";

			// Status tag (e.g. "For Sale", "Sold STC", "Under Offer")
			const tagEl = card.querySelector(".property-tag");
			const statusText = tagEl ? tagEl.textContent.trim() : "";

			results.push({ link: fullLink, title, typeText, priceRaw, statusText });
		}

		return results;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`);

	// Stop pagination if no properties found on this page
	if (properties.length === 0) {
		logger.page(pageNum, label, "No properties found — stopping pagination");
		return;
	}

	for (const property of properties) {
		if (!property.link) continue;

		const statusText = (property.statusText || "").trim();
		if (isSoldProperty(statusText)) {
			logger.property(pageNum, label, property.title.substring(0, 40), "N/A", property.link, isRental, null, "SKIPPED");
			continue;
		}

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		// Parse price - strip any formatting issues (e.g. "£1,80,000" → 180000)
		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.property(pageNum, label, property.title.substring(0, 40), "N/A", property.link, isRental, null, "SKIPPED");
			continue;
		}

		// Bedrooms from the type text: "1 bedroom apartment for sale"
		const bedrooms = extractBedroomsFromHTML(property.typeText) ?? extractBedroomsFromHTML(property.title);

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental
		);

		let propertyAction = "UNCHANGED";
		let coords = null;

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			// New property — fetch detail page for coordinates
			logger.page(pageNum, label, `[Detail] Fetching coords for: ${property.title.substring(0, 40)}`);
			const detailHtml = await fetchDetailPageHtml(page.context(), property.link);
			if (detailHtml) {
				coords = extractLocratingCoords(detailHtml);
				if (!coords) {
					coords = await extractCoordinatesFromHTML(detailHtml);
				}
			}

			const saved = await processPropertyWithCoordinates(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				detailHtml,
				coords?.latitude || null,
				coords?.longitude || null
			);

			// Use coords returned by processPropertyWithCoordinates if locrating didn't find them
			if (!coords?.latitude && saved?.latitude) {
				coords = saved;
			}

			counts.totalScraped++;
			counts.totalSaved++;
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
			null,        // totalPages (not used for this site)
			propertyAction,
			coords?.latitude || null,
			coords?.longitude || null
		);

		if (propertyAction === "CREATED") {
			await sleep(500);
		}
	}

	// Queue next page
	const nextPageNum = pageNum + 1;
	const nextUrl = `${baseUrl}page-${nextPageNum}/`;
	await crawler.addRequests([{
		url: nextUrl,
		userData: { pageNum: nextPageNum, isRental, label, baseUrl },
	}]);
	logger.page(pageNum, label, `Queued next page: ${nextUrl}`);
}

// ============================================================================
// CRAWLER
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
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeBalgoresProperty() {
	logger.step("Starting Balgores Property scraper (HTML extraction)...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const initialRequests = PROPERTY_TYPES.map((type) => {
		const pageUrl = startPage > 1
			? `${type.baseUrl}page-${startPage}/`
			: type.baseUrl;
		return {
			url: pageUrl,
			userData: { pageNum: startPage, isRental: type.isRental, label: type.label, baseUrl: type.baseUrl },
		};
	});

	await crawler.run(initialRequests);

	logger.step(`Completed — scraped: ${counts.totalScraped}, saved: ${counts.totalSaved}`);
	logger.step(`SALES: ${counts.savedSales}, LETTINGS: ${counts.savedRentals}`);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run — skipping updateRemoveStatus.");
	}
}

(async () => {
	try {
		await scrapeBalgoresProperty();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
