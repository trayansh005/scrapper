// Kerr & Co property scraper using PlaywrightCrawler
// Agent ID: 74
// Usage:
//   node backend/scraper-agent-74.js [startPage]
//
// Coordinate strategy: extracted from JavaScript JSON embedded in detail page HTML
// as "lat": xx.xxx / "lng": xx.xxx (Google Maps init data).
// Architecture: PlaywrightCrawler — listing pages queue detail pages for new properties.

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 74;
const logger = createAgentLogger(AGENT_ID);

const BASE_URL = "https://www.kerrandco.com";

const PROPERTY_TYPES = [
	{
		label: "SALES",
		isRental: false,
		totalPages: 3,
		buildUrl: (page) =>
			`${BASE_URL}/properties/sales/tag-residential/status-available/page-${page}#/`,
	},
	{
		label: "RENTALS",
		isRental: true,
		totalPages: 1,
		buildUrl: (page) =>
			`${BASE_URL}/properties/lettings/tag-residential/status-available/page-${page}#/`,
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
// UTILITY FUNCTIONS
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

function extractCoords(html) {
	const latMatch = html.match(/"lat"\s*:\s*([0-9.-]+)/);
	const lngMatch = html.match(/"lng"\s*:\s*([0-9.-]+)/);
	return {
		latitude: latMatch ? parseFloat(latMatch[1]) : null,
		longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
	};
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, url) {
	await sleep(700);
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
		await detailPage.waitForTimeout(800);
		const html = await detailPage.content();
		return extractCoords(html);
	} catch (err) {
		logger.error(`Error scraping detail page ${url}: ${err.message}`);
		return { latitude: null, longitude: null };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLERS
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, label, isRental, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	await page.waitForSelector(".property-card", { timeout: 15000 });

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();

			const propertyCards = Array.from(document.querySelectorAll(".property-card"));

			for (const card of propertyCards) {
				const anchor = card.querySelector("a");
				if (!anchor) continue;

				let href = anchor.getAttribute("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : `${window.location.origin}${href}`;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				const title =
					card
						.querySelector(".property-card__description")
						?.textContent?.replace(/\s+/g, " ")
						.trim() || "Property";

				const bedText = card.querySelector(".property-card__summary")?.textContent?.trim() || "";
				const bedMatch = bedText.match(/(\d+)/);
				const bedrooms = bedMatch ? parseInt(bedMatch[1]) : null;

				const priceRaw = card.querySelector(".property-card__price")?.textContent?.trim() || "";
				const statusText = card.innerText || "";

				results.push({ link, title, bedText, priceRaw, statusText, bedrooms });
			}
			return results;
		} catch (e) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link) continue;

		if (isSoldProperty(property.statusText || "")) continue;

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.page(pageNum, label, `Skipping update (no price found): ${property.link}`, totalPages);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "UNCHANGED";

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const { latitude, longitude } = await scrapePropertyDetail(page.context(), property.link);

			await processPropertyWithCoordinates(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
				null,
				latitude,
				longitude,
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
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(300);
		}
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

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
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: async (context) => {
			await handleListingPage(context);
		},
		failedRequestHandler({ request }) {
			logger.error(`Failed request: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeKerrCo() {
	logger.step("Starting Kerr & Co scraper...");

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
				url: type.buildUrl(pg),
				userData: {
					handler: "LISTING",
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
		`Completed Kerr & Co - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, RENTALS: ${counts.savedRentals}`);

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
		await scrapeKerrCo();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
