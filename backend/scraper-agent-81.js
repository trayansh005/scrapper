// Streets Ahead property scraper using PlaywrightCrawler + load-more API
// Agent ID: 81
// Usage:
//   node backend/scraper-agent-81.js [startPage]
//
// Coordinate strategy: extracted from detail page HTML via extractCoordinatesFromHTML.
// Architecture: PlaywrightCrawler — uses the site's internal wp-json API to fetch listing data faster.

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
	extractCoordinatesFromHTML,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 81;
const logger = createAgentLogger(AGENT_ID);

const BASE_URL = "https://www.streetsahead.info";
const API_URL = `${BASE_URL}/wp-json/property/load-more`;

const PROPERTY_TYPES = [
	{
		label: "SALES",
		isRental: false,
		tenure: "properties-for-sale",
	},
	{
		label: "RENTALS",
		isRental: true,
		tenure: "properties-to-rent",
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

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, url) {
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(url, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		await detailPage.waitForTimeout(1000);

		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);

		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
			},
			html: htmlContent,
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${url}: ${error.message}`);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNum, tenure } = request.userData;
	logger.page(pageNum, label, request.url, "??");

	// Fetch properties from the API
	const htmlResponse = await page.evaluate(async (url) => {
		const res = await fetch(url);
		return await res.text();
	}, request.url);

	// Parse the HTML fragment returned by the API
	const properties = await page.evaluate((html) => {
		try {
			const parser = new DOMParser();
			const doc = parser.parseFromString(html, "text/html");
			const results = [];
			const seenLinks = new Set();
			const propertyEls = Array.from(doc.querySelectorAll(".property"));

			for (const el of propertyEls) {
				const anchor = el.querySelector("a");
				if (!anchor) continue;

				const link = anchor.href;
				if (!link || seenLinks.has(link)) continue;
				seenLinks.add(link);

				const title = el.querySelector("h3")?.textContent?.trim() || "Property";
				const priceRaw = el.querySelector(".price")?.textContent?.trim() || "";
				const bedText = el.querySelector(".icons span")?.textContent?.trim() || "";
				const statusText = el.innerText || "";

				results.push({ link, title, priceRaw, bedText, statusText });
			}
			return results;
		} catch (e) {
			return [];
		}
	}, htmlResponse);

	if (properties.length === 0) {
		logger.page(pageNum, label, "No more properties found on this page.", "??");
		return;
	}

	logger.page(pageNum, label, `Found ${properties.length} properties`, "??");

	for (const property of properties) {
		if (!property.link) continue;

		if (isSoldProperty(property.statusText || "")) continue;

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.page(pageNum, label, `Skipping update (no price found): ${property.link}`, "??");
			continue;
		}

		let bedrooms = null;
		const bedMatch = property.bedText.match(/\d+/);
		if (bedMatch) bedrooms = parseInt(bedMatch[0]);

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
			const detail = await scrapePropertyDetail(page.context(), property.link);

			await processPropertyWithCoordinates(
				property.link,
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				detail?.html || null,
				detail?.coords?.latitude || null,
				detail?.coords?.longitude || null,
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
			"??",
			propertyAction,
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(500);
		}
	}

	// Queue next page if we found a full set of results
	if (properties.length >= 10) {
		const nextPage = pageNum + 1;
		const nextUrl = `${API_URL}?q_tenure=${tenure}&q_area=any-area&q_type=any-type&q_bedrooms=any-bedrooms&q_min_price=min-price-none&q_max_price=max-price-none&q_exclude_unavailable=exclude-unavalable&q_sort_direction=desc&q_page=${nextPage}`;

		await crawler.addRequests([
			{
				url: nextUrl,
				userData: { ...request.userData, pageNum: nextPage },
			},
		]);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

let crawler; // Need global-ish scope for addRequests inside handler

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
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed request: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeStreetsAhead() {
	logger.step("Starting Streets Ahead scraper (API version)...");

	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	crawler = createCrawler(browserWSEndpoint);

	const allRequests = PROPERTY_TYPES.map((type) => ({
		url: `${API_URL}?q_tenure=${type.tenure}&q_area=any-area&q_type=any-type&q_bedrooms=any-bedrooms&q_min_price=min-price-none&q_max_price=max-price-none&q_exclude_unavailable=exclude-unavalable&q_sort_direction=desc&q_page=1`,
		userData: {
			isRental: type.isRental,
			label: type.label,
			pageNum: 1,
			tenure: type.tenure,
		},
	}));

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	} else {
		logger.warn("No requests to process.");
	}

	logger.step(
		`Completed Streets Ahead - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, RENTALS: ${counts.savedRentals}`);

	logger.step("Updating remove status...");
	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeStreetsAhead();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
