// Meyer's Estates scraper using Playwright with Crawlee
// Agent ID: 3
// Usage:
// node backend/scraper-agent-3.js [startPage]

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

const AGENT_ID = 3;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl:
			"https://www.meyersestates.com/property-search/?department=residential-sales&per_page=13&view=list&maximum_price=999999999999&minimum_price=0&new_homes=false&include_stc=false&page=",
		totalPages: 50, // Default estimate, will be discovered during scraping
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl:
			"https://www.meyersestates.com/property-search/?department=residential-lettings&per_page=13&view=list&maximum_price=999999999999&minimum_price=0&new_homes=false&include_stc=false&page=",
		totalPages: 30, // Default estimate, will be discovered during scraping
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
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector("article.property", { timeout: 15000 });
	} catch (e) {
		logger.error("Listing container not found", e, pageNum, label);
	}

	const properties = await page.evaluate(() => {
	try {
		const results = [];
		const seenLinks = new Set();

		const propertyCards = document.querySelectorAll("article.property");

		for (const card of propertyCards) {

			// PROPERTY LINK
			const linkEl = card.querySelector("a.property__link");
			if (!linkEl) continue;

			let href = linkEl.getAttribute("href");
			if (!href) continue;

			const link = href.startsWith("http")
				? href
				: new URL(href, window.location.origin).href;

			if (seenLinks.has(link)) continue;
			seenLinks.add(link);


			// PROPERTY TITLE
			let title = "Property";
			const titleEl = card.querySelector("h2, h3");

			if (titleEl && titleEl.textContent.trim()) {
				title = titleEl.textContent.trim();
			}


			// PROPERTY PRICE
			let priceRaw = "";
			const priceEl = card.querySelector(".property__price");

			if (priceEl) {
				priceRaw = priceEl.textContent.trim();
			}


			// BEDROOMS
			let bedText = "";
			const bedMatch = card.innerText.match(/(\d+)\s*bed/i);

			if (bedMatch) {
				bedText = bedMatch[0];
			}


			// STATUS TEXT (used for SOLD/STC detection)
			const statusText = card.innerText || "";


			results.push({
				link,
				title,
				priceRaw,
				bedText,
				statusText,
			});
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
		let bedrooms = null;
		const bedMatch = property.bedText.match(/\d+/);
		if (bedMatch) bedrooms = parseInt(bedMatch[0]);

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
				null, // HTML not needed if we already have coords
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
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeMeyersEstates() {
	logger.step("Starting Meyer's Estates scraper...");

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
				url: `${type.baseUrl}${pg}`,
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
		`Completed Meyer's Estates - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, LETTINGS: ${counts.savedRentals}`);

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
		await scrapeMeyersEstates();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
