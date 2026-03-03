// Frank Harris scraper using Playwright with Crawlee
// Agent ID: 51
// Usage:
// node backend/scraper-agent-51.js

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

const AGENT_ID = 51;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.frankharris.co.uk/properties/sales/status-available/",
		totalPages: 10, // Estimate based on server-backup (was 7)
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.frankharris.co.uk/properties/lettings/status-available/",
		totalPages: 5, // Estimate for rentals
		isRental: true,
		label: "RENTALS",
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

		await detailPage.waitForTimeout(1000);

		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);

		return {
			coords: {
				latitude: coords?.latitude || null,
				longitude: coords?.longitude || null,
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
		await page.waitForSelector(".property-card", { timeout: 20000 });
	} catch (e) {
		logger.error("Listing container .property-card not found", e, pageNum, label);
		return;
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const cards = document.querySelectorAll(".property-card");

			cards.forEach((card) => {
				const anchor = card.querySelector("a");
				let href = anchor ? anchor.getAttribute("href") : null;
				if (!href) return;

				const link = href.startsWith("http")
					? href.split("?")[0]
					: new URL(href, window.location.origin).origin + href.split("?")[0];

				// Extract title from property-card-content anchor title attribute (if available) or text
				const cardContent = card.querySelector(".property-card-content");
				let title = "Property";
				if (cardContent) {
					const titleAnchor = cardContent.querySelector("a[title]");
					if (titleAnchor) {
						title = titleAnchor.getAttribute("title");
					} else {
						title =
							cardContent.querySelector("h2, h3, .address")?.innerText?.trim() ||
							cardContent.innerText?.split("\n")[0]?.trim() ||
							"Property";
					}
				}

				const priceRaw =
					card.querySelector(".price > data, .price, .property-price")?.innerText?.trim() || "";

				// Extract bedrooms
				let bedText = "";
				const bedIcon = card.querySelector(".bed-baths, .icon-bedroom, [class*='bed']");
				if (bedIcon) {
					bedText = bedIcon.innerText || bedIcon.parentElement?.innerText || "";
				}

				const statusText = card.querySelector(".card-line, .status")?.innerText?.trim() || "";

				results.push({ link, title, priceRaw, bedText, statusText });
			});
			return results;
		} catch (e) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link) continue;

		if (isSoldProperty(property.statusText || "")) {
			continue;
		}

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		let bedrooms = null;
		const bedMatch = property.bedText.match(/(\d+)\s*bed/i) || property.bedText.match(/(\d+)/);
		if (bedMatch) bedrooms = parseInt(bedMatch[1]);

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

function createCrawler() {
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

async function scrapeFrankHarris() {
	logger.step("Starting Frank Harris (Agent 51) scraper...");

	const args = process.argv.slice(2);
	const startPageArg = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPageArg > 1;
	const scrapeStartTime = new Date();

	const crawler = createCrawler();
	const initialRequests = [];

	for (const type of PROPERTY_TYPES) {
		for (let i = startPageArg; i <= type.totalPages; i++) {
			initialRequests.push({
				url: `${type.baseUrl}page-${i}`,
				userData: {
					pageNum: i,
					isRental: type.isRental,
					label: type.label,
					totalPages: type.totalPages,
				},
			});
		}
	}

	if (initialRequests.length === 0) {
		logger.step("No pages to scrape.");
		return;
	}

	await crawler.run(initialRequests);

	logger.step("Scraping completed.");
	console.log(`\n--- Agent 51 Summary ---`);
	console.log(`Total Saved:   ${counts.totalSaved}`);
	console.log(`Total Scraped: ${counts.totalScraped}`);
	console.log(`Sales Saved:   ${counts.savedSales}`);
	console.log(`Rentals Saved: ${counts.savedRentals}`);
	console.log(`------------------------\n`);

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

scrapeFrankHarris().catch((err) => {
	logger.error("Fatal error in scraper", err);
	process.exit(1);
});
