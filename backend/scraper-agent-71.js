// Hawes & Co scraper using Playwright with Crawlee
// Agent ID: 71
// Usage:
// node backend/scraper-agent-71.js

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

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 71;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// Configuration for sales and rentals
const PROPERTY_TYPES = [
	{
		urlPath: "properties-for-sale",
		totalRecords: 185,
		recordsPerPage: 12,
		isRental: false,
		label: "SALE",
	},
	{
		urlPath: "properties-to-rent",
		totalRecords: 36,
		recordsPerPage: 12,
		isRental: true,
		label: "RENTAL",
	},
];

const processedUrls = new Set();

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
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
		await page.waitForSelector(".property", { timeout: 30000 });
	} catch (e) {
		logger.error("Property selector not found", e, pageNum, label);
	}

	const properties = await page.$$eval(".property", (listings) => {
		const results = [];
		const seenLinks = new Set();

		listings.forEach((listing) => {
			try {
				// Extract link from data-link attribute
				const innerWrapper = listing.querySelector(".inner_wrapper");
				let link = innerWrapper ? innerWrapper.getAttribute("data-link") : null;
				if (link && !link.startsWith("http")) {
					link = "https://www.hawesandco.co.uk" + link;
				}

				// Extract price from .sale_price
				const priceEl = listing.querySelector(".sale_price");
				let priceRaw = null;
				if (priceEl) {
					priceRaw = priceEl.textContent.trim();
				}

				// Extract title from .blurb
				let title = null;
				const blurbEl = listing.querySelector(".blurb");
				if (blurbEl) {
					title = blurbEl.textContent.trim();
				}
				if (!title) {
					const headerLinkEl = listing.querySelector(".info_section__header__left a");
					if (headerLinkEl) {
						title = headerLinkEl.textContent.trim();
					}
				}

				// Extract bedrooms from .info_section__room.beds
				let bedrooms = null;
				const bedroomEl = listing.querySelector(".info_section__room.beds");
				if (bedroomEl) {
					const bedroomText = bedroomEl.textContent.trim();
					const bedroomMatch = bedroomText.match(/(\d+)/);
					if (bedroomMatch) {
						bedrooms = bedroomMatch[1];
					}
				}

				const statusText = listing.innerText || "";

				if (link && priceRaw && title) {
					results.push({
						link: link,
						title: title,
						priceRaw: priceRaw,
						bedrooms: bedrooms,
						statusText: statusText,
					});
				}
			} catch (err) {
				// Skip problematic listings
			}
		});

		return results;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link) continue;

		if (isSoldProperty(property.statusText || "")) continue;

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		let bedrooms = null;
		if (property.bedrooms) bedrooms = parseInt(property.bedrooms);

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
		} else if (result.isExisting) {
			counts.totalScraped++;
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

async function scrapeHawesAndCo() {
	logger.step(`Starting Hawes & Co scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const crawler = new PlaywrightCrawler({
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
				headless: true,
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} (${type.totalRecords} pages)`);
		const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
		for (let pg = Math.max(1, startPage); pg <= totalPages; pg++) {
			allRequests.push({
				url: `https://www.hawesandco.co.uk/${type.urlPath}/all-properties/!/page/${pg}`,
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: type.label,
					totalPages: totalPages,
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
		`Completed Hawes & Co - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, LETTINGS: ${counts.savedRentals}`);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

// Main execution
(async () => {
	try {
		await scrapeHawesAndCo();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
