// Newton Fallowell scraper using Playwright with Crawlee
// Agent ID: 248
// Usage:
// node backend/scraper-agent-248.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 248;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl:
			"https://www.newtonfallowell.co.uk/properties/for-sale/in-the-midlands/?orderby=price_desc&radius=3",
		label: "SALES",
		isRental: false,
	},
	{
		baseUrl:
			"https://www.newtonfallowell.co.uk/properties/for-letting/in-the-midlands/?orderby=price_desc&radius=3",
		label: "RENTALS",
		isRental: true,
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();
const scrapeStartTime = new Date();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

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
			timeout: 60000,
		});

		const detailData = await detailPage.evaluate(() => {
			try {
				const mapEl = document.querySelector("#leaflet-map-single-property-container");
				if (mapEl) {
					return {
						lat: parseFloat(mapEl.getAttribute("data-lat")),
						lng: parseFloat(mapEl.getAttribute("data-lng")),
					};
				}
				return null;
			} catch (e) {
				return null;
			}
		});

		if (detailData && detailData.lat && detailData.lng) {
			return {
				latitude: detailData.lat,
				longitude: detailData.lng,
			};
		}
		return null;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;

	// Determine total pages on the first page
	let totalPages = request.userData.totalPages || 0;
	if (pageNum === 1) {
		totalPages = await page.evaluate(() => {
			const resultsText = document.querySelector("p.fs-4.color-text")?.innerText;
			if (resultsText && resultsText.includes("of")) {
				const parts = resultsText.split("of");
				if (parts.length > 1) {
					const totalResults = parseInt(parts[1].trim());
					const itemsShown = parseInt(parts[0].replace("Showing", "").trim());
					if (!isNaN(totalResults) && itemsShown > 0) {
						return Math.ceil(totalResults / itemsShown);
					}
				}
			}

			// Fallback to page numbers
			const pageLinks = Array.from(
				document.querySelectorAll(".page-numbers .page-item a.page-divnk"),
			);
			if (pageLinks.length === 0) return 1;
			const pages = pageLinks.map((a) => parseInt(a.innerText)).filter((n) => !isNaN(n));
			return pages.length > 0 ? Math.max(...pages) : 1;
		});
	}

	logger.page(pageNum, label, request.url, totalPages);

	const properties = await page.evaluate((isRental) => {
		const cards = Array.from(document.querySelectorAll(".property--card"));
		return cards
			.map((card) => {
				const linkEl = card.querySelector(".property-title--search a");
				const priceEl = card.querySelector(".property-price--search");
				const typeEl = card.querySelector(".property-type--search");

				if (!linkEl || !priceEl) return null;

				const title = linkEl.innerText.trim();
				const link = linkEl.href;
				const priceText = priceEl.innerText.trim();
				const typeText = typeEl ? typeEl.innerText.trim() : "";

				// Extract bedroom count from typeText e.g. "6 bedroom Detached house"
				let bedrooms = null;
				const bedMatch = typeText.match(/(\d+)\s*bedroom/i);
				if (bedMatch) bedrooms = parseInt(bedMatch[1]);

				return {
					title,
					link,
					price: priceText,
					bedrooms,
					isRental,
					isSold:
						typeText.toLowerCase().includes("sold") ||
						typeText.toLowerCase().includes("stc") ||
						typeText.toLowerCase().includes("under offer"),
				};
			})
			.filter((p) => p !== null);
	}, isRental);

	for (const property of properties) {
		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		if (property.isSold) {
			logger.property(
				pageNum,
				label,
				property.title,
				property.price,
				property.link,
				isRental,
				totalPages,
				"SKIPPED",
			);
			continue;
		}

		counts.totalScraped++;

		const dbResult = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		if (dbResult.isExisting && !dbResult.updated) {
			logger.property(
				pageNum,
				label,
				property.title,
				property.price,
				property.link,
				isRental,
				totalPages,
				"UNCHANGED",
			);
			continue;
		}

		if (dbResult.updated) {
			logger.property(
				pageNum,
				label,
				property.title,
				property.price,
				property.link,
				isRental,
				totalPages,
				"UPDATED",
			);
		} else {
			// New property, needs detail scrape for coordinates
			const coords = await scrapePropertyDetail(page.context(), property);

			const finalProperty = {
				url: property.link,
				price: property.price,
				title: property.title,
				bedrooms: property.bedrooms,
				agentId: AGENT_ID,
				isRent: isRental,
				coords: coords || { latitude: null, longitude: null },
			};

			const saveResult = await processPropertyWithCoordinates(
				finalProperty.url,
				finalProperty.price,
				finalProperty.title,
				finalProperty.bedrooms,
				finalProperty.agentId,
				finalProperty.isRent,
				null, // no html needed since we have coords
				finalProperty.coords.latitude,
				finalProperty.coords.longitude,
			);

			if (saveResult && !saveResult.error) {
				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;

				logger.property(
					pageNum,
					label,
					property.title,
					property.price,
					property.link,
					isRental,
					totalPages,
					"CREATED",
					finalProperty.coords.latitude,
					finalProperty.coords.longitude,
				);

				// Apply polite sleep only for CREATED properties
				await sleep(500);
			}
		}
	}

	// Queue next page if page 1
	if (pageNum === 1 && totalPages > 1) {
		const requests = [];
		for (let i = 2; i <= totalPages; i++) {
			// Replace page-1 with page-X or inject if missing
			let nextUrl;
			if (request.url.includes("/page-1/")) {
				nextUrl = request.url.replace("/page-1/", `/page-${i}/`);
			} else {
				// Handle potential missing /page-1/ in initial URLs
				nextUrl = request.url.replace("/properties/", `/properties/page-${i}/`);
			}

			requests.push({
				url: nextUrl,
				userData: {
					pageNum: i,
					totalPages,
					isRental,
					label,
				},
			});
		}
		await crawler.addRequests(requests);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function run() {
	const startPageArg = parseInt(process.argv[2]) || 1;
	const isPartialRun = startPageArg > 1;

	const crawler = new PlaywrightCrawler({
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 180,
		maxConcurrency: 1,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
	});

	const initialRequests = [];
	for (const type of PROPERTY_TYPES) {
		let pagedUrl;
		if (isPartialRun) {
			pagedUrl = type.baseUrl.replace("/properties/", `/properties/page-${startPageArg}/`);
		} else {
			pagedUrl = type.baseUrl.replace("/properties/", "/properties/page-1/");
		}

		initialRequests.push({
			url: pagedUrl,
			userData: {
				pageNum: isPartialRun ? startPageArg : 1,
				isRental: type.isRental,
				label: type.label,
				totalPages: null, // will be detected for page 1
			},
		});
	}

	logger.step(`Starting Newton Fallowell scraper (Agent ${AGENT_ID})...`);
	await crawler.run(initialRequests);

	if (!isPartialRun) {
		logger.step("Marking removed properties...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}

	logger.step(`
=========================================
 Newton Fallowell Scraper Summary
=========================================
 Total Scraped: ${counts.totalScraped}
 Total Saved:   ${counts.totalSaved} (Sales: ${counts.savedSales}, Rent: ${counts.savedRentals})
=========================================
	`);
}

run().catch((err) => {
	logger.error(`Fatal error: ${err.message}`);
	process.exit(1);
});
