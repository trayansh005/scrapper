// Scott City scraper using Playwright with Crawlee
// Agent ID: 207
// Website: scottcity.co.uk
// Usage:
// node backend/scraper-agent-207.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 207;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

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
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNumber, totalPages } = request.userData;
	logger.page(pageNumber, label, request.url, totalPages);

	try {
		// Wait for property list to load
		await page.waitForSelector(".property-list .property", { timeout: 30000 }).catch(() => {
			logger.error(`No property cards found on page ${pageNumber}`, null, pageNumber, label);
		});

		// Extract both property cards AND the JSON data object from the page context
		const pageData = await page.evaluate(() => {
			// Extract coordinates from var properties declared in script tag
			// We can access it directly since it's global
			const rawProperties = window.properties || [];

			const cards = Array.from(document.querySelectorAll(".property-list .property"));
			const properties = cards.map((card) => {
				const linkEl = card.querySelector("a.property-description-link");
				let link = linkEl ? linkEl.getAttribute("href") : null;
				if (link && !link.startsWith("http")) {
					link = "https://www.scottcity.co.uk" + link;
				}

				const titleEl = card.querySelector(".list-address");
				const title = titleEl ? titleEl.textContent.trim() : "Scott City Property";

				const bedEl = card.querySelector("li.FeaturedProperty__list-stats-item--bedrooms span");
				const bedroomsText = bedEl ? bedEl.textContent.trim() : "";
				const bedroomsMatch = bedroomsText.match(/\d+/);
				const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[0]) : null;

				const priceEl = card.querySelector(".list-price");
				const priceText = priceEl ? priceEl.textContent.trim() : "";

				const bookmarkEl = card.querySelector("a.add_bookmark.bookmark");
				const dataId = bookmarkEl ? bookmarkEl.getAttribute("data-id") : null;

				const statusText = card.innerText || "";

				return {
					link,
					title,
					priceText,
					bedrooms,
					dataId,
					statusText,
				};
			});

			return { properties, rawProperties };
		});

		logger.page(pageNumber, label, `Found ${pageData.properties.length} properties`, totalPages);

		for (const property of pageData.properties) {
			if (!property.link || !property.priceText) continue;

			if (isSoldProperty(property.statusText)) {
				continue;
			}

			const price = parsePrice(property.priceText);
			if (!price) continue;

			// Find coordinates from JSON
			let latitude = null;
			let longitude = null;
			if (property.dataId && pageData.rawProperties.length > 0) {
				const found = pageData.rawProperties.find((p) => p.PropertyId === property.dataId);
				if (found) {
					latitude = found.latitude || null;
					longitude = found.longitude || null;
				}
			}

			const updateResult = await updatePriceByPropertyURLOptimized(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let action = "SEEN";

			if (updateResult.updated) {
				stats.totalSaved++;
				action = "UPDATED";
			}

			if (!updateResult.isExisting && !updateResult.error) {
				action = "CREATED";
				// Use the coordinate-aware persistence helper
				await processPropertyWithCoordinates(
					property.link,
					price,
					property.title,
					property.bedrooms,
					AGENT_ID,
					isRental,
					null, // HTML not needed if we have coords
					latitude,
					longitude,
				);
				stats.totalScraped++;
				stats.totalSaved++;
				if (isRental) stats.savedRentals++;
				else stats.savedSales++;
			} else if (updateResult.error) {
				action = "ERROR";
			}

			logger.property(
				pageNumber,
				label,
				property.title.substring(0, 40),
				`£${price}`,
				property.link,
				isRental,
				totalPages,
				action,
			);
		}
	} catch (error) {
		logger.error(`Error in handleListingPage: ${error.message}`, error, pageNumber, label);
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
		requestHandlerTimeoutSecs: 600,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
				await page.setExtraHTTPHeaders({
					"User-Agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
				});
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
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

async function scrapeScottCity() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Scott City Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const AREAS = [
		{
			label: "SALES",
			isRental: false,
			baseUrl: "https://www.scottcity.co.uk/buy/property-for-sale/?page=",
			totalPages: 2, // From snippet
		},
		{
			label: "LETTINGS",
			isRental: true,
			baseUrl: "https://www.scottcity.co.uk/let/property-to-let/?page=",
			totalPages: 2, // Safe assumption
		},
	];

	for (const area of AREAS) {
		const requests = [];

		for (let p = Math.max(1, startPage); p <= area.totalPages; p++) {
			requests.push({
				url: `${area.baseUrl}${p}`,
				userData: {
					pageNumber: p,
					totalPages: area.totalPages,
					isRental: area.isRental,
					label: area.label,
				},
			});
		}

		if (requests.length > 0) {
			logger.step(`Queueing ${requests.length} ${area.label} listing pages...`);
			await crawler.addRequests(requests);
		}
	}

	await crawler.run();

	logger.step(
		`Finished Scott City - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);

	if (startPage === 1) {
		logger.step("Updating remove status for properties not seen in this run...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeScottCity();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
