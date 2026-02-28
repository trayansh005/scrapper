// JLL Residential scraper using Playwright with Crawlee
// Agent ID: 72
// Website: residential.jll.co.uk
// Usage:
// node backend/scraper-agent-72.js [startPage]

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

const AGENT_ID = 72;
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
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// Small delay to ensure JS execution if needed
		await detailPage.waitForTimeout(1000);

		// Extract coordinates from window.__NEXT_DATA__
		const detailData = await detailPage.evaluate(() => {
			try {
				const nextData = window.__NEXT_DATA__;
				if (
					nextData &&
					nextData.props &&
					nextData.props.pageProps &&
					nextData.props.pageProps.property
				) {
					const p = nextData.props.pageProps.property;
					return {
						lat: p.lat || null,
						lng: p.lng || null,
						html: document.documentElement.innerHTML,
					};
				}
			} catch (e) {
				// Fallback or error
			}
			return { lat: null, lng: null, html: document.documentElement.innerHTML };
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			detailData.html,
			detailData.lat,
			detailData.lng,
		);

		stats.totalScraped++;
		stats.totalSaved++;
		if (isRental) stats.savedRentals++;
		else stats.savedSales++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNumber, totalPages } = request.userData;
	logger.page(pageNumber, label, request.url, totalPages);

	try {
		// Wait for property cards to be visible
		await page.waitForSelector(".property-card", { timeout: 30000 }).catch(() => {
			logger.error(`No property cards found on page ${pageNumber}`, null, pageNumber, label);
		});

		const properties = await page.evaluate(() => {
			const cards = Array.from(document.querySelectorAll(".property-card"));
			return cards.map((card) => {
				const linkEl = card.querySelector("a");
				const link = linkEl ? linkEl.href : null;

				// Selector discovered via browser subagent
				const titleEl = card.querySelector(
					".property-card__details > div:nth-child(2) > div:nth-child(1)",
				);
				const title = titleEl ? titleEl.textContent.trim() : "JLL Property";

				const detailsText = card.querySelector(".property-card__details")?.innerText || "";

				// Price extraction
				const priceMatch = detailsText.match(/£([\d,]+)/);
				const priceText = priceMatch ? priceMatch[0] : "";

				// Bedroom extraction
				const bedEl = card.querySelector(".property-card__details div:nth-child(3) span");
				const bedText = bedEl ? bedEl.textContent.trim() : "";
				const bedMatch = bedText.match(/(\d+)/);
				const bedrooms = bedMatch ? parseInt(bedMatch[1]) : null;

				return {
					link,
					title,
					priceText,
					bedrooms,
					statusText: detailsText,
				};
			});
		});

		logger.page(pageNumber, label, `Found ${properties.length} properties`, totalPages);

		for (const property of properties) {
			if (!property.link || !property.priceText) continue;

			if (isSoldProperty(property.statusText)) {
				continue;
			}

			const price = parsePrice(property.priceText);
			if (!price) continue;

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
				// New property, need coordinates from detail page
				await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
				// Small delay between detail pages
				await new Promise((r) => setTimeout(r, 1000));
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

async function scrapeJLL() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting JLL Residential Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const totalPages = 20; // Reasonable limit for JLL sales
	const requests = [];

	for (let p = Math.max(1, startPage); p <= totalPages; p++) {
		const url = `https://residential.jll.co.uk/search?tenureType=sale&sortBy=price&sortDirection=desc&page=${p}`;
		requests.push({
			url,
			userData: {
				pageNumber: p,
				totalPages,
				isRental: false,
				label: "SALES",
			},
		});
	}

	if (requests.length > 0) {
		logger.step(`Queueing ${requests.length} listing pages starting from page ${startPage}...`);
		await crawler.run(requests);
	}

	logger.step(
		`Finished JLL - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);

	// Only update remove status if we did a full run
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
		await scrapeJLL();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
