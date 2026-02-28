// BridgFords scraper using Playwright with Crawlee
// Agent ID: 127
// Website: bridgfords.co.uk
// Usage:
// node backend/scraper-agent-127.js [startPage]

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

const AGENT_ID = 127;
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

		// Small delay to ensure content is loaded
		await detailPage.waitForTimeout(1000);

		const htmlContent = await detailPage.content();

		// BridgFords coordinates are in HTML comments
		const latMatch = htmlContent.match(/<!--property-latitude:"([0-9.-]+)"-->/);
		const lngMatch = htmlContent.match(/<!--property-longitude:"([0-9.-]+)"-->/);

		let latitude = null;
		let longitude = null;
		if (latMatch && lngMatch) {
			latitude = parseFloat(latMatch[1]);
			longitude = parseFloat(lngMatch[1]);
		}

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			htmlContent,
			latitude,
			longitude,
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
		// Wait for property cards
		await page.waitForSelector(".hf-property-results .card", { timeout: 30000 }).catch(() => {
			logger.error(`No property cards found on page ${pageNumber}`, null, pageNumber, label);
		});

		const properties = await page.evaluate(() => {
			const cards = Array.from(document.querySelectorAll(".hf-property-results .card"));
			return cards.map((card) => {
				const linkEl = card.querySelector("a");
				let link = linkEl ? linkEl.getAttribute("href") : null;
				if (link && !link.startsWith("http")) {
					link = "https://www.bridgfords.co.uk" + link;
				}

				const titleEl = card.querySelector(".card__text-content");
				const title = titleEl ? titleEl.textContent.trim() : "BridgFords Property";

				const bedroomsEl = card.querySelector(".card-content__spec-list-number");
				let bedrooms = null;
				if (bedroomsEl) {
					const bedroomsText = bedroomsEl.textContent.trim();
					const bedroomsMatch = bedroomsText.match(/\d+/);
					if (bedroomsMatch) {
						bedrooms = parseInt(bedroomsMatch[0]);
					}
				}

				const priceEl = card.querySelector(".card__heading");
				const priceText = priceEl ? priceEl.textContent.trim() : "";

				const statusText = card.innerText || "";

				return {
					link,
					title,
					priceText,
					bedrooms,
					statusText,
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
				// New property
				await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
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

async function scrapeBridgFords() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting BridgFords Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	// BridgFords usually has around 40-50 pages for lettings
	const totalPages = 50;
	const requests = [];

	for (let p = Math.max(1, startPage); p <= totalPages; p++) {
		const url = `https://www.bridgfords.co.uk/properties/lettings/status-available/most-recent-first/page-${p}#/`;
		requests.push({
			url,
			userData: {
				pageNumber: p,
				totalPages,
				isRental: true,
				label: "LETTINGS",
			},
		});
	}

	if (requests.length > 0) {
		logger.step(`Queueing ${requests.length} listing pages starting from page ${startPage}...`);
		await crawler.run(requests);
	}

	logger.step(
		`Finished BridgFords - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
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
		await scrapeBridgFords();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
