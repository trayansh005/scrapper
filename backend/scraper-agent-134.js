// Stratton Creber scraper using Playwright with Crawlee
// Agent ID: 134
// Website: strattoncreber.co.uk
// Usage:
// node backend/scraper-agent-134.js [startPage]

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

const AGENT_ID = 134;
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

		// Stratton Creber coordinates are in propertyObject in script tags
		const latMatch = htmlContent.match(/ga4_property_latitude:\s*([0-9.-]+)/);
		const lngMatch = htmlContent.match(/ga4_property_longitude:\s*([0-9.-]+)/);

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
					link = "https://www.strattoncreber.co.uk" + link;
				}

				const titleEl = card.querySelector(".card__text-content");
				const title = titleEl ? titleEl.textContent.trim() : "Stratton Creber Property";

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
				args: [
					"--no-sandbox",
					"--disable-setuid-sandbox",
					"--disable-blink-features=AutomationControlled",
				],
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

async function scrapeStrattonCreber() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Stratton Creber Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const AREAS = [
		{
			type: "sales",
			label: "SALES",
			isRental: false,
			totalRecords: 157,
			urlPath: "properties/sales/status-available/most-recent-first",
		},
		{
			type: "lettings",
			label: "LETTINGS",
			isRental: true,
			totalRecords: 25,
			urlPath: "properties/lettings/status-available/most-recent-first",
		},
	];

	for (const area of AREAS) {
		const recordsPerPage = 10;
		const totalPages = Math.ceil(area.totalRecords / recordsPerPage);
		const requests = [];

		for (let p = Math.max(1, startPage); p <= totalPages; p++) {
			const url = `https://www.strattoncreber.co.uk/${area.urlPath}/page-${p}#/`;
			requests.push({
				url,
				userData: {
					pageNumber: p,
					totalPages,
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
		`Finished Stratton Creber - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
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
		await scrapeStrattonCreber();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
