// Snellers scraper using Playwright with Crawlee
// Agent ID: 19
// Usage:
// node backend/scraper-agent-19.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	parsePrice,
	formatPriceDisplay,
	extractCoordinatesFromHTML,
	isSoldProperty,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 19;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
};

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
}

// ============================================================================
// PROPERTY TYPES CONFIGURATION
// ============================================================================

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.snellers.co.uk/properties/sales/status-available",
		totalPages: 14,
		recordsPerPage: 12,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.snellers.co.uk/properties/lettings/status-available",
		totalPages: 20,
		recordsPerPage: 12,
		isRental: true,
		label: "RENTALS",
	},
];


// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function parsePropertyCard($card) {
	try {
		// Get link
		const linkEl = $card.find("a.no-decoration").first();
		let href = linkEl.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.snellers.co.uk" + href;

		// Get title
		const title = linkEl.attr("title") || linkEl.text().trim();
		if (!title) return null;

		// Get and validate price
		const priceText =
			$card.find(".price .money").text().trim() || $card.find(".price").text().trim();
		const price = parsePrice(priceText);
		if (!price) return null;

		// Get bedrooms
		const bedroomsText = $card.find(".bed-baths li:nth-child(1)").text().trim() || "";
		// Extract numeric part from "6 bedrooms"
		const bedrooms = bedroomsText.match(/\d+/) ? parseInt(bedroomsText.match(/\d+/)[0], 10) : null;

		return {
			link,
			title,
			price,
			bedrooms,
		};
	} catch (error) {
		logger.error(`Error parsing property card`, error);
		return null;
	}
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
// REQUEST HANDLER FOR LISTING PAGES
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData || {};
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.goto(request.url, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		// Wait for properties to load
		await page
			.waitForSelector(".property-card", { timeout: 30000 })
			.catch(() => {
				logger.warn(`No properties found on page ${pageNum}`);
			});

		// Wait for dynamic content
		await page.waitForTimeout(1500);

		// Parse properties from listing page using Cheerio
		const htmlContent = await page.content();
		const $ = cheerio.load(htmlContent);

		const propertyElements = [];
		$(".property-card").each((index, element) => {
			const property = parsePropertyCard($(element));
			if (property) {
				propertyElements.push(property);
			}
		});

		logger.page(pageNum, label, `Found ${propertyElements.length} properties`, totalPages);

		// Process each property
		for (const property of propertyElements) {
			if (!property.link || !property.price) {
				counts.totalSkipped++;
				continue;
			}

			counts.totalFound++;

			// Skip sold properties
			if (isSoldProperty(property)) {
				logger.property(
					pageNum,
					label,
					property.title.substring(0, 40),
					formatPriceDisplay(null, isRental),
					property.link,
					isRental,
					totalPages,
					"SKIPPED",
				);
				counts.totalSkipped++;
				continue;
			}

			// Update price in database (or insert minimal record if new)
			const result = await updatePriceByPropertyURLOptimized(
				property.link,
				property.price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let action = "UNCHANGED";

			if (result.updated) {
				action = "UPDATED";
				counts.totalSaved++;
				counts.totalScraped++;
			}

			// If new property, scrape full details immediately
			if (!result.isExisting && !result.error) {
				action = "CREATED";
				let latitude = null;
				let longitude = null;

				// Fetch detail page for coordinates
				const detailPage = await page.context().newPage();
				try {
					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});
					await detailPage.waitForTimeout(500);

					const detailHTML = await detailPage.content();
					const coords = await extractCoordinatesFromHTML(detailHTML);
					latitude = coords.latitude;
					longitude = coords.longitude;
				} catch (err) {
					logger.warn(`Error fetching detail page for ${property.link}: ${err.message}`);
				} finally {
					await detailPage.close();
				}

				// Save property with coordinates
				await processPropertyWithCoordinates(
					property.link,
					property.price,
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
			} else if (result.error) {
				action = "ERROR";
				counts.totalSkipped++;
			} else if (result.isExisting) {
				counts.totalScraped++;
			}

			logger.property(
				pageNum,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(property.price, isRental),
				property.link,
				isRental,
				totalPages,
				action,
			);

			if (action !== "UNCHANGED") {
				await sleep(500);
			}
		}
	} catch (error) {
		logger.error(`Error processing page ${pageNum} for ${label}`, error);
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
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			const { pageNum, label } = request.userData || {};
			logger.error(`Failed listing page: ${request.url}`, null, pageNum, label);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeSnellers() {
	logger.step(`Starting Snellers scraper (Agent ${AGENT_ID})...`);

	const startPage = getStartPage();
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	if (isPartialRun) {
		logger.step(`Partial run detected (startPage=${startPage}). Remove status update will be skipped.`);
	}

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	// Process each property type
	for (const propertyType of PROPERTY_TYPES) {
		const effectiveStartPage = Math.max(1, startPage);

		for (let pg = effectiveStartPage; pg <= propertyType.totalPages; pg++) {
			const url =
				pg === 1 ? `${propertyType.urlBase}` : `${propertyType.urlBase}/page-${pg}`;
			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					totalPages: propertyType.totalPages,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Snellers - Found: ${counts.totalFound}, Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Skipped: ${counts.totalSkipped}`,
	);

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
		await scrapeSnellers();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
