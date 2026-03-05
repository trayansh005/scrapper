// Romans scraper using Playwright with Crawlee
// Agent ID: 16
// Website: www.romans.co.uk
// Usage:
// node backend/scraper-agent-16.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	extractCoordinatesFromHTML,
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { logMemoryUsage, blockNonEssentialResources } = require("./lib/scraper-utils.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { EventEmitter } = require("events");

// Increase max listeners to prevent memory leak warnings
EventEmitter.defaultMaxListeners = 100;

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 16;
const logger = createAgentLogger(AGENT_ID);

const TIMING = {
	PAGE_DELAY_MIN: 500,
	PAGE_DELAY_MAX: 1200,
	DETAIL_DELAY_MIN: 400,
	DETAIL_DELAY_MAX: 900,
	AFTER_GOTO: 500,
	COOKIE_BANNER: 1000,
	STREETVIEW_LOAD: 4000,
	STREETVIEW_RETRY: 1500,
	RATE_LIMIT_BACKOFF: 60000,
};

const SELECTORS = {
	PROPERTY_CARD: ".property-card-wrapper",
	PROPERTY_LINK: 'a[href*="/properties"]',
	PROPERTY_TITLE: ".property-title h2",
	PROPERTY_PRICE: ".property-price",
	PROPERTY_STATUS: ".property-status",
	BEDROOMS_ICON: ".icon-bed",
	COOKIE_ACCEPT: 'button:has-text("Accept all")',
	STREETVIEW_BUTTON: 'button:has-text("Streetview")',
	GOOGLE_MAPS_LINK: 'a[href*="google.com/maps/@"]',
};

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.romans.co.uk/properties/for-sale",
		isRental: false,
		label: "SALES",
		totalRecords: 876,
		recordsPerPage: 8,
	},
	{
		urlBase: "https://www.romans.co.uk/properties/to-rent",
		isRental: true,
		label: "LETTINGS",
		totalRecords: 537,
		recordsPerPage: 8,
	},
];

// ============================================================================
// STATE
// ============================================================================

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedLettings: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function randBetween(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// PAGE SETUP HELPERS
// ============================================================================

async function dismissCookieBanner(page) {
	try {
		const acceptButton = await page.locator(SELECTORS.COOKIE_ACCEPT).first();
		const isVisible = await acceptButton.isVisible({ timeout: 2000 }).catch(() => false);

		if (isVisible) {
			await acceptButton.click();
			await page.waitForTimeout(TIMING.COOKIE_BANNER);
		}
	} catch (error) {
		// No cookie banner or already dismissed
	}
}

// ============================================================================
// COORDINATE EXTRACTION
// ============================================================================

async function extractCoordinatesFromStreetview(page) {
	try {
		const streetviewBtn = await page.locator(SELECTORS.STREETVIEW_BUTTON).first();
		const isVisible = await streetviewBtn.isVisible({ timeout: 5000 }).catch(() => false);

		if (!isVisible) {
			return { latitude: null, longitude: null };
		}

		await streetviewBtn.click();
		await page.waitForTimeout(TIMING.STREETVIEW_LOAD);

		// Retry coordinate extraction up to 5 times
		for (let retry = 0; retry < 5; retry++) {
			const googleMapsCoords = await page.evaluate(() => {
				const link = document.querySelector('a[href*="google.com/maps/@"]');
				if (link) {
					const href = link.getAttribute("href");
					const match = href.match(/@([\d.-]+),([\d.-]+)/);
					if (match) {
						return {
							lat: parseFloat(match[1]),
							lng: parseFloat(match[2]),
						};
					}
				}
				return null;
			});

			if (googleMapsCoords?.lat && googleMapsCoords?.lng) {
				return {
					latitude: googleMapsCoords.lat,
					longitude: googleMapsCoords.lng,
				};
			}

			if (retry < 4) {
				await page.waitForTimeout(TIMING.STREETVIEW_RETRY);
			}
		}

		return { latitude: null, longitude: null };
	} catch (error) {
		logger.error(`Could not extract streetview coords: ${error.message}`);
		return { latitude: null, longitude: null };
	}
}

async function extractCoordinates(htmlContent, detailPage) {
	// Try HTML extraction first (faster)
	const htmlCoords = await extractCoordinatesFromHTML(htmlContent);

	if (htmlCoords.latitude && htmlCoords.longitude) {
		return htmlCoords;
	}

	// Fall back to Streetview extraction
	return await extractCoordinatesFromStreetview(detailPage);
}

// ============================================================================
// PROPERTY PARSING
// ============================================================================

function parseBedrooms($card) {
	const bedEl = $card.find(SELECTORS.BEDROOMS_ICON);

	if (bedEl.length && bedEl.parent().length) {
		const bedText = bedEl.parent().text().trim();
		const bedMatch = bedText.match(/(\d+)/);
		return bedMatch ? bedMatch[1] : null;
	}

	return null;
}

function parsePropertyCard($card) {
	try {
		// Get link
		const linkEl = $card.find(SELECTORS.PROPERTY_LINK).first();
		let href = linkEl.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.romans.co.uk" + href;

		// Get title
		const title = $card.find(SELECTORS.PROPERTY_TITLE).text().trim();
		if (!title) return null;

		// Get and validate price
		const priceText = $card.find(SELECTORS.PROPERTY_PRICE).text().trim();
		const cardText = $card.text() || "";

		// Skip sold properties
		if (isSoldProperty(cardText) || isSoldProperty(priceText)) {
			return null;
		}

		const price = parsePrice(priceText);
		if (!price) return null;

		// Get bedrooms
		const bedrooms = parseBedrooms($card);

		// Check and skip "Let Agreed" status
		const status = $card.find(SELECTORS.PROPERTY_STATUS).text().trim();
		if (status === "Let Agreed") {
			return null;
		}

		return {
			link,
			title,
			price,
			bedrooms,
		};
	} catch (error) {
		return null;
	}
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const properties = [];

	$(SELECTORS.PROPERTY_CARD).each((index, element) => {
		const property = parsePropertyCard($(element));
		if (property) {
			properties.push(property);
		}
	});

	return properties;
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

		await dismissCookieBanner(detailPage);

		// Get HTML content and extract coordinates
		const htmlContent = await detailPage.content();
		const coords = await extractCoordinates(htmlContent, detailPage);

		// Save property to database
		const dbResult = await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			coords.latitude,
			coords.longitude,
		);

		counts.totalScraped++;
		counts.totalSaved++;
		if (isRental) counts.savedLettings++;
		else counts.savedSales++;

		return dbResult;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
		return { latitude: null, longitude: null };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNumber, isRental, label, totalPages } = request.userData || {};
	logger.page(pageNumber, label, request.url, totalPages);

	const response = await page.goto(request.url, {
		waitUntil: "domcontentloaded",
		timeout: 60000,
	});

	if (response?.status?.() === 429) {
		logger.warn(`429 on ${request.url} — backing off`);
		await sleep(TIMING.RATE_LIMIT_BACKOFF);
		throw new Error("429 Rate Limit");
	}

	await page.waitForTimeout(TIMING.AFTER_GOTO);
	await dismissCookieBanner(page);

	// Wait for properties to load
	await page.waitForSelector(SELECTORS.PROPERTY_CARD, { timeout: 30000 }).catch(() => {});

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);
	logger.page(pageNumber, label, `Found ${properties.length} properties`, totalPages);

	// Process each property
	for (const property of properties) {
		if (processedUrls.has(property.link)) {
			logger.property(
				pageNumber,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(property.price, isRental),
				property.link,
				isRental,
				totalPages,
				"SKIPPED: ALREADY PROCESSED",
			);
			continue;
		}
		processedUrls.add(property.link);

		// Update price in database
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		let action = "UNCHANGED";
		let coords = { latitude: null, longitude: null };

		if (result.updated) {
			action = "UPDATED";
			counts.totalSaved++;
		}

		// If new property, scrape full details immediately
		if (!result.isExisting && !result.error) {
			action = "CREATED";
			coords = await scrapePropertyDetail(page.context(), property, isRental);
		} else if (result.error) {
			action = "ERROR";
		}

		logger.property(
			pageNumber,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(property.price, isRental),
			property.link,
			isRental,
			totalPages,
			action,
			coords.latitude,
			coords.longitude,
		);

		if (action === "CREATED") {
			await sleep(1000);
		}
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 600,
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
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

async function scrapeRomans() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Romans scraper (Agent ${AGENT_ID})...`);
	logMemoryUsage("START");

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
		const validStartPage = startPage > 0 && startPage <= totalPages ? startPage : 1;

		logger.step(`Queueing ${type.label} pages: ${totalPages} (starting from ${validStartPage})`);

		for (let pg = validStartPage; pg <= totalPages; pg++) {
			allRequests.push({
				url: `${type.urlBase}/page-${pg}/`,
				userData: {
					pageNumber: pg,
					totalPages,
					isRental: type.isRental,
					label: type.label,
				},
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	} else {
		logger.warn("No pages to scrape with current arguments.");
	}

	logger.step(
		`Completed Romans - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status for properties not seen in this run...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}

	logMemoryUsage("END");
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeRomans();
		logger.step("All done!");
		process.exit(0);
	} catch (error) {
		logger.error("Fatal error", error);
		process.exit(1);
	}
})();
