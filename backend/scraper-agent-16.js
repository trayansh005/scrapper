// Romans scraper using Playwright with Crawlee
// Agent ID: 16
//
// Usage:
// node backend/scraper-agent-16.js [startPage]
// Example: node backend/scraper-agent-16.js 10 (starts from page 10)

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
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

// ============================================================================
// CONSTANTS
// ============================================================================

const AGENT_ID = 16;
const logger = createAgentLogger(AGENT_ID);
const START_PAGE = 1;

const USER_AGENTS = [
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
];

const TIMING = {
	PAGE_DELAY_MIN: 500,
	PAGE_DELAY_MAX: 1200,
	DETAIL_DELAY_MIN: 400,
	DETAIL_DELAY_MAX: 900,
	AFTER_GOTO: 2000,
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
	// {
	//   urlBase: "https://www.romans.co.uk/properties/for-sale",
	//   isRental: false,
	//   label: "SALES",
	//   totalRecords: 876,
	//   recordsPerPage: 8,
	// },
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

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function randBetween(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

function getRandomUserAgent() {
	return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

function getStartingPage(args) {
	const startPageArg = args.length > 0 ? parseInt(args[0]) : START_PAGE;
	return !isNaN(startPageArg) && startPageArg > 0 ? startPageArg : START_PAGE;
}

// ============================================================================
// PAGE SETUP HELPERS
// ============================================================================

async function setupPageHeaders(page) {
	try {
		await page.setUserAgent(getRandomUserAgent());
		await page.setExtraHTTPHeaders({
			"accept-language": "en-GB,en;q=0.9",
		});
	} catch (error) {
		// Silently fail - headers are nice-to-have
	}
}

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

async function navigateToPage(page, url) {
	await sleep(randBetween(TIMING.PAGE_DELAY_MIN, TIMING.PAGE_DELAY_MAX));

	const response = await page.goto(url, {
		waitUntil: "domcontentloaded",
		timeout: 30000,
	});

	if (response?.status?.() === 429) {
		console.warn(`⚠️ 429 on ${url} — backing off`);
		await sleep(TIMING.RATE_LIMIT_BACKOFF);
		throw new Error("429 Rate Limit");
	}

	await page.waitForTimeout(TIMING.AFTER_GOTO);
	return response;
}

// ============================================================================
// COORDINATE EXTRACTION
// ============================================================================

async function extractCoordinatesFromStreetview(page) {
	logger.step(`Trying Streetview button for coordinates`);

	try {
		const streetviewBtn = await page.locator(SELECTORS.STREETVIEW_BUTTON).first();
		const isVisible = await streetviewBtn.isVisible({ timeout: 5000 }).catch(() => false);

		if (!isVisible) {
			logger.page(0, "", `Streetview button not visible`);
			return { latitude: null, longitude: null };
		}

		await streetviewBtn.click();
		logger.page(0, "", `Clicked Streetview button, waiting for Google Maps`);
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
				logger.page(
					0,
					"",
					`Found coords from Streetview: ${googleMapsCoords.lat}, ${googleMapsCoords.lng}`,
				);
				return {
					latitude: googleMapsCoords.lat,
					longitude: googleMapsCoords.lng,
				};
			}

			if (retry < 4) {
				logger.page(0, "", `Retry ${retry + 1}/5 waiting for coords`);
				await page.waitForTimeout(TIMING.STREETVIEW_RETRY);
			}
		}

		logger.page(0, "", `No coords found after retries`);
		return { latitude: null, longitude: null };
	} catch (error) {
		logger.error(`Could not extract streetview coords`, error);
		return { latitude: null, longitude: null };
	}
}

async function extractCoordinates(htmlContent, detailPage) {
	// Try HTML extraction first (faster)
	const htmlCoords = await extractCoordinatesFromHTML(htmlContent);

	if (htmlCoords.latitude && htmlCoords.longitude) {
		logger.page(0, "", `Found coords in HTML: ${htmlCoords.latitude}, ${htmlCoords.longitude}`);
		return htmlCoords;
	}

	// Fall back to Streetview extraction
	return await extractCoordinatesFromStreetview(detailPage);
}

// ============================================================================
// PROPERTY PARSING
// ============================================================================

function parsePrice(priceText) {
	if (!priceText) return null;
	const digits = priceText.replace(/[^0-9]/g, "");
	return digits ? parseInt(digits) : null;
}

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

async function scrapePropertyDetail(browserContext, property) {
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		await setupPageHeaders(detailPage);
		await sleep(randBetween(TIMING.DETAIL_DELAY_MIN, TIMING.DETAIL_DELAY_MAX));

		const response = await navigateToPage(detailPage, property.link);
		await dismissCookieBanner(detailPage);

		// Get HTML content and extract coordinates
		const htmlContent = await detailPage.content();
		const coords = await extractCoordinates(htmlContent, detailPage);

		// Save property to database
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			property.isRental,
			htmlContent,
			coords.latitude,
			coords.longitude,
		);

		stats.totalScraped++;
		stats.totalSaved++;

		const coordsStr =
			coords.latitude && coords.longitude ? `${coords.latitude}, ${coords.longitude}` : "No coords";

		logger.property(
			0,
			"",
			property.title ? property.title.substring(0, 40) : "",
			formatPrice(property.price),
			property.link,
			property.isRental,
			0,
			"CREATED",
		);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData || {};

	logger.page(pageNum, label, request.url);

	await setupPageHeaders(page);
	await navigateToPage(page, request.url);

	// Wait for properties to load
	await page.waitForSelector(SELECTORS.PROPERTY_CARD, { timeout: 30000 }).catch(() => {
		logger.page(pageNum, label, `No properties found on page ${pageNum}`);
	});

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);
	logger.page(pageNum, label, `Found ${properties.length} properties`);

	// Process each property
	for (const property of properties) {
		// Update price in database (or insert minimal record if new)
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		// If new property, scrape full details immediately
		if (!result.isExisting && !result.error) {
			logger.property(
				pageNum,
				label,
				property.title.substring(0, 40),
				formatPrice(property.price),
				property.link,
				isRental,
				0,
				"CREATED",
			);
			await scrapePropertyDetail(page.context(), {
				...property,
				isRental,
			});
		}
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 2,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 120,
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(`❌ Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// PAGE QUEUEING
// ============================================================================

function generatePageRequests(propertyType, startPage) {
	const totalPages =
		propertyType.totalRecords && propertyType.recordsPerPage
			? Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage)
			: 1;

	logger.step(`Queueing ${propertyType.label} pages: ${totalPages}`);

	const validStartPage = startPage > 0 && startPage <= totalPages ? startPage : 1;

	logger.step(`Starting from page ${validStartPage} to ${totalPages}`);

	const requests = [];

	for (let page = validStartPage; page <= validStartPage; page++) {
		// Romans uses /page-N/ format
		const url = `${propertyType.urlBase}/page-${page}/`;
		const uniqueKey = `${propertyType.label}_page_${page}`;

		requests.push({
			url,
			uniqueKey,
			userData: {
				pageNum: page,
				isRental: propertyType.isRental,
				label: propertyType.label,
			},
		});
	}

	return requests;
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeRomans() {
	logger.step(`Starting Romans scraper...`);
	logMemoryUsage("START");

	await markAllPropertiesRemovedForAgent(AGENT_ID);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);
	const startPage = getStartingPage(process.argv.slice(2));

	// Process each property type
	for (const propertyType of PROPERTY_TYPES) {
		const requests = generatePageRequests(propertyType, startPage);

		await crawler.addRequests(requests);
		await crawler.run();

		logMemoryUsage(`After ${propertyType.label}`);
	}

	logger.step(
		`Completed Romans - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logMemoryUsage("END");
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeRomans();
		await updateRemoveStatus(AGENT_ID);
		logger.step("All done!");
		process.exit(0);
	} catch (error) {
		logger.error("Fatal error", error);
		process.exit(1);
	}
})();
