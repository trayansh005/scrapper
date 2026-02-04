// Moveli scraper using Playlist with Crawlee
// Agent ID: 18
//
// Usage:
// node backend/scraper-agent-18.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 18;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

const SELECTORS = {
	PROPERTY_CARD: "a.property_card",
	PROPERTY_HEADING: "h4",
	PROPERTY_PRICE: "p.format_price",
	STATUS_LABEL: "p.status_label",
	BEDROOMS: "p.inline_text",
};

const PROPERTY_TYPES = [
	{
		urlBase:
			"https://www.moveli.co.uk/test/properties?category=for-sale&searchKeywords=&status=For%20Sale&maxPrice=any&minBeds=any&sortOrder=price-desc",
		isRental: false,
		label: "SALES",
	},
	{
		urlBase:
			"https://www.moveli.co.uk/test/properties?category=for-rent&searchKeywords=&status=For%20Rent&maxPrice=any&minBeds=any&sortOrder=price-desc",
		isRental: true,
		label: "LETTINGS",
	},
];

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function randBetween(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

function formatPriceUK(price) {
	if (!price) return "£0";
	return "£" + price.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePrice(priceText) {
	const priceMatch = priceText.match(/[0-9][0-9,\s]*/g);
	if (!priceMatch) return null;

	const priceClean = priceMatch.join("").replace(/[^0-9]/g, "");
	return priceClean ? parseInt(priceClean) : null;
}

function parseBedrooms(cardText) {
	const bedroomsMatch = cardText.match(/(\d+)\s*beds?/i);
	return bedroomsMatch ? bedroomsMatch[1] : null;
}

function parsePropertyCard($card) {
	try {
		// Get link
		let href = $card.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.moveli.co.uk" + href;

		// Get title from h4
		const title = $card.find(SELECTORS.PROPERTY_HEADING).text().trim();
		if (!title) return null;

		// Get status - only include AVAILABLE properties
		const status = $card.find(SELECTORS.STATUS_LABEL).text().trim().toUpperCase();
		if (status !== "AVAILABLE") {
			return null;
		}

		// Get price
		const priceText = $card.find(SELECTORS.PROPERTY_PRICE).text().trim();
		const price = parsePrice(priceText);
		if (!price) return null;

		// Get bedrooms - find the number before "beds"
		let bedrooms = null;
		$card.find(SELECTORS.BEDROOMS).each((i, el) => {
			const text = $(el).text().trim();
			if (/^\d+$/.test(text)) {
				bedrooms = text;
			}
		});

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
// BROWSERLESS SETUP
// ============================================================================

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
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		// Block unnecessary resources
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		const response = await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		// Wait for dynamic content to load
		await detailPage.waitForTimeout(1500);

		// Get HTML content and extract coordinates
		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);

		// Save property to database
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
		);

		stats.totalScraped++;
		stats.totalSaved++;

		const coordsStr =
			coords.latitude && coords.longitude ? `${coords.latitude}, ${coords.longitude}` : "No coords";

		console.log(`✅ ${property.title} - ${formatPriceUK(property.price)} - ${coordsStr}`);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label } = request.userData || {};

	console.log(`📋 ${label} - ${request.url}`);

	await page.goto(request.url, {
		waitUntil: "domcontentloaded",
		timeout: 30000,
	});

	// Wait for properties to load
	await page.waitForSelector(SELECTORS.PROPERTY_CARD, { timeout: 30000 }).catch(() => {
		console.log(`⚠️ No properties found`);
	});

	// Wait for dynamic content (React/Vue to render)
	await page.waitForTimeout(3000);

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	console.log(`🔗 Found ${properties.length} properties`);

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
			console.log(`🆕 Scraping detail for new property: ${property.title}`);
			await scrapePropertyDetail(page.context(), property, isRental);
		}
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				// Block unnecessary resources for listing pages
				await page.route("**/*", (route) => {
					const resourceType = route.request().resourceType();
					if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
						route.abort();
					} else {
						route.continue();
					}
				});
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(`❌ Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeMoveli() {
	console.log(`\n🚀 Starting Moveli scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	// Process each property type
	for (const propertyType of PROPERTY_TYPES) {
		const requests = [
			{
				url: propertyType.urlBase,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			},
		];

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Moveli - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeMoveli();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (error) {
		console.error("❌ Fatal error:", error?.message || error);
		process.exit(1);
	}
})();
