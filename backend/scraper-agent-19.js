// Snellers scraper using Playwright with Crawlee
// Agent ID: 19
// Usage:
// node backend/scraper-agent-19.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 19;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.snellers.co.uk/properties/sales/status-available",
	// 	totalPages: 14,
	// 	recordsPerPage: 12,
	// 	isRental: false,
	// 	label: "SALES",
	// },
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

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePrice(priceText) {
	const priceMatch = priceText.match(/[0-9][0-9,\s]*/g);
	if (!priceMatch) return null;

	const priceClean = priceMatch.join("").replace(/[^0-9]/g, "");
	return priceClean ? parseInt(priceClean) : null;
}

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
		const priceText = $card.find(".price .money").text().trim();
		const price = parsePrice(priceText);
		if (!price) return null;

		// Get bedrooms
		const bedrooms = $card.find(".bed-baths li:nth-child(1)").text().trim() || null;

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

	$(".property-card").each((index, element) => {
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
		const response = await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

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

		console.log(`✅ ${property.title} - ${formatPrice(property.price)} - ${coordsStr}`);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData || {};

	console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

	await page.goto(request.url, {
		waitUntil: "domcontentloaded",
		timeout: 30000,
	});

	// Wait for properties to load
	await page.waitForSelector(".property-card", { timeout: 30000 }).catch(() => {
		console.log(`⚠️ No properties found on page ${pageNum}`);
	});

	// Wait for dynamic content
	await page.waitForTimeout(1500);

	// Parse properties from listing page
	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

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
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(`❌ Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeSnellers() {
	console.log(`\n🚀 Starting Snellers scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	// Process each property type
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = pg === 1 ? `${propertyType.urlBase}` : `${propertyType.urlBase}/page-${pg}`;
			requests.push({
				url,
				userData: {
					pageNum: pg,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Snellers - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================(async () => {
	try {
		await scrapeSnellers();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
