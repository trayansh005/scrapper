// Nestseekers scraper using Playwright with Crawlee
// Agent ID: 241
//
// Usage:
// node backend/scraper-agent-241.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 241;

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

function parsePrice(priceText) {
	if (!priceText) return null;

	// Nestseekers sites often display multiple currencies; the first one is primary.
	// We extract the part before any parentheses to isolate the main price.
	const primaryText = priceText.split("(")[0].trim();

	// Extract the first price found with a currency symbol (£, €, or $)
	const match = primaryText.match(/[£€$]\s*([\d,]+)/) || priceText.match(/[£€$]\s*([\d,]+)/);
	if (!match) {
		// Fallback for numeric sequences (at least 4 digits)
		const fallbackMatch = priceText.match(/([\d,]{4,})/);
		if (fallbackMatch) {
			const priceClean = fallbackMatch[1].replace(/,/g, "");
			return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
		}
		if (priceText.toLowerCase().includes("request")) return "0";
		return null;
	}

	const priceClean = match[1].replace(/,/g, "");
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($, element) {
	try {
		const $row = $(element);

		// Link
		const linkEl = $row.find("a[href]").first();
		if (!linkEl.length) return null;

		let href = linkEl.attr("href");
		if (!href) return null;
		const link = href.startsWith("http") ? href : `https://www.nestseekers.com${href}`;

		// Title
		let title = $row.find("a strong").text().trim();
		const address = $row.find("h2").text().trim().replace(/\s+/g, " ");
		if (address) title += " - " + address;

		// Price
		let $priceEl = $row.find(".price");
		if (!$priceEl.length) {
			$priceEl = $row.find(".p-4.text-center").first();
		}
		const priceAttr = $priceEl.text().trim();
		const price = parsePrice(priceAttr);

		// Bedrooms
		let bedrooms = null;
		const infoText = $row.find(".info .tight").text();
		const bedroomMatch = infoText.match(/(\d+)\+?\s*(?:BR|bedroom)/i);
		if (bedroomMatch) bedrooms = bedroomMatch[1];

		if (link && title) {
			return { link, title, price, bedrooms };
		}
		return null;
	} catch (error) {
		return null;
	}
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const properties = [];

	$("tr[id]").each((index, element) => {
		const property = parsePropertyCard($, element);
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

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		const htmlContent = await detailPage.content();
		
		const coords = await detailPage.evaluate(() => {
			try {
				const geoEl = document.querySelector("[geo]");
				if (!geoEl) return null;
				const geoAttr = geoEl.getAttribute("geo");
				if (!geoAttr) return null;
				const geoData = JSON.parse(geoAttr);
				if (geoData && geoData.lat && geoData.lon) {
					return { lat: parseFloat(geoData.lat), lon: parseFloat(geoData.lon) };
				}
			} catch (e) {}
			return null;
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			coords ? coords.lat : null,
			coords ? coords.lon : null
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (error) {
		console.error(`❌ Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(`📋 [${label}] Page ${pageNum} - ${request.url}`);

	await page.waitForTimeout(2000);
	await page.waitForSelector("tr[id]", { timeout: 30000 }).catch(() => {});

	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

	for (const property of properties) {
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			stats.totalSaved++;
		}

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

async function scrapeNestseekers() {
	console.log(`\n🚀 Starting Nestseekers scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	
	const totalSalesPages = 15; 
	const totalRentalPages = 10; 

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Sales
	for (let p = Math.max(1, startPage); p <= totalSalesPages; p++) {
		allRequests.push({
			url: p === 1 ? "https://www.nestseekers.com/Sales/united-kingdom/" : `https://www.nestseekers.com/Sales/united-kingdom/?page=${p}`,
			userData: { pageNum: p, isRental: false, label: "SALES" },
		});
	}

	// Rentals
	if (startPage === 1) {
		for (let p = 1; p <= totalRentalPages; p++) {
			allRequests.push({
				url: p === 1 ? "https://www.nestseekers.com/Rentals/united-kingdom/" : `https://www.nestseekers.com/Rentals/united-kingdom/?page=${p}`,
				userData: { pageNum: p, isRental: true, label: "RENTALS" },
			});
		}
	}

	if (allRequests.length === 0) {
		console.log("⚠️ No pages to scrape.");
		return;
	}

	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(`\n✅ Finished - Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`);
}

(async () => {
	try {
		await scrapeNestseekers();
		await updateRemoveStatus(AGENT_ID);
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err);
		process.exit(1);
	}
})();
