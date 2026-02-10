// Pattinson scraper using Playwright with Crawlee
// Agent ID: 125
//
// Usage:
// node backend/scraper-agent-125.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 125;

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

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	await sleep(2000); // Respectful delay

	const detailPage = await browserContext.newPage();

	try {
		// Set a realistic user agent
		await detailPage.setExtraHTTPHeaders({
			"Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
		});

		// Navigate to detail page
		await detailPage.goto(property.url, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		const htmlContent = await detailPage.content();

		const coords = await detailPage.evaluate(() => {
			try {
				const scripts = document.querySelectorAll('script[type="application/ld+json"]');
				for (const script of scripts) {
					try {
						const data = JSON.parse(script.textContent);

						// Handle graph-style or direct objects
						const searchObj = data["@graph"] ? data["@graph"] : [data];

						for (const obj of searchObj) {
							if (obj["@type"] === "GeoCoordinates") {
								return { lat: parseFloat(obj.latitude), lon: parseFloat(obj.longitude) };
							}
							if (obj.geo && (obj.geo.latitude || obj.geo["@type"] === "GeoCoordinates")) {
								return {
									lat: parseFloat(obj.geo.latitude),
									lon: parseFloat(obj.geo.longitude),
								};
							}
						}
					} catch (e) {}
				}
			} catch (e) {}
			return null;
		});

		await processPropertyWithCoordinates(
			property.url,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			htmlContent,
			coords ? coords.lat : null,
			coords ? coords.lon : null,
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (error) {
		console.error(`❌ Error scraping detail page ${property.url}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(`📋 [${label}] Page ${pageNum} - ${request.url}`);

	// Wait for property cards
	try {
		await page.waitForSelector("a.row.m-0.bg-white", { timeout: 30000 });
	} catch (e) {
		// Check for Cloudflare
		const content = await page.content();
		if (content.includes("Verifying") || content.includes("Just a moment")) {
			console.log("❌ Stuck on Cloudflare. Retrying after delay...");
			await sleep(10000);
			await page.reload();
			await page.waitForSelector("a.row.m-0.bg-white", { timeout: 30000 }).catch(() => null);
		}
	}

	const htmlContent = await page.content();
	const $ = cheerio.load(htmlContent);

	const cards = $("a.row.m-0.bg-white");
	console.log(`🔗 Found ${cards.length} properties on page ${pageNum}`);

	for (let i = 0; i < cards.length; i++) {
		const $card = $(cards[i]);

		// Skip sold
		if (isSoldProperty($card.text())) {
			console.log(`⏩ Skipping sold/under-offer property`);
			continue;
		}

		const href = $card.attr("href");
		const link = href
			? href.startsWith("http")
				? href
				: `https://www.pattinson.co.uk${href}`
			: null;

		const titleEl = $card.find("div.text-primary-dark.fw-medium");
		const title = titleEl.length ? titleEl.text().trim() : "Pattinson Property";

		const priceEl = $card.find("dt.display-5.text-primary");
		const priceText = priceEl.length ? priceEl.text().trim() : "";
		const price = parsePrice(priceText);

		// Bedrooms
		const specs = $card.find("div.d-flex.align-items-center");
		let bedrooms = null;
		if (specs.length > 0) {
			const bedroomEl = specs.first().find("span.lh-1.fs-14");
			if (bedroomEl.length) {
				bedrooms = bedroomEl.text().trim();
			}
		}

		if (link && price > 0) {
			const result = await updatePriceByPropertyURLOptimized(
				link,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			if (!result.isExisting || result.updated) {
				await scrapePropertyDetail(page.context(), { url: link, price, title, bedrooms }, isRental);
			}
		}
	}

	// Pagination
	const maxPages = isRental ? 15 : 100;
	if (pageNum < maxPages && cards.length > 0) {
		const nextP = pageNum + 1;
		const nextUrl = new URL(request.url);
		nextUrl.searchParams.set("p", nextP.toString());

		await crawler.addRequests([
			{
				url: nextUrl.toString(),
				userData: { pageNum: nextP, isRental, label },
			},
		]);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function run() {
	console.log(`🚀 Starting Pattinson Refactored (Agent ${AGENT_ID})...`);

	const crawler = new PlaywrightCrawler({
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		maxConcurrency: 1, // Single concurrency to stay under Cloudflare radar
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(`❌ Failed request: ${request.url}`);
		},
	});

	const initialRequests = [
		{
			url: "https://www.pattinson.co.uk/buy/property-search",
			userData: { pageNum: 1, isRental: false, label: "SALES" },
		},
		{
			url: "https://www.pattinson.co.uk/rent/property-search",
			userData: { pageNum: 1, isRental: true, label: "RENTALS" },
		},
	];

	await crawler.run(initialRequests);
	await updateRemoveStatus(AGENT_ID);

	console.log(`\n✅ Completed Agent ${AGENT_ID}`);
	console.log(`Total Scraped: ${stats.totalScraped}`);
	console.log(`Total Saved: ${stats.totalSaved}`);
}

if (require.main === module) {
	run().catch((err) => {
		console.error(`❌ Fatal Error:`, err.message);
		process.exit(1);
	});
}

module.exports = { run };
