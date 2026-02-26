// Allsop scraper using Playwright with Crawlee
// Agent ID: 22
//
// Usage:
// node backend/scraper-agent-22.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 22;

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

	const text = priceText.toString();
	if (
		text.includes("Withdrawn") ||
		text.includes("Sold Prior") ||
		text.includes("Sold After") ||
		text.includes("Withdrawn Prior")
	) {
		return null;
	}

	// Remove currency symbols and non-numeric chars except decimal point
	const priceMatch = text.match(/[\d,.]+/);
	if (!priceMatch) return null;

	let priceClean = priceMatch[0].replace(/,/g, "");
	// Handle decimal points if any
	if (priceClean.includes(".")) {
		priceClean = Math.round(parseFloat(priceClean)).toString();
	}

	if (!priceClean || priceClean === "0") return null;

	// Return formatted as string with commas for UK style
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parseBedrooms(prop) {
	const text = `${prop.main_byline || ""} ${(prop.features || []).join(" ")}`.toLowerCase();

	// Check for studio
	if (text.includes("studio")) return "0";

	// Map word numbers
	const wordNumbers = {
		one: "1",
		two: "2",
		three: "3",
		four: "4",
		five: "5",
		six: "6",
		seven: "7",
		eight: "8",
		nine: "9",
		ten: "10",
	};

	const words = text.match(/\b(one|two|three|four|five|six|seven|eight|nine|ten)\b/);
	if (words) {
		return wordNumbers[words[0]];
	}

	const digits = text.match(/(\d+)\s*(?:bed|bedroom)/);
	if (digits) {
		return digits[1];
	}

	return null;
}

function generatePropertyURL(prop) {
	const mainByline = prop.main_byline || "";
	const town = prop.town || "";
	const slug = `${mainByline} in ${town}`
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/^-+|-+$/g, "");
	const ref = (prop.reference || "").toLowerCase().replace(/\s+/g, "-");
	return `https://www.allsop.co.uk/lot-overview/${slug}/${ref}`;
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

		// Try to click Street View to get coords if possible
		try {
			const streetViewTab = await detailPage.$('a[data-tab="street"]');
			if (streetViewTab) {
				await streetViewTab.click();
				await detailPage.waitForTimeout(2000);
			}
		} catch (e) {}

		// Get HTML content and extract coordinates
		const htmlContent = await detailPage.content();

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
	} catch (error) {
		console.error(`❌ Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function handleSearchPage({ page, request, browserController }) {
	const browserContext = browserController.browser.contexts()[0];
	const url = request.url;
	const isRental = url.includes("rental") || url.includes("letting");

	console.log(`🔍 Navigating to: ${url}`);
	await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });

	// Extract auction_id from URL
	const auctionIdMatch = url.match(/auction_id=([^&]+)/);
	const auctionId = auctionIdMatch ? auctionIdMatch[1] : null;

	if (!auctionId) {
		console.error("❌ Could not find auction_id in URL");
		return;
	}

	console.log(`📬 Fetching properties from API for auction: ${auctionId}...`);

	// Fetch all properties via API directly in the browser context
	const allProperties = await page.evaluate(async (aucId) => {
		let results = [];
		let currentPage = 1;
		let totalPages = 1;

		try {
			// Get first page to find total pages
			const firstResp = await fetch(
				`https://www.allsop.co.uk/api/search?auction_id=${aucId}&page=1&react`,
			);
			const firstData = await firstResp.json();
			if (firstData && firstData.results) {
				results = results.concat(firstData.results);
				totalPages = firstData.total_pages || 1;
			}

			// Fetch remaining pages
			for (let p = 2; p <= totalPages && p <= 40; p++) {
				const resp = await fetch(
					`https://www.allsop.co.uk/api/search?auction_id=${aucId}&page=${p}&react`,
				);
				const data = await resp.json();
				if (data && data.results) {
					results = results.concat(data.results);
				}
			}
		} catch (e) {
			console.error("API error:", e);
		}

		return results;
	}, auctionId);

	console.log(`✅ Found ${allProperties.length} properties via API`);

	for (const prop of allProperties) {
		const link = generatePropertyURL(prop);

		// If sold, use sale_price. Otherwise use sort_price (clean numeric) or guide_price
		let priceText = prop.sort_price || prop.sale_price || prop.guide_price;
		const price = parsePrice(priceText);

		if (!price) continue;

		// Extract bedrooms numeric
		const bedrooms = parseBedrooms(prop);

		const title =
			prop.allsop_address ||
			prop.full_address ||
			`LOT ${prop.lot_number || ""} - ${prop.town || ""}`;

		// Upsert minimal record
		const result = await updatePriceByPropertyURLOptimized(
			link,
			price,
			title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			stats.totalSaved++;
		}

		// If new property, scrape detail for coordinates
		if (!result.isExisting && !result.error) {
			await scrapePropertyDetail(browserContext, { link, price, title, bedrooms }, isRental);
		}
	}
}

async function scrapeAllsop() {
	console.log(`\n🚀 Starting Allsop scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1, // Stay nice to the API
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},
		requestHandler: handleSearchPage,
		requestHandlerTimeoutSecs: 600,
	});

	await crawler.run([
		"https://www.allsop.co.uk/property-search?auction_id=f76e435a-46a5-11f0-ba8f-0242ac110002&page=1",
	]);

	console.log(
		`\n✅ Completed Allsop - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeAllsop();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
