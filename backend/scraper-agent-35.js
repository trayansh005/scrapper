// Guild Property scraper using Playwright with Crawlee
// Agent ID: 35
//
// Usage:
// node backend/scraper-agent-35.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 35;

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
	const match = priceText.match(/[£€]\s*([\d,]+)/);
	if (!match) return null;

	const priceClean = match[1].replace(/,/g, "");
	if (!priceClean) return null;

	// Return formated as string with commas
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($, element) {
	try {
		const $card = $(element);

		// Link - prefer h4.card-title a
		const linkEl =
			$card.find("h4.card-title a").length > 0
				? $card.find("h4.card-title a")
				: $card.find("a").first();

		let href = linkEl.attr("href");
		if (!href) return null;

		const link = href.startsWith("http") ? href : "https://www.guildproperty.co.uk" + href;

		// Title
		const titleEl = $card.find("h4.card-title a");
		const title = titleEl.text().trim() || "Unknown";

		// Price
		const priceEl = $card.find(".h4.m-0, .h4");
		const price = parsePrice(priceEl.text().trim());

		// Bedrooms
		let bedrooms = null;
		const pText = $card.find("p").text();
		const bedMatch = pText.match(/(\d+)\s*Bedroom/);
		if (bedMatch) bedrooms = bedMatch[1];

		if (link && title && price) {
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

	$(".panel.panel-default").each((index, element) => {
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

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(`📋 [${label}] Page ${pageNum} - ${request.url}`);

	// Wait for results to load
	await page.waitForTimeout(2000);
	await page.waitForSelector(".panel.panel-default", { timeout: 30000 }).catch(() => {});

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

		if (result.updated) {
			stats.totalSaved++;
		}

		// If new property, scrape full details immediately
		if (!result.isExisting && !result.error) {
			console.log(`🆕 Scraping detail for new property: ${property.title}`);
			await scrapePropertyDetail(page.context(), property, isRental);
		}
	}

	// Pagination - Guild Property uses page=N
	if (properties.length > 0 && pageNum < 5) {
		const currentUrl = new URL(request.url);
		currentUrl.searchParams.set("page", pageNum + 1);

		await crawler.addRequests([
			{
				url: currentUrl.toString(),
				userData: {
					pageNum: pageNum + 1,
					isRental,
					label,
				},
			},
		]);
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

async function scrapeGuildProperty() {
	console.log(`\n🚀 Starting GuildProperty scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(`🌐 Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [
		{
			url: "https://www.guildproperty.co.uk/search?page=1&national=false&p_department=RL&p_division=&location=London&searchRadius=50&availability=1&limit=20",
			userData: {
				pageNum: 1,
				isRental: true,
				label: "LETTINGS",
			},
		},
		{
			url: "https://www.guildproperty.co.uk/search?page=1&national=false&p_department=RS&p_division=&location=London&searchRadius=50&availability=1&limit=20",
			userData: {
				pageNum: 1,
				isRental: false,
				label: "SALES",
			},
		},
	];

	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n✅ Completed GuildProperty - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeGuildProperty();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
