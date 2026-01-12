// Gatekeeper scraper using Playwright with Crawlee
// Agent ID: 234
// Website: gatekeeper.co.uk
// Usage:
// node backend/scraper-agent-234.js

const { PlaywrightCrawler, log } = require("crawlee");
const { launchOptions } = require("camoufox-js");
const { firefox } = require("playwright");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 234;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		url: "https://www.gatekeeper.co.uk/properties",
		isRental: false,
		label: "SALES",
		buttonSelector: "#buyBtn",
		toggleValue: "SaleProperty",
	},
	{
		url: "https://www.gatekeeper.co.uk/properties",
		isRental: true,
		label: "RENTALS",
		buttonSelector: "#rentBtn",
		toggleValue: "LettingProperty",
	},
];

async function scrapeGatekeeper() {
	console.log(`\n🚀 Starting Gatekeeper scraper (Agent ${AGENT_ID})...\n`);

	for (const propertyType of PROPERTY_TYPES) {
		await scrapePropertyType(propertyType);
	}

	console.log(`\n✅ Scraping complete!`);
	console.log(`Total scraped: ${totalScraped}`);
	console.log(`Total saved: ${totalSaved}\n`);

	process.exit(0);
}

async function scrapePropertyType(propertyType) {
	const { url, isRental, label, buttonSelector } = propertyType;

	console.log(`\n📋 Starting ${label} scrape...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: firefox,
			launchOptions: await launchOptions({
				headless: true,
			}),
		},

		browserPoolOptions: {
			useFingerprints: false,
		},

		async requestHandler({ page, request }) {
			console.log(`📍 Loading: ${url}`);

			// Navigate to the page
			await page.goto(url, { waitUntil: "networkidle" });

			// Wait for properties list to load
			await page.waitForSelector("#properties_list", { timeout: 30000 }).catch(() => {
				console.log(`⚠️ Properties list not found`);
			});

			// Click the appropriate button to show sales or rentals
			const button = await page.$(buttonSelector);
			if (button) {
				console.log(`🔘 Clicking ${label} button...`);
				await button.click();
				await page.waitForTimeout(2000); // Wait for properties to load after click
			}

			// Load all properties by clicking "View More Properties" button
			await loadAllProperties(page);

			// Extract properties from the page
			const properties = await extractPropertiesFromPage(page, isRental, page);

			console.log(`✅ Found ${properties.length} ${label.toLowerCase()}`);
			totalScraped += properties.length;

			// Save properties to database
			for (const property of properties) {
				try {
					const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;
					const priceNum = parseFloat(priceClean);

					await updatePriceByPropertyURL(
						property.url,
						priceClean,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
						property.latitude,
						property.longitude
					);

					totalSaved++;
					const priceDisplay = isNaN(priceNum) ? "N/A" : formatPrice(priceNum);
					console.log(`✅ ${property.title} - ${priceDisplay}`);
				} catch (err) {
					console.error(`❌ Error saving property: ${err.message}`);
				}
			}
		},
	});

	try {
		await crawler.run([{ url, userData: { isRental, label } }]);
	} catch (error) {
		console.error(`Error during crawling: ${error.message}`);
	}
}

(async () => {
	try {
		await scrapeGatekeeper();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
