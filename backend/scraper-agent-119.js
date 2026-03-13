// Purple Frog XML Scraper
// Agent ID: 119
// Usage:
// node backend/scraper-agent-119.js

const axios = require("axios");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 119;
const logger = createAgentLogger(AGENT_ID);
const XML_FEED_URL = "https://www.xml2u.com/Xml/Purple%20Frog%20Property_1878/3543_Default.xml";

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function formatPriceDisplay(price) {
	if (!price) return "£0 pcm";
	return `£${price} pcm`;
}

// ============================================================================
// SCRAPER LOGIC
// ============================================================================

async function scrapePurpleFrog() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Purple Frog XML scraper at ${scrapeStartTime.toISOString()}...`);

	try {
		logger.step(`Fetching XML feed from ${XML_FEED_URL}...`);
		const response = await axios.get(XML_FEED_URL, {
			headers: {
				Accept: "application/xml, text/xml, */*",
				"User-Agent":
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
			},
		});

		if (!response.data) {
			throw new Error("Empty response from XML feed");
		}

		logger.step("Parsing XML content...");
		const $ = cheerio.load(response.data, { xmlMode: true });
		const properties = $("Property");

		logger.step(`Found ${properties.length} properties in XML feed.`);

		for (let i = 0; i < properties.length; i++) {
			const prop = $(properties[i]);

			const reference = prop.find("reference").text().trim();
			const propertyId = prop.find("propertyid").text().trim();

			// Construct a stable URL using the reference
			// Since we don't have the full slug, we'll use a search-friendly URL or the property ID
			// Most Purple Frog URLs end with the reference ID
			const link = `https://www.purplefrogproperty.com/accommodation/search/${reference}`;

			if (processedUrls.has(link)) continue;
			processedUrls.add(link);

			const title = prop.find("Description > title").text().trim() || "Property";
			const price = parseFloat(prop.find("Price > price").text().trim()) || null;
			const bedrooms = parseInt(prop.find("Description > bedrooms").text().trim()) || null;
			const latitude = parseFloat(prop.find("Address > latitude").text().trim()) || null;
			const longitude = parseFloat(prop.find("Address > longitude").text().trim()) || null;
			const category = prop.find("category").text().trim();
			const status = prop.find("Price > status").text().trim();

			// Filter out non-rentals or already let properties if necessary
			// But usually, we process all and let the database handle updates
			const isRental =
				category.toLowerCase().includes("rent") || category.toLowerCase().includes("lettings");

			// Skip if no price or reference
			if (!price || !reference) {
				logger.error(`Skipping property due to missing price or reference: ${propertyId}`);
				continue;
			}

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

			if (!result.isExisting && !result.error) {
				await processPropertyWithCoordinates(
					link,
					price,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					null, // detailPageHtml (not needed as we have coordinates)
					latitude,
					longitude,
				);

				stats.totalSaved++;
				stats.totalScraped++;
				if (isRental) stats.savedRentals++;
			}

			let propertyAction = "UNCHANGED";
			if (result.updated) propertyAction = "UPDATED";
			if (!result.isExisting && !result.error) propertyAction = "CREATED";

			logger.property(
				1,
				"XML_FEED",
				title.substring(0, 40),
				formatPriceDisplay(price),
				link,
				isRental,
				0,
				propertyAction,
			);
		}

		logger.step(
			`Completed Purple Frog XML scrape. Found: ${properties.length}, Saved: ${stats.totalSaved}`,
		);
		return { scrapeStartTime };
	} catch (err) {
		logger.error(`Scrape failed: ${err.message}`);
		throw err;
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		const { scrapeStartTime } = await scrapePurpleFrog();

		logger.step("Starting cleanup of stale properties...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("Cleanup finished successfully.");

		logger.step("Summary of Scraper Run:");
		logger.step(`- Total Collected: ${stats.totalScraped}`);
		logger.step(`- Total Saved to DB: ${stats.totalSaved}`);
		logger.step(`- Rentals Saved: ${stats.savedRentals}`);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
