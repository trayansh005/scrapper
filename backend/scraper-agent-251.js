// Springbok Properties XML Scraper
// Agent ID: 251
// Usage:
// node backend/scraper-agent-251.js

const axios = require("axios");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 251;
const logger = createAgentLogger(AGENT_ID);
const XML_FEED_URL = "https://www.xml2u.com/Xml/Springbok%20Estate%20Agency_997/7353_Default.xml";

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ===================================
// UTILITY FUNCTIONS
// ===================================

function formatPriceDisplay(price, isRental) {
	if (!price) return isRental ? "£0 pcm" : "£0";
	return isRental ? `£${price} pcm` : `£${price.toLocaleString()}`;
}

// ===================================
// SCRAPER LOGIC
// ===================================

async function scrapeSpringbok() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Springbok Properties XML scraper at ${scrapeStartTime.toISOString()}...`);

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

			const propertyId = prop.find("propertyid").text().trim();
			// Use dataSource as the link
			const link = prop.find("link > dataSource").text().trim();

			if (!link || !propertyId) continue;
			if (processedUrls.has(link)) continue;
			processedUrls.add(link);

			const title = prop.find("Description > title").text().trim() || "Property";
			const priceText = prop.find("Price > price").text().trim();
			const price = parseFloat(priceText) || null;
			const bedrooms = parseInt(prop.find("Description > bedrooms").text().trim()) || null;
			const latitude = parseFloat(prop.find("Address > latitude").text().trim()) || null;
			const longitude = parseFloat(prop.find("Address > longitude").text().trim()) || null;
			const category = prop.find("category").text().trim();

			const isRental =
				category.toLowerCase().includes("rent") || category.toLowerCase().includes("letting");

			// Skip if no price
			if (!price) {
				logger.error(`Skipping property due to missing price: ${propertyId}`);
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
					null, // detailPageHtml
					latitude,
					longitude,
				);

				stats.totalSaved++;
				stats.totalScraped++;
				if (isRental) stats.savedRentals++;
				else stats.savedSales++;
			}

			let propertyAction = "UNCHANGED";
			if (result.updated) propertyAction = "UPDATED";
			if (!result.isExisting && !result.error) propertyAction = "CREATED";

			logger.property(
				1,
				"XML_FEED",
				title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				link,
				isRental,
				0,
				propertyAction,
				latitude,
				longitude,
			);
		}

		logger.step(
			`Completed Springbok XML scrape. Found: ${properties.length}, Saved: ${stats.totalSaved}`,
		);
		return { scrapeStartTime };
	} catch (err) {
		logger.error(`Scrape failed: ${err.message}`);
		throw err;
	}
}

// ===================================
// MAIN EXECUTION
// ===================================

(async () => {
	try {
		const { scrapeStartTime } = await scrapeSpringbok();

		logger.step("Starting cleanup of stale properties...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("Cleanup finished successfully.");

		logger.step("Summary of Scraper Run:");
		logger.step(`- Total Collected: ${stats.totalScraped}`);
		logger.step(`- Total Saved (Inc updates): ${stats.totalSaved}`);
		logger.step(`- Sales Saved: ${stats.savedSales}`);
		logger.step(`- Rentals Saved: ${stats.savedRentals}`);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
