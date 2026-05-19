// AST Lettings XML Scraper (Rightmove V3)
// Agent ID: 266

const axios = require("axios");
const cheerio = require("cheerio");

const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 266;
const logger = createAgentLogger(AGENT_ID);

const XML_FEED_URL =
	"https://astlettings.10ninety.co.uk/portalexports/RightmoveV3Xml/1000";

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// HELPERS
// ============================================================================

function formatPriceDisplay(price, isRental) {
	return isRental ? `£${price} pcm` : `£${price}`;
}

function getText(prop, selector) {
	return prop.find(selector).first().text().trim();
}

function extractFullAddress(prop) {
	const parts = [];

	const addr1 = getText(prop, "ADDRESS_1");
	const addr2 = getText(prop, "ADDRESS_2");
	const town = getText(prop, "TOWN");
	const postcode1 = getText(prop, "POSTCODE1");
	const postcode2 = getText(prop, "POSTCODE2");

	if (addr1) parts.push(addr1);
	if (addr2) parts.push(addr2);
	if (town) parts.push(town);
	if (postcode1) parts.push(postcode1);
	if (postcode2) parts.push(postcode2);

	return parts.join(", ");
}

// ============================================================================
// PROCESS FUNCTION
// ============================================================================

async function processProperty(data, index, total) {
	const { link, title, price, bedrooms, isRental, latitude, longitude } = data;

	if (!price || !link) return;

	const result = await updatePriceByPropertyURLOptimized(
		link,
		price,
		title,
		bedrooms,
		AGENT_ID,
		isRental
	);

	let action = "UNCHANGED";

	// Count all valid
	if (!result.error) {
		stats.totalScraped++;
		if (isRental) stats.savedRentals++;
		else stats.savedSales++;
	}

	if (result.updated) {
		stats.totalSaved++;
		action = "UPDATED";
	}

	if (!result.isExisting && !result.error) {
		await processPropertyWithCoordinates(
			link,
			price,
			title,
			bedrooms,
			AGENT_ID,
			isRental,
			null,
			latitude,
			longitude
		);

		stats.totalSaved++;
		action = "CREATED";
	}

	logger.property(
		index,
		"XML_FEED",
		title.substring(0, 40),
		formatPriceDisplay(price, isRental),
		link,
		isRental,
		total,
		action
	);
}

// ============================================================================
// MAIN SCRAPER
// ============================================================================

async function scrapeASTLettings() {
	const scrapeStartTime = new Date();

	logger.step(`Starting scraper at ${scrapeStartTime.toISOString()}`);

	try {
		const response = await axios.get(XML_FEED_URL);
		const $ = cheerio.load(response.data, { xmlMode: true });

		const properties = $("property");

		logger.step(`Found ${properties.length} properties`);

		const rentals = [];

		// ====================================================================
		// CLASSIFY PROPERTIES
		// ====================================================================

		for (let i = 0; i < properties.length; i++) {
			const prop = $(properties[i]);

			// Extract property reference
			const propRef =
				getText(prop, "PROPERTY_REF") ||
				getText(prop, "AGENT_REF") ||
				`prop-${i}`;

			// Build property URL from reference
			const link = `https://astlettings.10ninety.co.uk/property/${propRef}`;

			// Skip if already processed in this batch
			if (processedUrls.has(link)) continue;
			processedUrls.add(link);

			// Extract full address as title
			const fullAddress = extractFullAddress(prop);
			const headline = getText(prop, "HEADLINE") || fullAddress;
			const title = headline || `Property ${propRef}`;

			// Extract price (in pcm for rentals)
			const priceRaw = getText(prop, "PRICE");
			const price = parseFloat(priceRaw.replace(/[^\d.]/g, ""));

			if (!price) continue;

			// All properties in this feed are rentals (TRANS_TYPE_ID=2 indicates rental)
			const transTypeId = getText(prop, "TRANS_TYPE_ID");
			const isRental = true; // AST Lettings is rental-only

			// Extract bedrooms
			const bedrooms = parseFloat(getText(prop, "BEDROOMS")) || null;

			// Extract coordinates
			const latitude = parseFloat(getText(prop, "LATITUDE")) || null;
			const longitude = parseFloat(getText(prop, "LONGITUDE")) || null;

			// Check if published
			const publishedFlag = getText(prop, "PUBLISHED_FLAG");
			if (publishedFlag === "0") {
				logger.property(
					i + 1,
					"XML_FEED",
					title.substring(0, 40),
					formatPriceDisplay(price, isRental),
					link,
					isRental,
					properties.length,
					"SKIPPED" // Not published
				);
				continue;
			}

			const data = {
				link,
				title,
				price,
				bedrooms,
				isRental,
				latitude,
				longitude,
			};

			rentals.push(data);
		}

		// ====================================================================
		// PROCESS RENTALS
		// ====================================================================
		logger.step(`Processing RENTALS (${rentals.length})`);

		for (let i = 0; i < rentals.length; i++) {
			await processProperty(rentals[i], i + 1, rentals.length);
		}

		logger.step(
			`Done → Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved} (Rentals: ${stats.savedRentals})`
		);

		return { scrapeStartTime };
	} catch (err) {
		logger.error(`Error: ${err.message}`);
		throw err;
	}
}

// ============================================================================
// RUN
// ============================================================================

(async () => {
	try {
		const { scrapeStartTime } = await scrapeASTLettings();

		await updateRemoveStatus(AGENT_ID, scrapeStartTime);

		logger.step("Finished successfully");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal:", err);
		process.exit(1);
	}
})();
