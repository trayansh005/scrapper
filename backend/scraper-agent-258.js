// Sharpes Estates XML Scraper
// Agent ID: 258

const axios = require("axios");
const cheerio = require("cheerio");

const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 258;
const logger = createAgentLogger(AGENT_ID);

const XML_FEED_URL_SALE = "https://sharpes.apex27.co.uk/exports/2040/homingin";

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

	const displayAddress = getText(prop, "DisplayAddress");
	const addr1 = getText(prop, "Address1");
	const addr2 = getText(prop, "Address2");
	const city = getText(prop, "City");
	const county = getText(prop, "County");
	const postalCode = getText(prop, "PostalCode");

	if (displayAddress) parts.push(displayAddress);
	if (addr1) parts.push(addr1);
	if (addr2) parts.push(addr2);
	if (city) parts.push(city);
	if (county) parts.push(county);
	if (postalCode) parts.push(postalCode);

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

async function scrapeSharpesEstates() {
	const scrapeStartTime = new Date();

	logger.step(`Starting scraper at ${scrapeStartTime.toISOString()}`);

	try {
		const response = await axios.get(XML_FEED_URL_SALE, {
			headers: {
				'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
			}
		});
		const $ = cheerio.load(response.data, { xmlMode: true });

		const listings = $("Listing");
		logger.step(`Found ${listings.length} listings`);

		const availableProperties = [];

		// ====================================================================
		// CLASSIFY PROPERTIES - Only Available status
		// ====================================================================

		for (let i = 0; i < listings.length; i++) {
			const listing = $(listings[i]);

			// Filter Available only
			const status = getText(listing, "Status");
			if (status !== "Available") {
				continue;
			}

			// Extract property reference (ID)
			const propId = getText(listing, "ID");
			if (!propId) continue;

			// Build property URL
			const link = `https://sharpes.apex27.co.uk/property/${propId}`;

			// Skip if already processed in this batch
			if (processedUrls.has(link)) continue;
			processedUrls.add(link);

			// Extract title from address fields
			const fullAddress = extractFullAddress(listing);
			const summary = getText(listing, "Summary");
			const title = summary || fullAddress || `Property ${propId}`;

			// Extract price
			const priceRaw = getText(listing, "Price");
			const price = parseFloat(priceRaw.replace(/[^\d.]/g, ""));
			if (!price || isNaN(price)) continue;

			// Sale properties (isRental = false)
			const isRental = getText(listing, "TransactionType") === "rent";

			// Extract bedrooms
			const bedroomsRaw = getText(listing, "Bedrooms");
			const bedrooms = parseInt(bedroomsRaw) || null;

			// Extract coordinates
			const latitude = parseFloat(getText(listing, "Latitude")) || null;
			const longitude = parseFloat(getText(listing, "Longitude")) || null;

			const data = {
				link,
				title,
				price,
				bedrooms,
				isRental,
				latitude,
				longitude,
			};

			availableProperties.push(data);
		}

		logger.step(`Processing AVAILABLE properties (${availableProperties.length})`);

		// ====================================================================
		// PROCESS PROPERTIES
		// ====================================================================
		for (let i = 0; i < availableProperties.length; i++) {
			await processProperty(availableProperties[i], i + 1, availableProperties.length);
		}

		logger.step(
			`Done → Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved} (${stats.savedSales} Sales, ${stats.savedRentals} Rentals)`
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
		const { scrapeStartTime } = await scrapeSharpesEstates();

		await updateRemoveStatus(AGENT_ID, scrapeStartTime);

		logger.step("Finished successfully");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal:", err);
		process.exit(1);
	}
})();

