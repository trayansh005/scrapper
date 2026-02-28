// Allsop scraper using native fetch (API-only)
// Agent ID: 22
//
// Usage:
// node backend/scraper-agent-22.js [startPage]

const { updateRemoveStatus } = require("./db.js");
const {
	processPropertyWithCoordinates,
	updatePriceByPropertyURLOptimized,
} = require("./lib/db-helpers.js");
const {
	parsePrice,
	formatPriceDisplay,
	isSoldProperty,
	extractBedroomsFromHTML,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 22;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
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
// MAIN SCRAPER LOGIC
// ============================================================================

async function fetchPage(pageNum) {
	const url = `https://www.allsop.co.uk/api/search?page=${pageNum}&available_only=true&react`;
	logger.step(`Fetching API page ${pageNum}: ${url}`);

	try {
		const response = await fetch(url);
		if (!response.ok) {
			logger.error(`Failed to fetch page ${pageNum}: ${response.status} ${response.statusText}`);
			return null;
		}
		const data = await response.json();
		return data;
	} catch (error) {
		logger.error(`Error fetching page ${pageNum}:`, error);
		return null;
	}
}

async function scrapeAllsop() {
	logger.step(`Starting Allsop API scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	let currentPage = startPage;
	let totalPages = 140; // Will be updated on first request

	while (currentPage <= totalPages) {
		const data = await fetchPage(currentPage);

		if (!data || !data.data || !data.data.results) {
			logger.warn(`No results found on page ${currentPage}, stopping.`);
			break;
		}

		// Update total pages on first successful request
		if (currentPage === startPage && data.data.total_pages) {
			totalPages = data.data.total_pages;
			logger.step(`Total pages available: ${totalPages}`);
		}

		const properties = data.data.results;
		logger.page(
			currentPage,
			"AUCTION",
			`Found ${properties.length} properties via API`,
			totalPages,
		);

		for (const prop of properties) {
			const link = generatePropertyURL(prop);

			// If sold, use sale_price. Otherwise use sort_price (clean numeric) or guide_price
			let priceText = prop.sort_price || prop.sale_price || prop.guide_price;
			if (isSoldProperty(priceText ? priceText.toString() : "")) continue;
			const price = parsePrice(priceText ? priceText.toString() : null);

			if (!price) continue;

			// Extract bedrooms directly from the stringified JSON payload,
			// mimicking what HTML scraping achieves
			const bedrooms = extractBedroomsFromHTML(JSON.stringify(prop));

			const title =
				prop.allsop_address ||
				prop.full_address ||
				`LOT ${prop.lot_number || ""} - ${prop.town || ""}`;

			const lat = prop.location?.lat || null;
			const lon = prop.location?.lon || null;

			// Determine if rental (Allsop is overwhelmingly sales/auctions, but check for safety)
			const isRental =
				prop.department === "LETTINGS" ||
				(prop.type && prop.type.toLowerCase().includes("lettings"));

			// Check if property exists first
			const result = await updatePriceByPropertyURLOptimized(
				link,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			if (result.updated) {
				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (result.isExisting) {
				counts.totalScraped++;
			}

			let propertyAction = "UNCHANGED";
			if (result.updated) propertyAction = "UPDATED";

			if (!result.isExisting && !result.error) {
				propertyAction = "CREATED";
				// Insert new property with coordinates
				await processPropertyWithCoordinates(
					link,
					price,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					null, // HTML content not needed/available
					lat,
					lon,
				);
				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			}

			logger.property(
				currentPage,
				"AUCTION",
				title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				link,
				isRental,
				totalPages,
				propertyAction,
			);
		}

		currentPage++;
		await sleep(1000); // Politeness delay between API calls
	}

	logger.step(
		`Completed Allsop - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeAllsop();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
