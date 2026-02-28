// Winkworth scraper using native fetch (API-only)
// Agent ID: 36
//
// Usage:
// node backend/scraper-agent-36.js [startPage]

const { updateRemoveStatus } = require("./db.js");
const {
	processPropertyWithCoordinates,
	updatePriceByPropertyURLOptimized,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 36;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// CONFIGURATION
// ============================================================================

const PROPERTY_TYPES = [
	// {
	// 	channel: "Sales",
	// 	channelId: "7f45d0b8-2d58-4403-a338-2f99b676254f",
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		channel: "Lettings",
		channelId: "582f4a53-fa70-4ee2-b45b-7b2b41545a0a",
		isRental: true,
		label: "LETTINGS",
	},
];

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function fetchPage(channelEnum, channelId, pageNum) {
	// locationId 5739439 is "London"
	const url = `https://www.winkworth.co.uk/PropertiesSearch/FindOnMap?channelEnum=${channelEnum}&channel=${channelId}&location=London%2CLondon&locationId=5739439&isSearchPageSearch=true&priceFromName=No+Min&priceToName=No+Max&bedroomsFromName=No+Min&bedroomsToName=No+Max&page=${pageNum}`;

	try {
		const response = await fetch(url, {
			headers: {
				accept: "application/json, text/javascript, */*; q=0.01",
				"x-requested-with": "XMLHttpRequest",
			},
		});

		if (!response.ok) {
			logger.error(
				`Failed to fetch ${channel} page ${pageNum}: ${response.status} ${response.statusText}`,
			);
			return null;
		}

		return await response.json();
	} catch (error) {
		logger.error(`Error fetching ${channel} page ${pageNum}:`, error);
		return null;
	}
}

async function scrapeWinkworth() {
	logger.step(`Starting Winkworth API scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	for (const typeInfo of PROPERTY_TYPES) {
		logger.step(`Processing ${typeInfo.label}...`);

		let currentPage = startPage;
		let resultsOnPreviousPage = -1;

		while (true) {
			const properties = await fetchPage(typeInfo.channel, typeInfo.channelId, currentPage);

			if (!properties || !Array.isArray(properties) || properties.length === 0) {
				logger.warn(`No results found on page ${currentPage} for ${typeInfo.label}, stopping.`);
				break;
			}

			// Detection for looping or end of results
			if (properties.length === resultsOnPreviousPage) {
				// Sometimes sites show the last results repeated or just stick at the end
				// But Winkworth usually returns 20 per page.
			}
			resultsOnPreviousPage = properties.length;

			logger.page(currentPage, typeInfo.label, `Found ${properties.length} properties via API`);

			for (const prop of properties) {
				const relativeLink = prop.propertyUrl;
				if (!relativeLink) continue;

				const link = relativeLink.startsWith("http")
					? relativeLink
					: `https://www.winkworth.co.uk${relativeLink}`;

				// Basic skipping
				if (isSoldProperty(prop.shortDescription || "")) continue;

				// Price extraction
				const priceText = prop.price || prop.formattedPrice || "";
				const price = parsePrice(priceText.toString());

				if (!price) continue;

				const title = prop.address1 || "Property";
				const bedrooms = prop.bedrooms ? parseInt(prop.bedrooms, 10) : null;

				const lat = prop.latitude ? parseFloat(prop.latitude) : null;
				const lon = prop.longitude ? parseFloat(prop.longitude) : null;

				// Check if property exists first
				const result = await updatePriceByPropertyURLOptimized(
					link,
					price,
					title,
					bedrooms,
					AGENT_ID,
					typeInfo.isRental,
				);

				if (result.updated) {
					counts.totalSaved++;
					counts.totalScraped++;
					if (typeInfo.isRental) counts.savedRentals++;
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
						typeInfo.isRental,
						null, // HTML config not needed
						lat,
						lon,
					);
					counts.totalSaved++;
					counts.totalScraped++;
					if (typeInfo.isRental) counts.savedRentals++;
					else counts.savedSales++;
				}

				logger.property(
					currentPage,
					typeInfo.label,
					title.substring(0, 40),
					formatPriceDisplay(price, typeInfo.isRental),
					link,
					typeInfo.isRental,
					"?", // Total pages unknown from map API
					propertyAction,
				);
			}

			currentPage++;
			await sleep(1000); // Politeness delay
		}
	}

	logger.step(
		`Completed Winkworth - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
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
		await scrapeWinkworth();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
