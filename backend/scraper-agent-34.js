// Strutt & Parker scraper using native fetch (API-only)
// Agent ID: 34
//
// Usage:
// node backend/scraper-agent-34.js [startPage]

const { updateRemoveStatus } = require("./db.js");
const {
	processPropertyWithCoordinates,
	updatePriceByPropertyURLOptimized,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 34;
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

const LOCATIONS = ["London"]; // Can be expanded

const PROPERTY_TYPES = [
	{
		type: "for-sale",
		isRental: false,
		label: "SALES",
	},
	{
		type: "to-rent",
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

async function fetchPage(location, searchType, pageNum) {
	const url = `https://www.struttandparker.com/properties/residential/${searchType}/london/search?r[list_page]=${pageNum}&r[sr]=${searchType}&r[loc]=${location}&r[sort_by]=property_price_min--desc&sold=on`;

	try {
		const response = await fetch(url, {
			headers: {
				accept: "application/json, text/javascript, */*; q=0.01",
				"x-requested-with": "XMLHttpRequest",
			},
		});

		if (!response.ok) {
			logger.error(
				`Failed to fetch page ${pageNum} for ${searchType} in ${location}: ${response.status} ${response.statusText}`,
			);
			return null;
		}

		return await response.json();
	} catch (error) {
		logger.error(`Error fetching page ${pageNum} for ${searchType} in ${location}:`, error);
		return null;
	}
}

async function scrapeStruttAndParker() {
	logger.step(`Starting Strutt & Parker API scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	for (const location of LOCATIONS) {
		for (const typeInfo of PROPERTY_TYPES) {
			logger.step(`Processing ${typeInfo.label} for ${location}...`);

			let currentPage = startPage;
			let totalPages = startPage;

			while (currentPage <= totalPages) {
				const data = await fetchPage(location, typeInfo.type, currentPage);

				if (!data || !data.message || !data.message.results || !data.message.results.matches) {
					logger.warn(`No results found on page ${currentPage} for ${typeInfo.label}, stopping.`);
					break;
				}

				// The API returns an empty array when pages are exhausted
				if (data.message.results.matches.length === 0) {
					break;
				}

				// Attempt to extract total pages from pagination data if available
				// If not available, we rely on the empty array break condition above
				if (currentPage === startPage && data.message.pagination) {
					// Extract max page number from pagination HTML string if present
					const maxPageMatch = data.message.pagination.match(/r\[list_page\]=(\d+)["']/g);
					if (maxPageMatch) {
						let maxFound = 1;
						for (const match of maxPageMatch) {
							const num = parseInt(match.replace(/\D/g, ""));
							if (num > maxFound) maxFound = num;
						}
						totalPages = Math.max(startPage, maxFound);
						logger.step(`Total pages estimated: ${totalPages}`);
					} else {
						// Fallback: assume at least current page + 1 to keep loop going
						totalPages = currentPage + 1;
					}
				} else if (totalPages === currentPage) {
					// Fallback: Keep increasing total pages as long as we get results
					totalPages++;
				}

				const properties = data.message.results.matches;
				logger.page(
					currentPage,
					typeInfo.label,
					`Found ${properties.length} properties via API`,
					totalPages,
				);

				for (const prop of properties) {
					const link = prop.url;

					// If sold, we skip it
					if (isSoldProperty(prop.status || "")) continue;

					// Priority: numeric min price, then display string
					const priceText = prop.property_price_min || prop.property_price_display || "";
					const price = parsePrice(priceText.toString());

					if (!price) continue;

					const title = prop.property_address || prop.name || "Property";

					// Beds extracted from explicit API fields
					const bedrooms = prop.property_bedrooms ? parseInt(prop.property_bedrooms, 10) : null;

					const lat = prop.location?.lat ? parseFloat(prop.location.lat) : null;
					const lon = prop.location?.lon ? parseFloat(prop.location.lon) : null;

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
						totalPages,
						propertyAction,
					);
				}

				currentPage++;
				await sleep(1000); // Politeness delay
			}
		}
	}

	logger.step(
		`Completed Strutt & Parker - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
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
		await scrapeStruttAndParker();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
