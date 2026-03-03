// Douglas Allen property scraper using Starberry GraphQL API
// Agent ID: 47
// Usage:
//   node backend/scraper-agent-47.js [startPage]
//
// Coordinate strategy: fetches all property listings (incl. latitude/longitude)
// from the Starberry GraphQL API — no browser/detail page visits needed.
// Architecture: 100% API-first using native Node.js fetch.
// SALES: ~280 listings, RENTALS: ~4 listings (paginated, 100 per request)

const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 47;
const logger = createAgentLogger(AGENT_ID);

const GRAPHQL_ENDPOINT = "https://arunestates-feed.q.starberry.com/graphql";
const BASE_URL = "https://www.douglasallen.co.uk/property";
// Long-lived JWT hardcoded in the Douglas Allen site JS (exp 2034)
const BEARER_TOKEN =
	"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IjY3NjExNjc4MmU3OGY3MjcyOGJjZDk3ZiIsImlhdCI6MTczNDQxNjExMSwiZXhwIjoyMDQ5OTkyMTExfQ.DzgFnykDv4Sd1zFwi14x3ovMq1Q13dkMehNjQWfTsAE";

const PROPERTY_TYPES = [
	{
		label: "SALES",
		isRental: false,
		searchType: "sales",
		buildUrl: (crm_id) => `${BASE_URL}/for-sale/${crm_id}/`,
	},
	{
		label: "RENTALS",
		isRental: true,
		searchType: "lettings",
		buildUrl: (crm_id) => `${BASE_URL}/to-rent/${crm_id}/`,
	},
];

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

function buildTitle(item) {
	const parts = [];
	if (item.bedroom) {
		parts.push(`${item.bedroom} bed`);
	}
	if (item.building) {
		parts.push(item.building);
	}
	if (item.area) {
		parts.push(item.area);
	}
	if (item.display_address) {
		parts.push(item.display_address);
	}
	return parts.join(", ") || item.title || "Property";
}

function parsePrice(priceStr) {
	if (!priceStr) return null;
	const cleanPrice = priceStr.toString().replace(/[^\d]/g, "");
	const price = parseInt(cleanPrice);
	return price > 0 ? price : null;
}

// ============================================================================
// GRAPHQL API FETCH
// ============================================================================

async function fetchPropertiesFromAPI(searchType) {
	logger.step(`Fetching ${searchType} properties from Starberry GraphQL API (paginated)...`);

	const allProperties = [];
	let start = 0;
	const pageSize = 100;

	try {
		while (true) {
			const gqlQuery = `{
  properties(where: { brand_id: "da", publish: true, search_type: "${searchType}" }, start: ${start}, limit: ${pageSize}) {
    id crm_id title display_address address price price_qualifier search_type
    building bedroom bathroom reception latitude longitude description area status
  }
}`;

			const response = await fetch(GRAPHQL_ENDPOINT, {
				method: "POST",
				headers: {
					"Content-Type": "application/json",
					Origin: "https://www.douglasallen.co.uk",
					Referer: "https://www.douglasallen.co.uk/",
					Authorization: `Bearer ${BEARER_TOKEN}`,
					"User-Agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
				},
				body: JSON.stringify({ query: gqlQuery }),
			});

			if (!response.ok) {
				logger.step(`API fetch failed with status ${response.status} (start=${start})`);
				break;
			}

			const json = await response.json();

			if (json?.errors) {
				logger.step(`GraphQL errors: ${JSON.stringify(json.errors)}`);
				break;
			}

			const page = json?.data?.properties || [];
			if (!page.length) break;

			allProperties.push(...page);
			logger.step(
				`Fetched ${page.length} ${searchType} properties (start=${start}, total=${allProperties.length})`,
			);

			if (page.length < pageSize) break;
			start += pageSize;
		}

		return allProperties;
	} catch (err) {
		logger.step(`API fetch error: ${err.message}`);
		return allProperties;
	}
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function processProperties(properties, typeConfig) {
	for (const item of properties) {
		if (!item.crm_id) continue;

		// Build property URL and get market data
		const propertyUrl = typeConfig.buildUrl(item.crm_id);
		const price = parsePrice(item.price);
		const bedrooms = item.bedroom ? parseInt(item.bedroom) : null;
		const title = buildTitle(item);
		const latitude = item.latitude ? parseFloat(item.latitude) : null;
		const longitude = item.longitude ? parseFloat(item.longitude) : null;

		if (!price) {
			logger.page(1, typeConfig.label, `Skipping (no price): ${propertyUrl}`, 1);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			propertyUrl,
			price,
			title,
			bedrooms,
			AGENT_ID,
			typeConfig.isRental,
		);

		let propertyAction = "UNCHANGED";

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			// Coordinates come directly from GraphQL API — no detail page visit needed
			await processPropertyWithCoordinates(
				propertyUrl,
				price,
				title,
				bedrooms,
				AGENT_ID,
				typeConfig.isRental,
				null,
				latitude,
				longitude,
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (typeConfig.isRental) counts.savedRentals++;
			else counts.savedSales++;
			propertyAction = "CREATED";
		} else if (result.error) {
			propertyAction = "ERROR";
		}

		logger.property(
			1,
			typeConfig.label,
			title.substring(0, 40),
			formatPriceDisplay(price, typeConfig.isRental),
			propertyUrl,
			typeConfig.isRental,
			1,
			propertyAction,
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(300);
		}
	}
}

async function scrapeDouglasAllen() {
	logger.step("Starting Douglas Allen (Agent 47) scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label}...`);
		const properties = await fetchPropertiesFromAPI(type.searchType);
		await processProperties(properties, type);
	}

	logger.step(
		`Completed Douglas Allen - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, RENTALS: ${counts.savedRentals}`);

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
		await scrapeDouglasAllen();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
