// Sotheby's International Realty UK property scraper
// Agent ID: 55
// Usage:
//   node backend/scraper-agent-55.js [startPage]
//
// Coordinate strategy: fetches all property listings (incl. latitude/longitude)
// from the Sotheby's GraphQL API — no browser/crawler needed.
// Architecture: 100% API-first using native Node.js fetch.
// Scrapes: SALES (~578 listings) + RENTALS (~160 listings)

const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 55;
const logger = createAgentLogger(AGENT_ID);

const GRAPHQL_ENDPOINT = "https://api.sothebysrealty.co.uk/api/graphql";

const PROPERTY_TYPES = [
	{
		label: "SALES",
		isRental: false,
		baseUrl: "https://sothebysrealty.co.uk/buy/property-for-sale",
		listingtype: "Sale",
	},
	{
		label: "RENTALS",
		isRental: true,
		baseUrl: "https://sothebysrealty.co.uk/rent/property-for-rent",
		listingtype: "Rent",
	},
];

const GQL_QUERY = `
query Listings($where: ListingWhereInput, $skip: Int, $take: Int, $orderBy: [ListingOrderByInput!]!) {
  listings(where: $where, skip: $skip, take: $take, orderBy: $orderBy) {
    id
    slug
    listingprice
    bedrooms
    latitude
    longitude
    propertytype
    community_propertyfinder
    sub_community_propertyfinder
    city_propertyfinder
    reference_number
    __typename
  }
  listingsCount(where: $where)
}
`.trim();

function buildWhere(listingtype) {
	return {
		OR: [],
		propertyStatus: { equals: "Exist" },
		status: { equals: "Published" },
		listingprice: {},
		totalarea: {},
		country_code: { equals: "UK" },
		listingtype: { contains: listingtype },
	};
}

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
	if (item.bedrooms && parseInt(item.bedrooms) > 0) {
		parts.push(`${parseInt(item.bedrooms)} bed`);
	}
	if (item.propertytype) {
		parts.push(item.propertytype);
	}
	const location = item.sub_community_propertyfinder || item.community_propertyfinder;
	if (location) parts.push(location);
	if (item.city_propertyfinder) parts.push(item.city_propertyfinder);
	return parts.join(", ") || `Property ${item.reference_number || item.id}`;
}

// ============================================================================
// GRAPHQL API PAGINATION
// ============================================================================

async function fetchAllListings(listingtype, label) {
	logger.step(`Fetching ${label} listings from Sotheby's GraphQL API...`);

	const allListings = [];
	const TAKE = 100;
	let skip = 0;
	let totalCount = null;
	const where = buildWhere(listingtype);

	do {
		const response = await fetch(GRAPHQL_ENDPOINT, {
			method: "POST",
			headers: {
				"Content-Type": "application/json",
				"User-Agent":
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
			},
			body: JSON.stringify({
				operationName: "Listings",
				query: GQL_QUERY,
				variables: {
					where,
					skip,
					take: TAKE,
					orderBy: [{ listingprice: "desc" }],
				},
			}),
		});

		if (!response.ok) {
			logger.step(`GraphQL fetch failed with status ${response.status} at skip=${skip} — stopping`);
			break;
		}

		const json = await response.json();

		if (json?.errors) {
			logger.step(`GraphQL errors at skip=${skip}: ${JSON.stringify(json.errors)}`);
			break;
		}

		const listings = json?.data?.listings || [];

		if (totalCount === null) {
			totalCount = json?.data?.listingsCount ?? 0;
			logger.step(`${label} — total reported by API: ${totalCount}`);
		}

		allListings.push(...listings);
		skip += listings.length;
		logger.step(`${label} — fetched ${allListings.length} / ${totalCount}`);

		if (listings.length < TAKE) break;

		await sleep(300);
	} while (allListings.length < (totalCount || Infinity));

	logger.step(`${label} — API fetch complete: ${allListings.length} listings`);
	return allListings;
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function processListings(listings, baseUrl, label, isRental) {
	for (const item of listings) {
		if (!item.slug) continue;

		const propertyUrl = `${baseUrl}/${item.slug}`;
		const price = item.listingprice ? parseFloat(item.listingprice) : null;
		const bedrooms = item.bedrooms ? parseInt(item.bedrooms) : null;
		const title = buildTitle(item);
		const latitude = item.latitude ? parseFloat(item.latitude) : null;
		const longitude = item.longitude ? parseFloat(item.longitude) : null;

		if (!price) {
			logger.page(1, label, `Skipping (no price): ${propertyUrl}`, 1);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			propertyUrl,
			price,
			title,
			bedrooms,
			AGENT_ID,
			isRental,
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
				isRental,
				null,
				latitude,
				longitude,
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
			propertyAction = "CREATED";
		} else if (result.error) {
			propertyAction = "ERROR";
		}

		logger.property(
			1,
			label,
			title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			propertyUrl,
			isRental,
			1,
			propertyAction,
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(500);
		}
	}
}

async function scrapeSothebys() {
	logger.step("Starting Sotheby's International Realty UK scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label}...`);
		const listings = await fetchAllListings(type.listingtype, type.label);
		await processListings(listings, type.baseUrl, type.label, type.isRental);
	}

	logger.step(
		`Completed Sotheby's - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
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
		await scrapeSothebys();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
