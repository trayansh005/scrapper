// The Agency UK scraper using direct Propertystream API
// Agent ID: 111
// Website: theagencyuk.com
// Usage:
// node backend/scraper-agent-111.js [startPage]

const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 111;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
};

const processedUrls = new Set();

// ============================================================================
// CONSTANTS
// ============================================================================

const API_BASE =
	"https://theagencyuk.api.propertystream.co/wp-json/wp/v2/property/?radius=3&per_page=12&new_homes=&include_stc=&address_keyword=&view=list&maximum_price=999999999999&minimum_price=0&hydrated=true";

const REQUEST_HEADERS = {
	Accept: "application/json",
	"User-Agent":
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
};

const PROPERTY_TYPES = [
	{
		label: "SALES",
		isRental: false,
		department: "residential-sales",
	},
	{
		label: "LETTINGS",
		isRental: true,
		department: "residential-lettings",
	},
];

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
}

function toNumber(value) {
	const n = Number(value);
	return Number.isFinite(n) ? n : null;
}

// ============================================================================
// API FETCHING
// ============================================================================

async function fetchPage(department, pageNum) {
	const url = `${API_BASE}&department=${department}&page=${pageNum}`;
	const response = await fetch(url, { headers: REQUEST_HEADERS });

	if (!response.ok) {
		throw new Error(`API ${response.status} for ${department} page ${pageNum}`);
	}

	const totalPages = parseInt(response.headers.get("x-wp-totalpages") || "1", 10);
	const properties = await response.json();

	return {
		properties: Array.isArray(properties) ? properties : [],
		totalPages: Math.max(1, totalPages),
	};
}

// ============================================================================
// PROPERTY MAPPING
// ============================================================================

function mapProperty(raw) {
	// Property link points to the canonical public site URL (not the API subdomain)
	// e.g. https://theagencyuk.api.propertystream.co/property/slug/ → https://theagencyuk.com/property/slug/
	const rawLink = (raw.link || "").toString().trim();
	if (!rawLink) return null;

	const link = rawLink.replace(
		"https://theagencyuk.api.propertystream.co/",
		"https://theagencyuk.com/",
	);

	const price = toNumber(raw.price_actual ?? raw.price);
	if (!price || price <= 0) return null;

	const titleRaw = raw.title?.rendered || raw.address_street || "Property";
	// WP HTML-encodes the title, decode the common entities
	const title = titleRaw
		.replace(/&#8211;/g, "–")
		.replace(/&amp;/g, "&")
		.trim();

	const statusText = raw.availability || "";

	return {
		link,
		title,
		price,
		bedrooms: toNumber(raw.bedrooms),
		latitude: toNumber(raw.latitude),
		longitude: toNumber(raw.longitude),
		statusText,
	};
}

// ============================================================================
// PAGE PROCESSING
// ============================================================================

async function processPage(typeConfig, pageNum, totalPages, properties) {
	logger.page(
		pageNum,
		typeConfig.label,
		`Processing ${properties.length} properties from API`,
		totalPages,
	);

	for (const raw of properties) {
		const property = mapProperty(raw);

		if (!property) {
			counts.totalSkipped++;
			continue;
		}

		if (isSoldProperty(property.statusText)) {
			logger.page(
				pageNum,
				typeConfig.label,
				`Skipped: Sold/Let Agreed (${property.link})`,
				totalPages,
			);
			counts.totalSkipped++;
			continue;
		}

		if (processedUrls.has(property.link)) {
			continue;
		}
		processedUrls.add(property.link);

		counts.totalFound++;

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			typeConfig.isRental,
		);

		let action = "UNCHANGED";

		if (result.updated) {
			action = "UPDATED";
			counts.totalSaved++;
			counts.totalScraped++;
		}

		if (!result.isExisting && !result.error) {
			// Coordinates come from the API — no detail page needed
			await processPropertyWithCoordinates(
				property.link,
				property.price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				typeConfig.isRental,
				null,
				property.latitude,
				property.longitude,
			);

			action = "CREATED";
			counts.totalSaved++;
			counts.totalScraped++;
			if (typeConfig.isRental) counts.savedRentals++;
			else counts.savedSales++;
		} else if (result.error) {
			action = "ERROR";
			counts.totalSkipped++;
		}

		logger.property(
			pageNum,
			typeConfig.label,
			property.title.substring(0, 60),
			formatPriceDisplay(property.price, typeConfig.isRental),
			property.link,
			typeConfig.isRental,
			totalPages,
			action,
		);

		// Only pause on writes; skip delay for UNCHANGED to speed up known-good records
		if (action !== "UNCHANGED") {
			await sleep(120);
		}
	}
}

// ============================================================================
// SCRAPE TYPE
// ============================================================================

async function scrapeType(typeConfig, startPage) {
	// Fetch first page to learn total pages
	const first = await fetchPage(typeConfig.department, startPage);
	const totalPages = first.totalPages;

	logger.page(startPage, typeConfig.label, `Total pages: ${totalPages}`, totalPages);

	await processPage(typeConfig, startPage, totalPages, first.properties);

	for (let pageNum = startPage + 1; pageNum <= totalPages; pageNum++) {
		logger.page(pageNum, typeConfig.label, `Fetching page ${pageNum}`, totalPages);
		const { properties } = await fetchPage(typeConfig.department, pageNum);
		await processPage(typeConfig, pageNum, totalPages, properties);
	}
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeTheAgencyUK() {
	const startPage = getStartPage();
	const scrapeStartTime = new Date();
	const isPartialRun = startPage > 1;

	logger.step(`Starting The Agency UK scraper (Agent ${AGENT_ID})...`);

	if (isPartialRun) {
		logger.step(
			`Partial run detected (startPage=${startPage}). Remove status update will be skipped.`,
		);
	}

	for (const typeConfig of PROPERTY_TYPES) {
		await scrapeType(typeConfig, startPage);
	}

	logger.step(
		`Completed The Agency UK — Found: ${counts.totalFound}, Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Skipped: ${counts.totalSkipped}`,
	);

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step("Skipping remove status update (partial run).");
	}
}

scrapeTheAgencyUK()
	.then(() => {
		logger.step("All done!");
		process.exit(0);
	})
	.catch((err) => {
		logger.error("Fatal error", err);
		process.exit(1);
	});
