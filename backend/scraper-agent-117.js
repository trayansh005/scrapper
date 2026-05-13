// Parkers Properties scraper using CheerioCrawler
// Agent ID: 117
//
// Usage:
// node backend/scraper-agent-117.js

const { CheerioCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
	extractBedroomsFromHTML,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Reduce logging noise
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 117;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// REQUEST HANDLERS
// ============================================================================

/**
 * Handle listing pages to extract property URLs and basic info
 */
async function handleListingPage({ $, request, crawler }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	// Find property items
	// The analysis showed .property__meta contains the details
	const properties = $(".property__meta");

	if (properties.length === 0) {
		logger.warn(`No properties found on page ${pageNum} (${label})`);
		return;
	}

	for (const element of properties.toArray()) {
		const $el = $(element);

		const titleLink = $el.find(".property-title--search a");
		const fullUrl = titleLink.attr("href");

		if (!fullUrl) continue;

		if (processedUrls.has(fullUrl)) continue;
		processedUrls.add(fullUrl);

		const title = titleLink.text().replace(/\s+/g, " ").trim();
		const priceText = $el.find(".property-price--search").text().trim();
		const typeText = $el.find(".property-type--search").text().trim();

		if (isSoldProperty(priceText) || isSoldProperty(typeText)) {
			logger.property(pageNum, label, title.substring(0, 40), priceText, fullUrl, isRental, totalPages, "SKIPPED");
			continue;
		}

		const price = parsePrice(priceText);
		const bedrooms = extractBedroomsFromHTML(typeText);

		if (!price) {
			logger.property(pageNum, label, title.substring(0, 40), "NO PRICE", fullUrl, isRental, totalPages, "SKIPPED");
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			fullUrl,
			price,
			title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "UNCHANGED";

		if (result && result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (result && !result.isExisting && !result.error) {
			// New property, need coordinates from detail page
			await crawler.addRequests([
				{
					url: fullUrl,
					userData: {
						label: "DETAIL",
						pageNum,
						isRental,
						price,
						title,
						bedrooms,
						totalPages,
					},
				},
			]);
			propertyAction = "CREATED";
		} else if (result && result.isExisting && result.updated) {
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
		} else if (!result || result.error) {
			propertyAction = "ERROR";
		}

		logger.property(
			pageNum,
			label,
			title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			fullUrl,
			isRental,
			totalPages,
			propertyAction,
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(100);
		}
	}
}

/**
 * Handle detail pages to extract coordinates
 */
async function handleDetailPage({ $, request }) {
	const { pageNum, isRental, price, title, bedrooms, totalPages } = request.userData;

	let latitude = null;
	let longitude = null;

	try {
		// Parkers uses application/ld+json for coordinates
		const schemaJson = $("script.tpj-schema-graph").html();
		if (schemaJson) {
			const data = JSON.parse(schemaJson);
			const graph = data["@graph"];
			const listing = Array.isArray(graph) ? graph.find(item => item["@type"] === "RealEstateListing") : graph;

			if (listing && listing.contentLocation && listing.contentLocation.geo) {
				latitude = parseFloat(listing.contentLocation.geo.latitude);
				longitude = parseFloat(listing.contentLocation.geo.longitude);
			}
		}
	} catch (e) {
		logger.error(`Error parsing schema JSON on ${request.url}`, e);
	}

	const html = $.html();
	await processPropertyWithCoordinates(
		request.url,
		price,
		title,
		bedrooms,
		AGENT_ID,
		isRental,
		html,
		latitude,
		longitude,
	);

	counts.totalScraped++;
	if (isRental) counts.savedRentals++;
	else counts.savedSales++;

	logger.property(
		pageNum,
		"DETAIL",
		title.substring(0, 40),
		formatPriceDisplay(price, isRental),
		request.url,
		isRental,
		totalPages,
		"CREATED",
		latitude,
		longitude,
	);
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler() {
	return new CheerioCrawler({
		maxConcurrency: 2,
		maxRequestRetries: 2,
		requestHandler: async (context) => {
			const { request } = context;
			if (request.userData.label === "DETAIL") {
				return handleDetailPage(context);
			}
			return handleListingPage(context);
		},
		preNavigationHooks: [
			async ({ request }) => {
				await sleep(500);
				request.headers = {
					...request.headers,
					"User-Agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
					Accept:
						"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
					"Accept-Language": "en-GB,en;q=0.9",
				};
			},
		],
		failedRequestHandler({ request }) {
			logger.error(`Failed request: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeParkers() {
	logger.step("Starting Parkers scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const crawler = createCrawler();

	// Configuration for sales and lettings
	const PROPERTY_TYPES = [
		{
			baseUrl: "https://www.parkersproperties.co.uk/search-results/for-sale/in-south-england/",
			isRental: false,
			label: "SALES",
		},
		{
			baseUrl: "https://www.parkersproperties.co.uk/search-results/for-letting/in-south-england/",
			isRental: true,
			label: "LETTINGS",
		},
	];

	const allInitialRequests = [];

	for (const type of PROPERTY_TYPES) {
		// We'll visit the first page to find the total results if possible, 
		// but since we know it's around 500, we can just start and paginate.
		// Let's stick to the base rule of queueing multiple pages.

		// For Parkers, the URL with orderby and department:
		const searchParams = "?orderby=price_desc&department=residential";

		allInitialRequests.push({
			url: `${type.baseUrl}${searchParams}`,
			userData: {
				pageNum: 1,
				isRental: type.isRental,
				label: type.label,
				totalPages: null, // Will update or estimate
			},
		});

		// We'll handle pagination by finding the "Next" button in handleListingPage
		// or pre-queueing if we know the total.
		// Since we saw 493 results and 9 per page -> ~55 pages.
		const estimatedPages = 60;
		for (let pg = 2; pg <= estimatedPages; pg++) {
			if (isPartialRun && pg < startPage) continue;

			allInitialRequests.push({
				url: `${type.baseUrl}page-${pg}/${searchParams}`,
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: type.label,
					totalPages: estimatedPages,
				},
			});
		}
	}

	if (allInitialRequests.length > 0) {
		await crawler.run(allInitialRequests);
	} else {
		logger.warn("No requests to process.");
	}

	logger.step(
		`Completed Parkers - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New lettings: ${counts.savedRentals}`,
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

scrapeParkers()
	.then(() => {
		logger.step("All done!");
		process.exit(0);
	})
	.catch((error) => {
		logger.error("Unhandled scraper error", error);
		process.exit(1);
	});
