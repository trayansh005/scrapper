// Daniel Cobb property scraper using CheerioCrawler
// Agent ID: 66
// Usage:
//   node backend/scraper-agent-66.js [startPage]
//
// Coordinate strategy: extracted from JavaScript JSON embedded in detail page HTML
// as "lat": xx.xxx / "lng": xx.xxx (Google Maps init data).
// Architecture: CheerioCrawler — listing pages queue detail pages for new properties.

const { CheerioCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 66;
const logger = createAgentLogger(AGENT_ID);

const BASE_URL = "https://www.danielcobb.co.uk";

const PROPERTY_TYPES = [
	{
		label: "SALES",
		isRental: false,
		totalPages: 8,
		buildUrl: (page) =>
			`${BASE_URL}/property-sales/properties-available-for-sale-in-london/page-${page}`,
	},
	{
		label: "RENTALS",
		isRental: true,
		totalPages: 4,
		// Page 1 has no /page-N suffix; subsequent pages do
		buildUrl: (page) =>
			page === 1
				? `${BASE_URL}/property-lettings/properties-to-rent-in-london`
				: `${BASE_URL}/property-lettings/properties-to-rent-in-london/page-${page}`,
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// Pending detail work: keyed by detail URL, stores property metadata
const pendingDetails = new Map();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractCoords(html) {
	const latMatch = html.match(/"lat"\s*:\s*([0-9.-]+)/);
	const lngMatch = html.match(/"lng"\s*:\s*([0-9.-]+)/);
	return {
		latitude: latMatch ? parseFloat(latMatch[1]) : null,
		longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
	};
}

// ============================================================================
// REQUEST HANDLERS
// ============================================================================

async function handleListingPage({ $, request, crawler }) {
	const { pageNum, label, isRental, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	const detailRequests = [];

	$(".property").each((index, element) => {
		try {
			const $el = $(element);

			const href = $el.find("a").first().attr("href");
			if (!href) return;

			const link = href.startsWith("http") ? href : `${BASE_URL}${href}`;

			const title =
				$el.find(".form-control-static a").text().replace(/\s+/g, " ").trim() ||
				$el.find(".h3 a").text().replace(/\s+/g, " ").trim() ||
				"Property";

			const bedText = $el.find(".h3 a").text().trim();
			const bedMatch = bedText.match(/(\d+)/);
			const bedrooms = bedMatch ? parseInt(bedMatch[1]) : null;

			const priceRaw = $el.find(".price-container strong").text().trim();
			const statusText = $el.text() || "";

			if (isSoldProperty(statusText)) return;

			const price = parsePrice(priceRaw);
			if (!price) return;

			if (processedUrls.has(link)) return;

			detailRequests.push({ link, title, bedrooms, price, isRental, label, pageNum, totalPages });
		} catch (err) {
			logger.error("Error parsing listing card", err, pageNum, label);
		}
	});

	logger.page(pageNum, label, `Found ${detailRequests.length} properties`, totalPages);

	for (const prop of detailRequests) {
		if (processedUrls.has(prop.link)) continue;
		processedUrls.add(prop.link);

		const result = await updatePriceByPropertyURLOptimized(
			prop.link,
			prop.price,
			prop.title,
			prop.bedrooms,
			AGENT_ID,
			prop.isRental,
		);

		let propertyAction = "UNCHANGED";

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			// Queue detail page to fetch coordinates
			pendingDetails.set(prop.link, {
				price: prop.price,
				title: prop.title,
				bedrooms: prop.bedrooms,
				isRental: prop.isRental,
				label: prop.label,
				pageNum: prop.pageNum,
				totalPages: prop.totalPages,
			});

			await crawler.addRequests([
				{
					url: prop.link,
					userData: { handler: "DETAIL", propertyLink: prop.link },
				},
			]);

			propertyAction = "QUEUED";
		} else if (result.error) {
			propertyAction = "ERROR";
		}

		logger.property(
			prop.pageNum,
			prop.label,
			prop.title.substring(0, 40),
			formatPriceDisplay(prop.price, prop.isRental),
			prop.link,
			prop.isRental,
			prop.totalPages,
			propertyAction,
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(300);
		}
	}
}

async function handleDetailPage({ $, request }) {
	const { propertyLink } = request.userData;
	const meta = pendingDetails.get(propertyLink);
	if (!meta) return;
	pendingDetails.delete(propertyLink);

	const html = $.html();
	const { latitude, longitude } = extractCoords(html);

	await processPropertyWithCoordinates(
		propertyLink,
		meta.price,
		meta.title,
		meta.bedrooms,
		AGENT_ID,
		meta.isRental,
		null,
		latitude,
		longitude,
	);

	counts.totalSaved++;
	counts.totalScraped++;
	if (meta.isRental) counts.savedRentals++;
	else counts.savedSales++;

	logger.property(
		meta.pageNum,
		meta.label,
		meta.title.substring(0, 40),
		formatPriceDisplay(meta.price, meta.isRental),
		propertyLink,
		meta.isRental,
		meta.totalPages,
		"CREATED",
	);

	await sleep(500);
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler() {
	return new CheerioCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 60,
		requestHandlerTimeoutSecs: 180,
		async requestHandler(context) {
			const handler = context.request.userData?.handler;
			if (handler === "DETAIL") {
				await handleDetailPage(context);
			} else {
				await handleListingPage(context);
			}
		},
		failedRequestHandler({ request }) {
			const { handler, propertyLink } = request.userData || {};
			if (handler === "DETAIL" && propertyLink) pendingDetails.delete(propertyLink);
			logger.error(`Failed request: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeDanielCobb() {
	logger.step("Starting Daniel Cobb scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const crawler = createCrawler();

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} (${type.totalPages} pages)`);
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			allRequests.push({
				url: type.buildUrl(pg),
				userData: {
					handler: "LISTING",
					pageNum: pg,
					isRental: type.isRental,
					label: type.label,
					totalPages: type.totalPages,
				},
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	} else {
		logger.warn("No requests to process.");
	}

	logger.step(
		`Completed Daniel Cobb - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
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
		await scrapeDanielCobb();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
