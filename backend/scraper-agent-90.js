// OpenRent scraper using direct API endpoints
// Agent ID: 90
// Website: openrent.co.uk

const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 90;
const logger = createAgentLogger(AGENT_ID);

const USER_AGENT =
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	totalCreated: 0,
	totalUpdated: 0,
	totalSkipped: 0,
	totalErrors: 0,
};

const BATCH_SIZE = 50;

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizePropertyUrl(rawUrl) {
	if (!rawUrl) return null;
	try {
		const url = new URL(rawUrl, "https://www.openrent.co.uk");
		return `${url.origin}${url.pathname}`.replace(/\/+$/, "");
	} catch (error) {
		return rawUrl.trim();
	}
}

async function scrapePropertyDetail(propertyUrl, property, isRental) {
	try {
		const res = await fetch(propertyUrl, {
			headers: { "User-Agent": USER_AGENT },
		});

		if (!res.ok) return { success: false };

		const detailHtml = await res.text();
		const $ = cheerio.load(detailHtml);

		const mapDiv = $("[data-lat][data-lng]").first();
		const lat = parseFloat(mapDiv.attr("data-lat")) || null;
		const lng = parseFloat(mapDiv.attr("data-lng")) || null;

		const headingTitle = $("h1, .property-title").first().text().trim();
		const finalTitle = headingTitle && headingTitle.length > 5 ? headingTitle : property.title;

		const result = await processPropertyWithCoordinates(
			propertyUrl,
			property.price,
			finalTitle,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			detailHtml,
			lat,
			lng,
		);

		return { success: true, coordsFound: !!(lat && lng), result };
	} catch (error) {
		logger.error(`Detail page error for ${propertyUrl}`, error);
		return { success: false };
	}
}

async function fetchMasterPropertyIds() {
	const url = "https://www.openrent.co.uk/properties-to-rent/greater-london?term=Greater%20London&isLive=true";
	const res = await fetch(url, {
		headers: { "User-Agent": USER_AGENT },
	});

	if (!res.ok) {
		throw new Error(`Master search request failed with status: ${res.status}`);
	}

	const html = await res.text();
	const match = html.match(/var\s+PROPERTYIDS\s*=\s*\[([\s\S]*?)\];/);

	if (!match) {
		throw new Error("Could not locate PROPERTYIDS array in OpenRent search page HTML.");
	}

	const rawIds = match[1];
	const propertyIds = rawIds
		.split(",")
		.map((id) => parseInt(id.trim()))
		.filter((id) => Number.isInteger(id) && id > 0);

	return propertyIds;
}

async function fetchPropertyBatch(idsBatch) {
	const query = idsBatch.map((id) => `ids=${id}`).join("&");
	const apiUrl = `https://www.openrent.co.uk/search/propertiesbyid?${query}`;

	const res = await fetch(apiUrl, {
		headers: {
			"User-Agent": USER_AGENT,
			"X-Requested-With": "XMLHttpRequest",
		},
	});

	if (!res.ok) {
		throw new Error(`Batch API call failed with status: ${res.status}`);
	}

	const data = await res.json();
	return Array.isArray(data) ? data : [];
}

function parseBedroomsFromDetails(details) {
	if (!Array.isArray(details)) return null;

	for (const detail of details) {
		const text = String(detail).trim();
		const match = text.match(/(\d+)\s*(?:bed|bedroom|bedrooms|room|rooms)/i);
		if (match) {
			return parseInt(match[1]);
		}
	}
	return null;
}

async function scrapeOpenRent() {
	logger.step(`Starting OpenRent Scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const isRental = true;
	const label = "RENTALS";

	logger.step("Fetching master property list from OpenRent...");
	const allPropertyIds = await fetchMasterPropertyIds();
	logger.step(`Found ${allPropertyIds.length} active property IDs!`);

	const totalBatches = Math.ceil(allPropertyIds.length / BATCH_SIZE);
	const effectiveStartBatch = Math.max(1, startPage);

	for (let b = effectiveStartBatch - 1; b < totalBatches; b++) {
		const pageNum = b + 1;
		const startIdx = b * BATCH_SIZE;
		const batchIds = allPropertyIds.slice(startIdx, startIdx + BATCH_SIZE);

		logger.page(pageNum, label, `Fetching batch of ${batchIds.length} properties via API...`, totalBatches);

		try {
			const propertiesData = await fetchPropertyBatch(batchIds);
			logger.page(pageNum, label, `Received ${propertiesData.length} property items`, totalBatches);

			if (propertiesData.length === 0) continue;

			for (const item of propertiesData) {
				const rawLink = `https://www.openrent.co.uk/${item.id}`;
				const propertyLink = normalizePropertyUrl(rawLink);

				if (!propertyLink) continue;

				if (item.letAgreed || isSoldProperty(item.description || "")) {
					counts.totalSkipped++;
					logger.property(
						pageNum,
						label,
						(item.title || "Property").substring(0, 30),
						"",
						propertyLink,
						isRental,
						totalBatches,
						"SKIPPED",
					);
					continue;
				}

				let price = item.rentPerMonth ? parseFloat(item.rentPerMonth) : null;
				if (!price && item.rentPerWeek) {
					price = parsePrice(item.rentPerWeek);
				}

				if (!price) {
					counts.totalSkipped++;
					continue;
				}

				const title = (item.title || "Property")
					.replace(/\s*[•-]\s*£[\d,]+.*$/gi, "")
					.replace(/£[\d,]+.*$/gi, "")
					.replace(/\s+/g, " ")
					.trim()
					.substring(0, 150);

				const bedrooms = parseBedroomsFromDetails(item.details);

				const propertyObj = {
					link: propertyLink,
					price,
					title,
					bedrooms,
				};

				const dbResult = await updatePriceByPropertyURLOptimized(
					propertyLink,
					price,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
				);

				if (dbResult?.error) {
					counts.totalErrors++;
					continue;
				}

				let propertyAction = "UNCHANGED";
				if (dbResult.updated) propertyAction = "UPDATED";

				if (dbResult?.isExisting && !dbResult.missingData) {
					counts.totalScraped++;
					if (dbResult.updated) counts.totalUpdated++;

					logger.property(
						pageNum,
						label,
						title.substring(0, 30),
						formatPriceDisplay(price, isRental),
						propertyLink,
						isRental,
						totalBatches,
						propertyAction,
					);
					continue;
				}

				// New property or missing data -> visit detail page
				if (!dbResult.isExisting) propertyAction = "CREATED";

				await scrapePropertyDetail(propertyLink, propertyObj, isRental);

				counts.totalScraped++;
				counts.totalSaved++;
				if (dbResult?.isExisting) counts.totalUpdated++;
				else counts.totalCreated++;

				logger.property(
					pageNum,
					label,
					title.substring(0, 30),
					formatPriceDisplay(price, isRental),
					propertyLink,
					isRental,
					totalBatches,
					propertyAction,
				);

				if (propertyAction !== "UNCHANGED") {
					await sleep(100); // Politeness delay
				}
			}
		} catch (error) {
			counts.totalErrors++;
			logger.error(`Error processing batch ${pageNum}`, error);
		}

		await sleep(200);
	}

	logger.step(
		`Finished OpenRent - Total scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Created: ${counts.totalCreated}, Updated: ${counts.totalUpdated}, Skipped: ${counts.totalSkipped}, Errors: ${counts.totalErrors}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

(async () => {
	try {
		await scrapeOpenRent();
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
