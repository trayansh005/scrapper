// Chase Evans scraper using direct API extraction (Algolia via fetch)
// Agent ID: 37
//
// Usage:
// node backend/scraper-agent-37.js [startPage]

const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 37;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
};

const processedUrls = new Set();

const ALGOLIA_ENDPOINT =
	"https://ajxmbs3l60-2.algolianet.com/1/indexes/prod_properties/query?x-algolia-agent=Algolia%20for%20JavaScript%20(4.26.0)%3B%20Browser%20(lite)&x-algolia-api-key=c289da67dd593fa5f9d618502fa0cc9d&x-algolia-application-id=AJXMBS3L60";

const REQUEST_HEADERS = {
	"Content-Type": "application/json",
	Accept: "application/json",
	"User-Agent":
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
};

const PROPERTY_TYPES = [
	{
		label: "SALES",
		isRental: false,
		searchType: "sales",
		pathRoot: "property-for-sale",
	},
	{
		label: "RENTALS",
		isRental: true,
		searchType: "lettings",
		pathRoot: "property-to-rent",
	},
];

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
}

function toNumber(value) {
	const number = Number(value);
	return Number.isFinite(number) ? number : null;
}

function buildPropertyLink(hit, typeConfig) {
	const slug = (hit?.slug || "").toString().trim();
	const id = hit?.objectID ? hit.objectID.toString().trim() : null;
	if (!slug) return null;
	return `https://www.chaseevans.com/${typeConfig.pathRoot}/${slug}-${id}`;
}

function mapHitToProperty(hit, typeConfig) {
	const link = buildPropertyLink(hit, typeConfig);
	if (!link) return null;

	const price = toNumber(hit?.price);
	if (!price || price <= 0) return null;

	const title = (hit?.display_address || hit?.title || slugFromLink(link)).toString().trim();
	if (!title) return null;

	return {
		link,
		title,
		price,
		bedrooms: toNumber(hit?.bedroom),
		latitude: toNumber(hit?._geoloc?.lat),
		longitude: toNumber(hit?._geoloc?.lng),
	};
}

function slugFromLink(link) {
	try {
		const url = new URL(link);
		const parts = url.pathname.split("/").filter(Boolean);
		return parts[parts.length - 1] || "Property";
	} catch {
		return "Property";
	}
}

async function fetchListingPage(typeConfig, pageNum) {
	const algoliaPage = Math.max(0, pageNum - 1);
	const body = {
		query: "",
		page: algoliaPage,
		hitsPerPage: 24,
		filters: `department:residential AND search_type:${typeConfig.searchType} AND publish:true`,
	};

	const response = await fetch(ALGOLIA_ENDPOINT, {
		method: "POST",
		headers: REQUEST_HEADERS,
		body: JSON.stringify(body),
	});

	if (!response.ok) {
		throw new Error(`API request failed (${response.status})`);
	}

	const data = await response.json();
	const hits = Array.isArray(data?.hits) ? data.hits : [];
	const totalPages = Number.isFinite(data?.nbPages) ? Number(data.nbPages) : pageNum;

	return {
		hits,
		totalPages: Math.max(1, totalPages),
	};
}

async function processPage(typeConfig, pageNum, totalPages) {
	logger.page(pageNum, typeConfig.label, `Fetching API page ${pageNum}`, totalPages);

	const { hits } = await fetchListingPage(typeConfig, pageNum);
	logger.page(pageNum, typeConfig.label, `Found ${hits.length} properties`, totalPages);

	for (const hit of hits) {
		const property = mapHitToProperty(hit, typeConfig);
		if (!property) {
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

		if (action !== "UNCHANGED") {
			await sleep(120);
		}
	}
}

async function scrapeType(typeConfig, startPage) {
	const firstPage = await fetchListingPage(typeConfig, startPage);
	const totalPages = firstPage.totalPages;

	logger.page(startPage, typeConfig.label, `Found total pages: ${totalPages}`, totalPages);

	for (const hit of firstPage.hits) {
		// console.log(hit);
		const property = mapHitToProperty(hit, typeConfig);
		if (!property) {
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
		} else if (result.error) {
			action = "ERROR";
			counts.totalSkipped++;
		}

		logger.property(
			startPage,
			typeConfig.label,
			property.title.substring(0, 60),
			formatPriceDisplay(property.price, typeConfig.isRental),
			property.link,
			typeConfig.isRental,
			totalPages,
			action,
		);

		if (action !== "UNCHANGED") {
			await sleep(120);
		}
	}

	if (startPage < totalPages) {
		for (let pageNum = startPage + 1; pageNum <= totalPages; pageNum++) {
			await processPage(typeConfig, pageNum, totalPages);
		}
	}
}

async function scrapeChaseEvans() {
	logger.step(`Starting Chase Evans scraper (Agent ${AGENT_ID})...`);

	const startPage = getStartPage();
	const scrapeStartTime = new Date();
	const isPartialRun = startPage > 1;

	if (isPartialRun) {
		logger.step(
			`Partial run detected (startPage=${startPage}). Remove status update will be skipped.`,
		);
	}

	for (const typeConfig of PROPERTY_TYPES) {
		await scrapeType(typeConfig, startPage);
	}

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step("Skipping remove status update (Partial run)");
	}

	logger.step(
		`Completed Chase Evans - Found: ${counts.totalFound}, Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Skipped: ${counts.totalSkipped}`,
	);
}

scrapeChaseEvans()
	.then(() => {
		logger.step("All done!");
		process.exit(0);
	})
	.catch((error) => {
		logger.error("Fatal error", error);
		process.exit(1);
	});
