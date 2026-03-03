// Foxtons scraper using API-first extraction via Next.js JSON endpoints
// Agent ID: 50
//
// Usage:
// node backend/scraper-agent-50.js [startPage]

const { CheerioCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 50;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
};

const scrapeStartTime = new Date();

const PROPERTY_TYPES = [
	{
		discoveryUrl:
			"https://www.foxtons.co.uk/properties-for-sale/south-east-england?order_by=price_desc&radius=5&available_for_auction=0&sold=0",
		slug: "properties-for-sale/south-east-england",
		query: {
			order_by: "price_desc",
			radius: "5",
			available_for_auction: "0",
			sold: "0",
		},
		label: "SALES",
		isRental: false,
		includeForStartPage: () => true,
	},
	{
		discoveryUrl:
			"https://www.foxtons.co.uk/properties-to-rent/south-east-england?order_by=price_desc&expand=5&sold=0",
		slug: "properties-to-rent/south-east-england",
		query: {
			order_by: "price_desc",
			expand: "5",
			sold: "0",
		},
		label: "RENTALS",
		isRental: true,
		includeForStartPage: (startPage) => startPage === 1,
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

function buildApiUrl(typeConfig, buildId, pageNum) {
	const params = new URLSearchParams(typeConfig.query);
	params.set("page", String(pageNum));
	return `https://www.foxtons.co.uk/_next/data/${buildId}/${typeConfig.slug}.json?${params.toString()}`;
}

function parseJsonBody(body) {
	const bodyText = typeof body === "string" ? body : body?.toString?.("utf8") || "";
	if (!bodyText) return null;

	try {
		return JSON.parse(bodyText);
	} catch (error) {
		return null;
	}
}

async function fetchBuildId(discoveryUrl) {
	const response = await fetch(discoveryUrl, {
		headers: {
			"User-Agent":
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
			Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
			"Accept-Language": "en-GB,en;q=0.9",
		},
	});

	if (!response.ok) {
		throw new Error(`BuildId discovery failed (${response.status}) for ${discoveryUrl}`);
	}

	const html = await response.text();
	const match = html.match(/"buildId":"([^"]+)"/);
	if (!match?.[1]) {
		throw new Error(`Could not extract buildId from ${discoveryUrl}`);
	}

	return match[1];
}

function mapProperty(item, isRental) {
	const reference = (item?.propertyReference || "").trim();
	if (!reference) return null;

	const postcode = (item?.postcodeShort || "london").toString().toLowerCase();
	const pathRoot = isRental ? "properties-to-rent" : "properties-for-sale";
	const link = `https://www.foxtons.co.uk/${pathRoot}/${postcode}/${reference}`;

	const rawPrice = isRental
		? (item?.pricePcm ?? item?.priceFrom ?? item?.priceTo)
		: (item?.priceTo ?? item?.priceFrom);
	const price = Number(rawPrice);
	if (!Number.isFinite(price) || price <= 0) return null;

	const bedrooms = Number.isFinite(Number(item?.bedrooms)) ? Number(item.bedrooms) : null;
	const latitude = Number.isFinite(Number(item?.location?.lat)) ? Number(item.location.lat) : null;
	const longitude = Number.isFinite(Number(item?.location?.lon)) ? Number(item.location.lon) : null;

	const titleParts = [item?.streetName, item?.locationName, item?.postcodeShort].filter(Boolean);
	const title = titleParts.length > 0 ? titleParts.join(", ") : reference;

	return { link, title, price, bedrooms, latitude, longitude };
}

async function handleListingPage({ request, body, crawler }) {
	const { pageNum, label, isRental, startPage, totalPages, typeConfig, buildId } = request.userData;

	logger.page(pageNum, label, `Processing ${request.url}`, totalPages || null);

	const json = parseJsonBody(body);
	const data = json?.pageProps?.pageData?.data;
	const properties = Array.isArray(data?.data) ? data.data : [];

	if (!data || !Array.isArray(data.data)) {
		logger.error("Invalid listing API payload", null, pageNum, label);
		return;
	}

	const discoveredTotalPages = Math.max(pageNum, Number(data.totalPages) || pageNum);

	if (pageNum === startPage && discoveredTotalPages > pageNum) {
		const pendingRequests = [];
		for (let p = pageNum + 1; p <= discoveredTotalPages; p++) {
			pendingRequests.push({
				url: buildApiUrl(typeConfig, buildId, p),
				userData: {
					...request.userData,
					pageNum: p,
					totalPages: discoveredTotalPages,
				},
			});
		}

		if (pendingRequests.length > 0) {
			await crawler.addRequests(pendingRequests);
		}
	}

	logger.page(pageNum, label, `Found ${properties.length} properties`, discoveredTotalPages);

	for (const item of properties) {
		const property = mapProperty(item, isRental);
		if (!property) {
			counts.totalSkipped++;
			continue;
		}

		counts.totalFound++;

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
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
				isRental,
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
			label,
			property.title.substring(0, 60),
			formatPriceDisplay(property.price, isRental),
			property.link,
			isRental,
			totalPages || discoveredTotalPages,
			action,
		);

		if (action !== "UNCHANGED") {
			await sleep(100);
		}
	}
}

function createCrawler() {
	return new CheerioCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 180,
		additionalMimeTypes: ["application/json"],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			const { pageNum, label } = request.userData || {};
			logger.error(`Failed API page: ${request.url}`, null, pageNum, label);
		},
	});
}

async function scrapeFoxtons() {
	logger.step(`Starting Foxtons scraper (Agent ${AGENT_ID})...`);

	const startPage = getStartPage();
	const isPartialRun = startPage > 1;

	if (isPartialRun) {
		logger.step(
			`Partial run detected (startPage=${startPage}). Remove status update will be skipped.`,
		);
	}

	const initialRequests = [];
	for (const typeConfig of PROPERTY_TYPES) {
		if (!typeConfig.includeForStartPage(startPage)) continue;

		const buildId = await fetchBuildId(typeConfig.discoveryUrl);
		initialRequests.push({
			url: buildApiUrl(typeConfig, buildId, startPage),
			userData: {
				pageNum: startPage,
				startPage,
				totalPages: null,
				isRental: typeConfig.isRental,
				label: typeConfig.label,
				typeConfig,
				buildId,
			},
		});
	}

	if (initialRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}

	const crawler = createCrawler();
	await crawler.run(initialRequests);

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step("Skipping remove status update (Partial run)");
	}

	logger.step(
		`Completed Foxtons - Found: ${counts.totalFound}, Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Skipped: ${counts.totalSkipped}`,
	);
}

scrapeFoxtons()
	.then(() => {
		logger.step("All done!");
		process.exit(0);
	})
	.catch((error) => {
		logger.error("Fatal error", error);
		process.exit(1);
	});
