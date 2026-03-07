// Carter Jonas scraper using Playwright with Crawlee
// Agent ID: 113
//
// Usage:
// node backend/scraper-agent-113.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 113;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildApiRequestBody({ isRental, pageNum }) {
	return {
		searchValues: {
			division: "Homes",
			toBuy: !isRental,
			area: "GreaterLondon",
			location: { lat: 0, lng: 0 },
			searchTerm: "",
			radius: 10,
			includeSoldOrSoldSTC: true,
			includeLetAgreedOrUnderOffer: true,
		},
		page: Math.max(0, pageNum - 1), // API is zero-based; agent paging is one-based
		pageSize: 12,
		sortOrder: "HighestPriceFirst",
	};
}

async function fetchPropertiesFromApi(page, { isRental, pageNum }) {
	const requestBody = buildApiRequestBody({ isRental, pageNum });

	const apiResult = await page.evaluate(async (body) => {
		const response = await fetch("/api/properties", {
			method: "POST",
			headers: { "content-type": "text/plain;charset=UTF-8" },
			body: JSON.stringify(body),
			credentials: "include",
		});

		if (!response.ok) {
			return { ok: false, status: response.status, properties: [] };
		}

		const data = await response.json();
		return {
			ok: true,
			status: response.status,
			properties: Array.isArray(data?.properties) ? data.properties : [],
		};
	}, requestBody);

	if (!apiResult?.ok) {
		throw new Error(`API request failed with status ${apiResult?.status || "unknown"}`);
	}

	return apiResult.properties.map((item) => {
		const link = item?.url
			? item.url.startsWith("http")
				? item.url
				: `https://www.carterjonas.co.uk${item.url}`
			: null;

		return {
			link,
			title: item?.name || "Property",
			price: typeof item?.price === "number" ? item.price : parsePrice(String(item?.price || "")),
			bedrooms: typeof item?.beds === "number" ? item.beds : null,
			statusText: item?.status || "",
			lat: typeof item?.location?.lat === "number" ? item.location.lat : null,
			lng: typeof item?.location?.lng === "number" ? item.location.lng : null,
		};
	});
}

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	// Handle cookie dismissal if present
	const cookieButton = page.getByRole("button", { name: "Accept All Cookies" });
	if (await cookieButton.isVisible()) {
		await cookieButton.click();
		await page.waitForTimeout(1000);
	}

	await page.waitForTimeout(2000);

	const properties = await fetchPropertiesFromApi(page, { isRental, pageNum });

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		try {
			const price = property.price;
			if (!price || price === 0) continue;

			if (!property.link) continue;
			if (isSoldProperty(property.statusText || "")) continue;

			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			const result = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";
			if (result.updated) {
				stats.totalSaved++;
				propertyAction = "UPDATED";
			}

			let lat = null;
			let lng = null;
			let finalPrice = price;
			let finalTitle = property.title;
			let finalBedrooms = property.bedrooms;

			if (!result.isExisting && !result.error) {
				lat = property.lat;
				lng = property.lng;

				await processPropertyWithCoordinates(
					property.link.trim(),
					finalPrice,
					finalTitle,
					finalBedrooms,
					AGENT_ID,
					isRental,
					null, // html
					lat,
					lng,
				);

				stats.totalSaved++;
				propertyAction = "CREATED";
			}

			logger.property(
				pageNum,
				label,
				finalTitle,
				formatPriceDisplay(finalPrice, isRental),
				property.link,
				isRental,
				totalPages,
				propertyAction,
				lat,
				lng,
			);

			if (propertyAction === "CREATED") {
				await sleep(500);
			}
		} catch (err) {
			logger.error(
				`Error processing property ${property.link}: ${err.message}`,
				err,
				pageNum,
				label,
			);
		}
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 600,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeCarterJonas() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Carter Jonas scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const maxPages = 50;

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Queue SALES pages
	for (let pg = Math.max(1, startPage); pg <= maxPages; pg++) {
		allRequests.push({
			url: `https://www.carterjonas.co.uk/property-search?division=Homes&area=GreaterLondon&toBuy=true&sortOrder=HighestPriceFirst&page=${pg}`,
			userData: { pageNum: pg, isRental: false, label: "SALES", totalPages: maxPages },
		});
	}

	// Queue RENTALS pages
	if (startPage === 1) {
		for (let pg = 1; pg <= maxPages; pg++) {
			allRequests.push({
				url: `https://www.carterjonas.co.uk/property-search?division=Homes&area=GreaterLondon&toBuy=false&sortOrder=HighestPriceFirst&page=${pg}`,
				userData: { pageNum: pg, isRental: true, label: "RENTALS", totalPages: maxPages },
			});
		}
	}

	if (allRequests.length > 0) {
		logger.step(`Queueing ${allRequests.length} listing pages...`);
		await crawler.run(allRequests);
	}

	logger.step(`Finished Carter Jonas - Saved: ${stats.totalSaved}`);

	if (startPage === 1) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

(async () => {
	try {
		await scrapeCarterJonas();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.message || err}`);
		process.exit(1);
	}
})();
