// ESPC scraper using Playwright with Crawlee
// Agent ID: 222
// Website: espc.com
// Usage:
// node backend/scraper-agent-222.js

const { PlaywrightCrawler, log } = require("crawlee");
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

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 222;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const recentPageSignatures = new Map();
const processedUrls = new Set();

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

const PROPERTY_TYPES = [
	{
		baseUrl: "https://espc.com/properties",
		isRental: false,
		label: "SALES",
	},
];

// ============================================================================
// COORDINATE EXTRACTION — ESPC JSON-LD schema.org/GeoCoordinates
// Structure:
//   {"@type":"SingleFamilyResidence", ...,
//    "geo":{"@type":"GeoCoordinates","latitude":"55.952...","longitude":"-3.146..."}}
// ============================================================================

function extractCoordsFromJsonLd(html) {
	try {
		// Match all JSON-LD script blocks
		const scriptRegex = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
		let match;
		while ((match = scriptRegex.exec(html)) !== null) {
			try {
				const obj = JSON.parse(match[1]);
				const geo = obj?.geo;
				if (geo?.latitude && geo?.longitude) {
					const lat = parseFloat(geo.latitude);
					const lon = parseFloat(geo.longitude);
					if (!isNaN(lat) && !isNaN(lon)) {
						return { latitude: lat, longitude: lon };
					}
				}
			} catch (_) { }
		}
	} catch (_) { }
	return { latitude: null, longitude: null };
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNumber } = request.userData;

	logger.page(pageNumber || 1, label, request.url);

	try {
		await page.waitForTimeout(700);
		await page.waitForSelector('a[href*="/property/"] h3', { timeout: 15000 }).catch(() => {
			logger.warn(`Listing container not found on page ${pageNumber || 1}`);
		});

		const result = await page.evaluate(() => {
			try {
				const items = Array.from(document.querySelectorAll('a[href*="/property/"]')).filter(
					(a) => a.querySelector("h3"),
				);

				const scraped = items.map((item) => {
					const titleEl = item.querySelector("h3.propertyTitle");
					const title = titleEl ? titleEl.innerText.trim() : "N/A";

					const priceEl = item.querySelector(".price");
					const priceRaw = priceEl ? priceEl.innerText.trim() : null;

					let bedrooms = null;
					const bedEl = Array.from(item.querySelectorAll(".facilities .opt")).find((opt) =>
						opt.querySelector(".icon.bed"),
					);
					if (bedEl) {
						const bedText = bedEl.innerText.trim();
						const match = bedText.match(/(\d+)/);
						if (match) bedrooms = parseInt(match[1]);
					}

					const statusText = item.innerText || "";
					return { link: item.href, title, priceRaw, bedrooms, statusText };
				});

				const paginationNext = document.querySelector(
					"a.next, a.nextPage, .paginationList a.next",
				);

				return { properties: scraped, hasNextPage: !!paginationNext };
			} catch (e) {
				return { properties: [], hasNextPage: false };
			}
		});

		const properties = result.properties;
		logger.page(pageNumber || 1, label, `Found ${properties.length} properties`);

		// Duplicate-page detection
		const pageSignature = properties.map((p) => p.link).slice(0, 5).join("|");
		const signatureKey = isRental ? "RENTALS" : "SALES";
		const previousSignature = recentPageSignatures.get(signatureKey);
		if (pageSignature && previousSignature === pageSignature) {
			logger.warn(`${signatureKey} page ${pageNumber || 1} has same links as previous — possible pagination loop`);
		}
		recentPageSignatures.set(signatureKey, pageSignature);

		for (const property of properties) {
			if (!property.link) continue;
			if (isSoldProperty(property.statusText || "")) continue;
			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			// Parse price using shared helper
			const price = parsePrice(property.priceRaw || "");
			if (!price) {
				logger.warn(`Skipping (no price): ${property.link}`);
				continue;
			}

			const bedrooms =
				property.bedrooms || extractBedroomsFromHTML(property.title || "");

			// --- Agent 39 base pattern: check existing → update or create ---
			const dbResult = await updatePriceByPropertyURLOptimized(
				property.link,
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			if (dbResult.updated) {
				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (dbResult.isExisting) {
				counts.totalScraped++;
			}

			let propertyAction = "UNCHANGED";
			if (dbResult.updated) propertyAction = "UPDATED";

			if (!dbResult.isExisting && !dbResult.error) {
				propertyAction = "CREATED";

				// Fetch detail page to get JSON-LD coordinates
				const detailPage = await page.context().newPage();
				let html = null;
				let latitude = null;
				let longitude = null;

				try {
					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 40000,
					});
					html = await detailPage.content();

					// Extract coords from ESPC's schema.org JSON-LD geo block
					const coords = extractCoordsFromJsonLd(html);
					latitude = coords.latitude;
					longitude = coords.longitude;

					logger.step(
						`Coords: ${latitude || "No Lat"}, ${longitude || "No Lng"}`,
					);
				} catch (err) {
					logger.error(`Failed to load detail page: ${property.link}`);
				} finally {
					await detailPage.close();
				}

				await processPropertyWithCoordinates(
					property.link,
					price,
					property.title,
					bedrooms,
					AGENT_ID,
					isRental,
					html,
					latitude,
					longitude,
				);

				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			}

			logger.property(
				pageNumber || 1,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				null,
				propertyAction,
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500); // DB politeness delay for writes
			}
		}

		// Queue next page if pagination detected
		if (result.hasNextPage) {
			const nextPage = (pageNumber || 1) + 1;
			const nextUrl = `https://espc.com/properties?p=${nextPage}`;
			await crawler.addRequests([
				{
					url: nextUrl,
					userData: { isRental, label, pageNumber: nextPage },
				},
			]);
		}
	} catch (error) {
		logger.error(`Error in handleListingPage: ${error.message}`);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeAll() {
	logger.step(`Starting ESPC Scraper (Agent ${AGENT_ID})...`);
	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	for (const propertyType of PROPERTY_TYPES) {
		const crawler = createCrawler(browserWSEndpoint);
		await crawler.addRequests([
			{
				url: propertyType.baseUrl,
				userData: { isRental: propertyType.isRental, label: propertyType.label, pageNumber: 1 },
			},
		]);
		await crawler.run();
	}

	logger.step(
		`Completed ESPC - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
}

(async () => {
	try {
		const scrapeStartTime = new Date();
		await scrapeAll();
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
