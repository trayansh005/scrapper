// Howards scraper using Playwright with Crawlee
// Agent ID: 229
// Website: howards.co.uk
// Usage:
// node backend/scraper-agent-229.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 229;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();
let scrapeStartTime = null;

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
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
// PROPERTY TYPE CONFIGURATION
// ============================================================================

const PROPERTY_TYPES = [
	{
		baseUrl:
			"https://howards.co.uk/listings?viewType=gallery&sortby=dateListed-desc&saleOrRental=Sale&rental_period=week&status=available",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl:
			"https://howards.co.uk/listings?viewType=gallery&sortby=dateListed-desc&saleOrRental=Rental&rental_period=month&status=available",
		isRental: true,
		label: "RENTALS",
	},
];

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function extractCoordsFromDetailsPage(browserContext, propertyUrl) {
	let detailPage = null;
	const mapRequestUrls = [];
	try {
		return await Promise.race([
			(async () => {
				detailPage = await browserContext.newPage({
					ignoreHTTPSErrors: true,
				});

				detailPage.on("request", (req) => {
					const reqUrl = req.url();
					if (
						reqUrl.includes("StaticMapService.GetMapImage") ||
						reqUrl.includes("/maps/vt?pb=") ||
						reqUrl.includes("tiles.stadiamaps.com")
					) {
						mapRequestUrls.push(reqUrl);
					}
				});

				await blockNonEssentialResources(detailPage);
				await detailPage.goto(propertyUrl, { waitUntil: "domcontentloaded", timeout: 40000 });
				await detailPage.waitForTimeout(2000);

				let coords = await detailPage.evaluate(() => {
					const scripts = Array.from(document.querySelectorAll("script"));
					for (const script of scripts) {
						const text = script.textContent;
						const latMatch = text.match(/"latitude"\s*:\s*"([\d.-]+)"/i);
						const lngMatch = text.match(/"longitude"\s*:\s*"([\d.-]+)"/i);
						if (latMatch && lngMatch) {
							return {
								latitude: parseFloat(latMatch[1]),
								longitude: parseFloat(lngMatch[1]),
							};
						}
						const latNumMatch = text.match(/latitude['":\s]+([0-9.-]+)/i);
						const lngNumMatch = text.match(/longitude['":\s]+([0-9.-]+)/i);
						if (latNumMatch && lngNumMatch) {
							return {
								latitude: parseFloat(latNumMatch[1]),
								longitude: parseFloat(lngNumMatch[1]),
							};
						}
					}
					return null;
				});

				if (coords && isUkCoordinate(coords.latitude, coords.longitude)) {
					return coords;
				}

				coords = extractCoordsFromMapRequests(mapRequestUrls);
				if (coords) {
					return coords;
				}

				return null;
			})(),
			new Promise((_, reject) =>
				setTimeout(
					() => reject(new Error("Coordinate extraction timeout after 20 seconds")),
					20000,
				),
			),
		]);
	} catch (err) {
		logger.error(`Error extracting coordinates from ${propertyUrl}`, err);
		return null;
	} finally {
		if (detailPage) {
			await detailPage.close().catch(() => {});
		}
	}
}

// ============================================================================
// CRAWLER CONFIGURATION
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
		preNavigationHooks: [
			async (crawlingContext) => {
				await blockNonEssentialResources(crawlingContext.page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// REQUEST HANDLERS
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNum, totalPages } = request.userData;

	try {
		logger.page(pageNum || 1, label, "Starting", totalPages);

		await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });
		await page.waitForTimeout(2000);

		const result = await extractPropertiesFromPage(page, isRental);
		const properties = result.properties;
		const hasNextPage = result.hasNextPage;
		const newTotalPages = result.totalPages || totalPages || pageNum || 1;

		counts.totalFound += properties.length;

		if (properties.length === 0) {
			logger.page(pageNum || 1, label, `No properties found`, newTotalPages);
		}

		for (const property of properties) {
			if (processedUrls.has(property.url.trim())) {
				continue;
			}
			processedUrls.add(property.url.trim());
			counts.totalScraped++;

			try {
				const priceNum = parsePrice(property.price);
				if (priceNum === null) {
					counts.totalSkipped++;
					continue;
				}

				// Scrape detail page first to get coordinates for new properties
				const coords = await extractCoordsFromDetailsPage(page.context(), property.url);

				const result = await updatePriceByPropertyURLOptimized(
					property.url.trim(),
					priceNum,
					property.title,
					property.bedrooms,
					AGENT_ID,
					isRental,
				);

				let action = "UNCHANGED";

				if (result.updated) {
					action = "UPDATED";
					counts.totalSaved++;
					if (isRental) counts.savedRentals++;
					else counts.savedSales++;
				} else if (!result.isExisting && !result.error) {
					action = "CREATED";
					await processPropertyWithCoordinates(
						property.url.trim(),
						priceNum,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
						null,
						coords ? coords.latitude : null,
						coords ? coords.longitude : null,
					);
					counts.totalSaved++;
					if (isRental) counts.savedRentals++;
					else counts.savedSales++;
				} else if (result.error) {
					action = "ERROR";
					counts.totalSkipped++;
				}

				logger.property(
					property.title.substring(0, 50),
					formatPriceDisplay(priceNum, isRental),
					property.url,
					isRental ? "RENTALS" : "SALES",
					action,
				);

				if (action !== "UNCHANGED") {
					await sleep(100);
				}
			} catch (err) {
				logger.error(`Error saving property ${property.url}`, err);
				counts.totalSkipped++;
			}
		}

		// Queue next page if available
		if (hasNextPage) {
			const nextPageNum = (pageNum || 1) + 1;
			const urlObj = new URL(request.url);
			urlObj.searchParams.set("page", nextPageNum.toString());
			const nextUrl = urlObj.toString();

			await crawler.addRequests([
				{
					url: nextUrl,
					userData: { ...request.userData, pageNum: nextPageNum, totalPages: newTotalPages },
				},
			]);
		}

		logger.page(pageNum || 1, label, `Complete`, newTotalPages);
	} catch (error) {
		logger.error(`Error in ${label} scrape at page ${pageNum || 1}`, error);
	}
}

// ============================================================================
// PROPERTY EXTRACTION
// ============================================================================

async function extractPropertiesFromPage(page, isRental) {
	const result = await page.evaluate(() => {
		const propertyMap = new Map();
		const links = Array.from(document.querySelectorAll('a[href*="/listings/"]'));

		links.forEach((linkEl) => {
			try {
				const url = linkEl.href;
				if (!url || propertyMap.has(url)) return;

				const card = linkEl.closest('div[class*="v2-flex"]') || linkEl.parentElement?.parentElement;
				if (!card || !card.textContent.includes("Bed")) return;

				const titleEl = card.querySelector("h4");
				const title = titleEl ? titleEl.textContent.trim() : "N/A";

				let price = null;
				const strongEl = card.querySelector("strong");
				if (strongEl) {
					const priceMatch = strongEl.textContent.match(/([\d,]+)/);
					if (priceMatch) {
						price = priceMatch[1];
					}
				}

				if (!price) {
					const priceMatch = card.textContent.match(/([\d,]+)/);
					if (priceMatch) price = priceMatch[1];
				}

				let bedrooms = null;
				const pElements = Array.from(card.querySelectorAll("p"));
				const bedsP = pElements.find((p) => p.textContent.includes("Bed"));
				if (bedsP) {
					const bedsMatch = bedsP.textContent.match(/(\d+)/);
					if (bedsMatch) {
						bedrooms = parseInt(bedsMatch[1], 10);
					}
				}

				if (price) {
					propertyMap.set(url, {
						url,
						title,
						price,
						bedrooms,
						latitude: null,
						longitude: null,
					});
				}
			} catch (err) {
				// Silently skip malformed properties
			}
		});

		const nextLink = Array.from(document.querySelectorAll("a")).find(
			(a) => a.textContent.trim() === "Next " || a.textContent.trim() === "Next",
		);
		const hasNextPage = !!nextLink;

		return { properties: Array.from(propertyMap.values()), hasNextPage };
	});

	return result;
}

// ============================================================================
// COORDINATE EXTRACTION HELPERS
// ============================================================================

function extractCoordsFromMapRequests(requestUrls) {
	if (!Array.isArray(requestUrls) || requestUrls.length === 0) {
		return null;
	}

	for (const requestUrl of requestUrls) {
		const staticMatch = requestUrl.match(/[?&]1i=(\d+).*?[?&]2i=(\d+).*?[?&]3u=(\d+)/);
		if (staticMatch) {
			const pixelX = parseInt(staticMatch[1], 10);
			const pixelY = parseInt(staticMatch[2], 10);
			const zoom = parseInt(staticMatch[3], 10);

			const coords = pixelToLatLng(pixelX, pixelY, zoom);
			if (coords && isUkCoordinate(coords.latitude, coords.longitude)) {
				return coords;
			}
		}

		const vtMatch = requestUrl.match(/!1x(-?\d+)!2x(-?\d+)/);
		if (vtMatch) {
			const latE7 = parseInt(vtMatch[1], 10);
			const lonRaw = parseInt(vtMatch[2], 10);
			const lonE7 = toSigned32(lonRaw);

			const latitude = latE7 / 1e7;
			const longitude = lonE7 / 1e7;

			if (isUkCoordinate(latitude, longitude)) {
				return { latitude, longitude };
			}
		}
	}

	return null;
}

function pixelToLatLng(pixelX, pixelY, zoom) {
	if (!Number.isFinite(pixelX) || !Number.isFinite(pixelY) || !Number.isFinite(zoom)) {
		return null;
	}

	const worldSize = 256 * Math.pow(2, zoom);
	const longitude = (pixelX / worldSize) * 360 - 180;
	const n = Math.PI - (2 * Math.PI * pixelY) / worldSize;
	const latitude = (180 / Math.PI) * Math.atan(Math.sinh(n));

	if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
		return null;
	}

	return { latitude, longitude };
}

function toSigned32(value) {
	if (!Number.isFinite(value)) return value;
	return value > 2147483647 ? value - 4294967296 : value;
}

function isUkCoordinate(latitude, longitude) {
	return latitude > 49 && latitude < 61 && longitude > -11 && longitude < 3;
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		scrapeStartTime = new Date();
		const startPage = getStartPage();
		const isPartialRun = startPage > 1;

		logger.step(`Starting Howards scraper (Agent ${AGENT_ID})`);

		const browserWSEndpoint = getBrowserlessEndpoint();

		for (const propertyType of PROPERTY_TYPES) {
			const { baseUrl, isRental, label } = propertyType;

			const crawler = createCrawler(browserWSEndpoint);

			await crawler.addRequests([
				{
					url: baseUrl,
					userData: {
						isRental,
						label,
						pageNum: startPage,
					},
				},
			]);

			await crawler.run();
		}

		// Partial run protection: only mark as removed if full scrape from page 1
		if (!isPartialRun) {
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		}

		logger.step(
			`Howards scraper complete - Scraped: ${counts.totalScraped} | Saved: ${counts.totalSaved} (Sales: ${counts.savedSales}, Rentals: ${counts.savedRentals})`,
		);

		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();

