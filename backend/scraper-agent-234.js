// Miles Byron scraper using Playwright with Crawlee
// Agent ID: 234
// Website: milesbyron.com
// Usage:
// node backend/scraper-agent-234.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 234;
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
			"https://www.milesbyron.com/properties-search/?location%5B%5D=wiltshire&status=for-sale",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl:
			"https://www.milesbyron.com/properties-search/?location%5B%5D=wiltshire&status=for-rent",
		isRental: true,
		label: "RENTALS",
	},
];

// ============================================================================
// COORDINATE EXTRACTION HELPERS
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
					if (reqUrl.includes("StaticMapService.GetMapImage") || reqUrl.includes("/maps/vt?pb=")) {
						mapRequestUrls.push(reqUrl);
					}
				});

				await detailPage.goto(propertyUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
				await detailPage.waitForTimeout(2000);

				let coords = extractCoordsFromMapRequests(mapRequestUrls);
				if (coords) {
					return coords;
				}

				coords = await detailPage.evaluate(() => {
					const scripts = Array.from(document.querySelectorAll("script"));
					for (const script of scripts) {
						const text = script.textContent;
						const latMatch = text.match(/latitude['":\s]+([0-9.-]+)/i);
						const lngMatch = text.match(/longitude['":\s]+([0-9.-]+)/i);
						if (latMatch && lngMatch) {
							return {
								latitude: parseFloat(latMatch[1]),
								longitude: parseFloat(lngMatch[1]),
							};
						}
					}
					return null;
				});

				return coords;
			})(),
			new Promise((_, reject) =>
				setTimeout(() => reject(new Error("Coordinate extraction timeout")), 20000),
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
// PROPERTY EXTRACTION
// ============================================================================

async function extractPropertiesFromPage(page, isRental) {
	const result = await page.evaluate(() => {
		const properties = [];
		const cards = Array.from(document.querySelectorAll("article.rh_list_card"));

		cards.forEach((card) => {
			try {
				const linkEl = card.querySelector("a[href*='/property/']");
				if (!linkEl) return;
				const url = linkEl.getAttribute("href");
				if (!url) return;

				const titleEl = card.querySelector("h3 a");
				const title = titleEl ? titleEl.textContent.trim() : "N/A";

				let price = null;
				const priceText = card.innerText;
				const priceMatch = priceText.match(/£([\d,]+)/);
				if (priceMatch) {
					price = priceMatch[1];
				}

				let bedrooms = null;
				const bedsSpans = Array.from(card.querySelectorAll("div, span"));
				const bedsSpan = bedsSpans.find((s) => s.textContent.trim() === "Bedrooms");
				if (bedsSpan && bedsSpan.nextElementSibling) {
					const bedsMatch = bedsSpan.nextElementSibling.textContent.match(/(\d+)/);
					if (bedsMatch) {
						bedrooms = parseInt(bedsMatch[1], 10);
					}
				}

				if (price) {
					properties.push({
						url,
						title,
						price,
						bedrooms,
						latitude: null,
						longitude: null,
					});
				}
			} catch (err) {
				// Filter out broken cards
			}
		});

		const nextLink =
			document.querySelector("a.rh_pagination__btn.next") ||
			Array.from(document.querySelectorAll("a")).find((a) => a.textContent.includes("Next"));
		const hasNextPage = !!nextLink;

		return { properties, hasNextPage };
	});

	return result;
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		async requestHandler({ page, request, crawler }) {
			const { isRental, label, pageNumber } = request.userData;

			logger.page(pageNumber, label, request.url, 999);

			try {
				await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });
				await page.waitForTimeout(2000);

				const result = await extractPropertiesFromPage(page, isRental);
				const properties = result.properties;
				const hasNextPage = result.hasNextPage;

				counts.totalFound += properties.length;

				// Process properties sequentially (not in batches)
				for (const property of properties) {
					if (!property.url) continue;

					if (processedUrls.has(property.url.trim())) {
						continue;
					}
					processedUrls.add(property.url.trim());

					try {
						const priceNum = parsePrice(property.price);
						if (priceNum === null) {
							counts.totalSkipped++;
							continue;
						}

						// Check if property exists first
						const updateResult = await updatePriceByPropertyURLOptimized(
							property.url.trim(),
							priceNum,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
						);

						let action = "UNCHANGED";

						if (updateResult.updated) {
							action = "UPDATED";
							counts.totalSaved++;
							if (isRental) counts.savedRentals++;
							else counts.savedSales++;
						} else if (!updateResult.isExisting && !updateResult.error) {
							// Only load detail page for NEW properties
							const coords = await extractCoordsFromDetailsPage(page.context(), property.url);
							if (coords) {
								property.latitude = coords.latitude;
								property.longitude = coords.longitude;
							}

							const htmlContent = null; // Not needed since we have coordinates
							await processPropertyWithCoordinates(
								property.url.trim(),
								priceNum,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								htmlContent,
								property.latitude,
								property.longitude,
							);

							action = "CREATED";
							counts.totalSaved++;
							counts.totalScraped++;
							if (isRental) counts.savedRentals++;
							else counts.savedSales++;
						} else if (updateResult.error) {
							action = "ERROR";
							counts.totalSkipped++;
						}

						logger.property(
							property.title.substring(0, 50),
							formatPriceDisplay(priceNum, isRental),
							property.url,
							label,
							action,
						);

						// Only sleep for CREATED properties
						if (action === "CREATED") {
							await sleep(500);
						}
					} catch (err) {
						logger.error(`Error processing property ${property.url}`, err);
						counts.totalSkipped++;
					}
				}

				// Queue next page if available
				if (hasNextPage) {
					const nextPageNum = (pageNumber || 1) + 1;
					const nextUrl = `https://www.milesbyron.com/properties-search/page/${nextPageNum}/?location%5B%5D=wiltshire&status=${isRental ? "for-rent" : "for-sale"}`;
					await crawler.addRequests([
						{
							url: nextUrl,
							userData: { isRental, label, pageNumber: nextPageNum },
						},
					]);
				}

				logger.page(pageNumber, label, "Complete", 999);
			} catch (error) {
				logger.error(`Error in ${label} scrape for page ${pageNumber}`, error);
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		scrapeStartTime = new Date();

		logger.step(`Starting Miles Byron scraper (Agent ${AGENT_ID})`);

		const browserWSEndpoint = getBrowserlessEndpoint();

		for (const propertyType of PROPERTY_TYPES) {
			const { baseUrl, isRental, label } = propertyType;

			const crawler = createCrawler(browserWSEndpoint);

			const initialRequests = [
				{
					url: baseUrl,
					userData: { isRental, label, pageNumber: 1 },
				},
			];

			await crawler.run(initialRequests);
		}

		await updateRemoveStatus(AGENT_ID, scrapeStartTime);

		logger.step(
			`Miles Byron scraper complete - Found: ${counts.totalFound} | Scraped: ${counts.totalScraped} | Saved: ${counts.totalSaved} (Sales: ${counts.savedSales}, Rentals: ${counts.savedRentals})`,
		);

		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
