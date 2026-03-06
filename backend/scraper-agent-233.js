// Gatekeeper scraper using Playwright with Crawlee
// Agent ID: 233
// Website: gatekeeper.co.uk
// Usage:
// node backend/scraper-agent-233.js [startPage]

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

const AGENT_ID = 233;
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
		url: "https://www.gatekeeper.co.uk/properties",
		isRental: true,
		label: "RENTALS",
		buttonSelector: "#rentBtn",
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
				await detailPage.waitForTimeout(1500);

				const locationFound = await detailPage.evaluate(() => {
					const headings = Array.from(document.querySelectorAll("h3"));
					const locationHeading = headings.find((h) => h.textContent.trim() === "Location");
					if (locationHeading) {
						const rect = locationHeading.getBoundingClientRect();
						const scrollY = window.pageYOffset + rect.top;
						window.scrollTo({
							top: scrollY - 200,
							behavior: "smooth",
						});
						setTimeout(() => {
							window.scrollBy(0, 100);
						}, 500);
						return true;
					}
					return false;
				});

				if (locationFound) {
					await detailPage.waitForTimeout(1000);
				} else {
					await detailPage.evaluate(() => {
						window.scrollTo(0, document.body.scrollHeight);
					});
				}

				await detailPage.waitForTimeout(2000);

				let coords = extractCoordsFromMapRequests(mapRequestUrls);
				if (coords) {
					return coords;
				}

				coords = await detailPage.evaluate(() => {
					const mapLink = document.querySelector('a[href*="maps.google.com"]');
					if (mapLink) {
						const href = mapLink.getAttribute("href");
						if (href) {
							const llMatch = href.match(/ll=([0-9.]+),(-?[0-9.]+)/);
							if (llMatch) {
								return {
									latitude: parseFloat(llMatch[1]),
									longitude: parseFloat(llMatch[2]),
								};
							}
						}
					}
					return null;
				});

				if (!coords) {
					try {
						const iframes = await detailPage.$$("iframe");
						for (const iframe of iframes) {
							try {
								const frameHandle = await iframe.contentFrame();
								if (frameHandle) {
									const iframeCoords = await Promise.race([
										frameHandle.evaluate(() => {
											const mapLink = document.querySelector('a[href*="maps.google.com"]');
											if (mapLink) {
												const href = mapLink.getAttribute("href");
												if (href) {
													const llMatch = href.match(/ll=([0-9.]+),(-?[0-9.]+)/);
													if (llMatch) {
														return {
															latitude: parseFloat(llMatch[1]),
															longitude: parseFloat(llMatch[2]),
														};
													}
												}
											}
											return null;
										}),
										new Promise((_, reject) =>
											setTimeout(() => reject(new Error("iframe evaluation timeout")), 5000),
										),
									]);

									if (iframeCoords) {
										coords = iframeCoords;
										break;
									}
								}
							} catch (err) {
								// Continue to next iframe
							}
						}
					} catch (err) {
						// Continue with next extraction method
					}
				}

				if (!coords) {
					coords = await detailPage.evaluate(() => {
						const scripts = Array.from(document.querySelectorAll("script"));
						for (const script of scripts) {
							const text = script.textContent;
							if (text.includes("function onEmbedLoad()") && text.includes("initEmbed")) {
								const matches = text.match(/\[(\d+\.\d+),(-?\d+\.\d+)\]/g);
								if (matches && matches.length > 0) {
									for (const match of matches) {
										const coordsMatch = match.match(/\[(\d+\.\d+),(-?\d+\.\d+)\]/);
										if (coordsMatch) {
											const lat = parseFloat(coordsMatch[1]);
											const lon = parseFloat(coordsMatch[2]);
											if (lat > 50 && lat < 56 && lon > -8 && lon < 2) {
												return { latitude: lat, longitude: lon };
											}
										}
									}
								}
							}
						}
						return null;
					});
				}

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
	const properties = await page.evaluate(() => {
		const cards = Array.from(document.querySelectorAll("#properties_list a[id^='property_']"));
		const results = [];

		cards.forEach((card) => {
			try {
				const statusImg = card.querySelector('img[alt="Recently Sold"]');
				if (statusImg) {
					return;
				}

				const href = card.getAttribute("href");
				const url = href
					? href.startsWith("http")
						? href
						: "https://www.gatekeeper.co.uk" + href
					: null;

				if (!url) return;

				const titleEl = card.querySelector("h3");
				const title = titleEl ? titleEl.textContent.trim() : "N/A";

				const locationEls = Array.from(card.querySelectorAll("p")).filter(
					(p) =>
						p.textContent.includes("Oxfordshire") ||
						p.textContent.includes("Witney") ||
						p.textContent.includes("Standlake"),
				);
				const location =
					locationEls.length > 0
						? locationEls[0].textContent.trim()
						: card.querySelector("p")?.textContent.trim() || "N/A";

				const priceEls = Array.from(card.querySelectorAll("p")).filter((p) =>
					p.textContent.match(/£[\d,]+/),
				);
				const priceRaw = priceEls.length > 0 ? priceEls[0].textContent.trim() : "N/A";
				const priceText = priceRaw
					.replace(/£/g, "")
					.replace(/[^0-9,.-]/g, "")
					.trim();

				let bedrooms = null;
				const flexItems = Array.from(
					card.querySelectorAll(".flex.flex-col.justify-center.items-center"),
				);
				flexItems.forEach((item) => {
					const text = item.textContent;
					if (text.includes("Beds")) {
						const numMatch = text.match(/(\d+)\s*Beds/);
						bedrooms = numMatch ? parseInt(numMatch[1]) : null;
					}
				});

				results.push({
					url,
					title: `${title}, ${location}`,
					price: priceText,
					bedrooms,
					latitude: null,
					longitude: null,
				});
			} catch (err) {
				// Filter out broken cards
			}
		});

		return results;
	});

	return properties;
}

// ============================================================================
// PAGE LOADING WITH VIEW MORE
// ============================================================================

async function loadAllProperties(page) {
	let clickCount = 0;
	let previousCount = 0;
	let noChangeCount = 0;

	while (true) {
		try {
			const currentCount = await page.evaluate(() => {
				return document.querySelectorAll("#properties_list a[id^='property_']").length;
			});

			const button = await page.$('button[data-turbo-submits-with="Loading More Properties ..."]');
			if (!button) {
				break;
			}

			const isVisible = await button.isVisible().catch(() => false);
			if (!isVisible) {
				break;
			}

			if (currentCount === previousCount) {
				noChangeCount++;
				if (noChangeCount >= 3) {
					break;
				}
			} else {
				noChangeCount = 0;
				previousCount = currentCount;
			}

			await button.click();
			clickCount++;
			await page.waitForTimeout(2500);
		} catch (error) {
			break;
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
		async requestHandler({ page, request }) {
			const { url, isRental, label } = request.userData;

			logger.page(1, label, url, 1);

			await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
			await page.waitForTimeout(2000);

			await page.waitForSelector("#properties_list", { timeout: 30000 }).catch(() => null);

			const button = await page.$(label === "RENTALS" ? "#rentBtn" : "#buyBtn");
			if (button) {
				await button.click();
				await page.waitForTimeout(2000);

				const searchButton =
					(await page.$('button[onclick*="search_properties_form"]')) ||
					(await page.$('#search_properties_form button[type="submit"]')) ||
					(await page.$('button:has-text("Search")'));

				if (searchButton) {
					await Promise.all([
						searchButton.click(),
						page
							.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 30000 })
							.catch(() => {}),
					]);
					await page.waitForTimeout(1500);
				}
			}

			await loadAllProperties(page);

			const properties = await extractPropertiesFromPage(page, isRental);
			counts.totalFound += properties.length;

			// Process properties sequentially (not in batches for better control)
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
					} else if (result.error) {
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

			logger.page(1, label, "Complete", 1);
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
		const isPartialRun = false; // Gatekeeper has fixed pagination, not paginated

		logger.step(`Starting Gatekeeper scraper (Agent ${AGENT_ID})`);

		const browserWSEndpoint = getBrowserlessEndpoint();

		for (const propertyType of PROPERTY_TYPES) {
			const crawler = createCrawler(browserWSEndpoint);

			const initialRequests = [
				{
					url: propertyType.url,
					userData: {
						url: propertyType.url,
						isRental: propertyType.isRental,
						label: propertyType.label,
					},
				},
			];

			await crawler.run(initialRequests);
		}

		if (!isPartialRun) {
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		}

		logger.step(
			`Gatekeeper scraper complete - Found: ${counts.totalFound} | Scraped: ${counts.totalScraped} | Saved: ${counts.totalSaved} (Rentals: ${counts.savedRentals})`,
		);

		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
