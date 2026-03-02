// Kinleigh Folkard & Hayward (KFH) scraper using Playwright with Crawlee
// Agent ID: 75
// Modeled after agent 39 pattern (positional args, create vs update)
//
// Usage:
// node backend/scraper-agent-75.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	extractCoordinatesFromHTML,
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce logging noise
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 75;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// Configuration for sales and rentals — full KFH URLs
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.kfh.co.uk/property/for-sale/in-london/exclude-sale-agreed/",
		totalRecords: 1689,
		totalPages: 94,
		recordsPerPage: 18,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.kfh.co.uk/property/to-rent/in-london/exclude-let-agreed/",
		totalRecords: 691,
		totalPages: 39,
		recordsPerPage: 18,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeKFH() {
	logger.step(`Starting KFH scraper (Agent ${AGENT_ID})...`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			logger.page(pageNum, label, request.url);

			// Wait for page to load
			await page.waitForTimeout(6000);

			// Accept cookies if banner is present
			const cookieButton = await page.$("#onetrust-accept-btn-handler");
			if (cookieButton) {
				await cookieButton.click();
				await page.waitForTimeout(2000);
			}

			// Try to wait for a known listing container
			await page
				.waitForSelector(
					".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0, .property-card, .result-card",
					{ timeout: 40000 },
				)
				.catch(() => {
					logger.step(`No listing container found on page ${pageNum}`);
				});

			// Extract properties from DOM
			const properties = await page.evaluate(() => {
				try {
					const cardSelector =
						".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0";
					const cards = Array.from(document.querySelectorAll(cardSelector));

					return cards
						.map((card) => {
							try {
								// Link: prefer property detail links
								let link = null;
								const anchors = card.querySelectorAll("a[href]");
								for (const a of anchors) {
									const href = a.getAttribute("href");
									if (
										href &&
										(href.includes("/property-for-sale/") ||
											href.includes("/property-to-rent/"))
									) {
										link = a.href;
										break;
									}
									if (href && href.includes("/property")) {
										link = a.href;
										break;
									}
								}

								// Title
								let title =
									card.querySelector("h3")?.textContent?.trim() ||
									card
										.querySelector("a[class*='PropertyCard__StyledAddressLink']")
										?.textContent?.trim() ||
									null;

								// Price (raw text — will be parsed server-side)
								let price =
									card.querySelector(".highlight-text")?.textContent?.trim() ||
									card
										.querySelector(
											".PropertyPriceAndStatus__StyledPrice-sc-1dv7ovq-0 h2",
										)
										?.textContent?.trim() ||
									card
										.querySelector(
											"p[class*='PropertyPriceAndStatus__StyledPrice']",
										)
										?.textContent?.trim() ||
									card
										.querySelector(".PropertyCard__StyledPrice")
										?.textContent?.trim() ||
									"";

								// Bedrooms
								let bedrooms = null;
								const bedSpan = card.querySelector(".p-bed");
								if (bedSpan) {
									const text =
										bedSpan.parentElement?.textContent?.trim() || "";
									const match = text.match(/(\d+)\s+bedrooms?/i);
									if (match) bedrooms = match[1];
								}
								if (!bedrooms) {
									const bedIcon = card.querySelector(".icon-bed");
									if (bedIcon) {
										bedrooms =
											bedIcon.nextElementSibling?.textContent?.trim();
									}
								}
								if (!bedrooms) {
									const bedSpans = card.querySelectorAll(
										"span[class*='PropertyMeta__StyledMetaItem'], .property-meta-item",
									);
									for (const span of bedSpans) {
										const text = span.textContent?.trim() || "";
										if (text.toLowerCase().includes("bedroom")) {
											const match = text.match(/(\d+)/);
											bedrooms = match ? match[1] : null;
											break;
										}
									}
								}
								if (!bedrooms && link) {
									const urlBedMatch = link.match(/(\d+)-bedroom/i);
									if (urlBedMatch) bedrooms = urlBedMatch[1];
								}

								if (link && title) {
									return { link, title, price, bedrooms };
								}
								return null;
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					return [];
				}
			});

			logger.page(pageNum, label, `Found ${properties.length} properties`);

			// Process properties in batches
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						// Parse price to number
						const price = parsePrice(property.price.toString());
						if (!price) return;

						// Open detail page to get coords + sold status
						const detailPage = await page.context().newPage();
						let htmlContent = "";
						let coords = { latitude: null, longitude: null };
						let sold = false;

						try {
							await blockNonEssentialResources(detailPage);
							await detailPage.goto(property.link, {
								// domcontentloaded is enough — __NEXT_DATA__ is SSR'd into the initial HTML.
								// networkidle often times out on KFH due to analytics/3rd-party scripts.
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							// Brief pause for React hydration to complete before evaluate()
							await detailPage.waitForTimeout(1000);

							htmlContent = await detailPage.content();

							// KFH is Next.js/React — coords live in JS state.
							// Extract directly from the live browser context.
							const evalCoords = await detailPage.evaluate(() => {
								try {
									// 1. Next.js: __NEXT_DATA__ page props
									if (window.__NEXT_DATA__) {
										const str = JSON.stringify(window.__NEXT_DATA__);
										const latM = str.match(/"lat(?:itude)?"\s*:\s*([-\d.]+)/i);
										const lngM = str.match(/"l(?:ng|on)(?:gitude)?"\s*:\s*([-\d.]+)/i);
										if (latM && lngM) return { latitude: parseFloat(latM[1]), longitude: parseFloat(lngM[1]) };
									}

									// 2. JSON-LD structured data
									for (const el of document.querySelectorAll('script[type="application/ld+json"]')) {
										try {
											const obj = JSON.parse(el.textContent);
											const geo = obj?.geo || obj?.location?.geo;
											if (geo?.latitude && geo?.longitude)
												return { latitude: parseFloat(geo.latitude), longitude: parseFloat(geo.longitude) };
										} catch (_) { }
									}

									// 3. Window globals (propertyData, digitalData, etc.)
									for (const key of ["propertyData", "digitalData", "pageData", "property"]) {
										const obj = window[key];
										if (obj) {
											const str = JSON.stringify(obj);
											const latM = str.match(/"lat(?:itude)?"\s*:\s*([-\d.]+)/i);
											const lngM = str.match(/"l(?:ng|on)(?:gitude)?"\s*:\s*([-\d.]+)/i);
											if (latM && lngM) return { latitude: parseFloat(latM[1]), longitude: parseFloat(lngM[1]) };
										}
									}

									// 4. data-lat / data-lng / data-latitude / data-longitude attributes
									const el =
										document.querySelector("[data-lat][data-lng]") ||
										document.querySelector("[data-latitude][data-longitude]");
									if (el) {
										const lat = parseFloat(el.dataset.lat || el.dataset.latitude);
										const lng = parseFloat(el.dataset.lng || el.dataset.longitude);
										if (!isNaN(lat) && !isNaN(lng)) return { latitude: lat, longitude: lng };
									}

									// 5. Google Maps iframe embed src: ?q=lat,lng or &center=lat,lng
									const iframe = document.querySelector('iframe[src*="google.com/maps"]');
									if (iframe) {
										const src = iframe.src;
										const qM = src.match(/[?&]q=([-\d.]+),([-\d.]+)/);
										const cM = src.match(/[?&]center=([-\d.]+),([-\d.]+)/);
										const m = qM || cM;
										if (m) return { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };
									}

									return null;
								} catch (_) {
									return null;
								}
							});

							if (evalCoords?.latitude && evalCoords?.longitude) {
								coords = evalCoords;
							} else {
								// Fallback: static HTML regex (covers older patterns)
								const extracted = extractCoordinatesFromHTML(htmlContent);
								if (extracted?.latitude && extracted?.longitude) coords = extracted;
							}

							sold = isSoldProperty(htmlContent);

							logger.step(
								`Coords: ${coords?.latitude || "No Lat"}, ${coords?.longitude || "No Lng"} | Sold: ${sold}`,
							);
						} catch (err) {
							logger.error(`Detail page error: ${err.message || err}`);
						} finally {
							await detailPage.close();
						}

						if (sold) {
							logger.step(`Skipping sold property: ${property.link}`);
							return;
						}

						// --- Agent 39 pattern: check existing → create or update ---
						const result = await updatePriceByPropertyURLOptimized(
							property.link,
							price,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
						);

						if (result.updated) {
							counts.totalSaved++;
							counts.totalScraped++;
							if (isRental) counts.savedRentals++;
							else counts.savedSales++;
						} else if (result.isExisting) {
							counts.totalScraped++;
						}

						let propertyAction = "UNCHANGED";
						if (result.updated) propertyAction = "UPDATED";

						if (!result.isExisting && !result.error) {
							propertyAction = "CREATED";
							// Insert new property with coordinates extracted from detail page HTML
							await processPropertyWithCoordinates(
								property.link,
								price,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								htmlContent,
								coords?.latitude || null,
								coords?.longitude || null,
							);
							counts.totalSaved++;
							counts.totalScraped++;
							if (isRental) counts.savedRentals++;
							else counts.savedSales++;
						}

						logger.property(
							pageNum,
							label,
							property.title.substring(0, 40),
							formatPriceDisplay(price, isRental),
							property.link,
							isRental,
							null,
							propertyAction,
						);
					}),
				);

				// Small delay between batches
				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
		},
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
	});

	// Enqueue all listing pages
	for (const propertyType of PROPERTY_TYPES) {
		logger.step(
			`Processing ${propertyType.label} (${propertyType.totalPages} pages, ${propertyType.recordsPerPage} per page)`,
		);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}page-${pg}/`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		if (requests.length > 0) {
			await crawler.run(requests);
		}
	}

	logger.step(
		`Completed KFH - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
}

(async () => {
	try {
		const scrapeStartTime = new Date();
		await scrapeKFH();
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
