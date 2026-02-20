// Darlows scraper using Playwright with Crawlee
// Agent ID: 247
// Wales search: https://www.darlows.co.uk/search/?IsPurchase=True&Location=Wales%2C+UK&SearchDistance=50&Latitude=52.1306607&Longitude=-3.78371117&NumberOfResults=50
// Total: 373 properties, 8 pages (50 per page)
// Uses Agent 245 (Beresfords) architecture with Darlows-specific adaptations:
// - Darlows does NOT use JSON-LD (only Beresfords does)
// - Coordinates are in onclick="openStreetView()" attributes
// - Robust fallback extraction with multiple layers
// Usage:
// node backend/scraper-agent-247.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 247;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

function formatPrice(num, isRental) {
	if (!num || isNaN(num)) return isRental ? "£0 pcm" : "£0";
	return "£" + Number(num).toLocaleString("en-GB") + (isRental ? " pcm" : "");
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

// Configuration for Darlows Wales
// Total: 373 properties, 8 pages (50 per page)
const PROPERTY_TYPES = [
	{
		// Sales - Wales search
		urlBase:
			"https://www.darlows.co.uk/search/?IsPurchase=True&Location=Wales%2C+UK&SearchDistance=50&Latitude=52.1306607&Longitude=-3.78371117&NumberOfResults=50",
		totalPages: 8,
		isRental: false,
		label: "SALES",
	},
];

async function scrapeDarlows() {
	console.log(`\n🚀 Starting Darlows scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 5,
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

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Wait for the property listings to load
			await page
				.waitForSelector("article", { timeout: 30000 })
				.catch(() => console.log(`⚠️ No properties found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const results = [];
					const articles = Array.from(document.querySelectorAll("article"));

					for (const article of articles) {
						// Skip non-property blocks (like branch promotions)
						if (article.querySelector("img[src*='valuing-homes']")) continue;

						// Check for sold/let agreed statuses
						const articleHTML = article.innerHTML;
						if (
							articleHTML.includes("Sold STC") ||
							articleHTML.includes("Sold Subject to contract") ||
							articleHTML.includes("Let Agreed") ||
							articleHTML.includes("Sale Agreed") ||
							articleHTML.includes("Under Offer")
						) {
							continue;
						}

						// Get the property link from the heading
						const titleLink = article.querySelector("h3 a");
						if (!titleLink) continue;

						const href = titleLink.getAttribute("href");
						if (!href) continue;

						const link = href.startsWith("http")
							? href
							: new URL(href, window.location.origin).href;

						const title = titleLink.textContent?.trim() || "Property";
						results.push({ link, title });
					}
					return results;
				} catch (e) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Batch processing with interval to avoid overwhelming the server
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 90000,
							});

							const html = await detailPage.content();

							// Darlows has onclick attributes for coordinates (no JSON-LD like Beresfords)
							const detailData = await detailPage.evaluate(() => {
								try {
									const data = {
										price: null,
										bedrooms: null,
										address: null,
										lat: null,
										lng: null,
									};

									// 1. Extract price from p.price element
									const priceEl = document.querySelector("p.price");
									if (priceEl) {
										const priceText = priceEl.textContent || "";
										const priceMatch = priceText.match(/£([0-9,]+)/);
										if (priceMatch) {
											data.price = priceMatch[0];
										}
									}

									// 2. Extract address from p.meta element (after the middle dot)
									const metaEl = document.querySelector("p.meta");
									if (metaEl) {
										const metaText = metaEl.textContent || "";
										// Format: "5 Bedrooms ● Astoria Close, Cardiff"
										const parts = metaText.split("●");
										if (parts.length >= 2) {
											data.address = parts[1].trim();
										}
									}

									// Fallback: try h1 or property heading
									if (!data.address) {
										const headingEl = document.querySelector("h3.property-heading a");
										if (headingEl) {
											data.address = headingEl.textContent.trim();
										}
									}

									// 3. Extract bedrooms from page text (more robust)
									// Look for "X Bedrooms" pattern in meta tag first, then fallback to page text
									if (metaEl) {
										const metaText = metaEl.textContent || "";
										const bedMatch = metaText.match(/(\d+)\s+Bedroom/i);
										if (bedMatch) {
											data.bedrooms = bedMatch[1];
										}
									}

									// Fallback: search in page text
									if (!data.bedrooms) {
										const pageText = document.body.innerText;
										let bedMatch = pageText.match(/\b(\d+)\s+Bedroom(?:s)?\b/i);
										if (bedMatch) {
											data.bedrooms = bedMatch[1];
										}
									}

									// 4. Coordinate extraction - PRIMARY: onclick attributes
									// Format: onclick="openStreetView(this.id, lat, lng, '')"
									const onclickEls = Array.from(
										document.querySelectorAll("[onclick*='openStreetView']"),
									);
									for (const el of onclickEls) {
										const onclick = el.getAttribute("onclick");
										if (onclick) {
											// Match: openStreetView(someId, 51.565594, -3.22999, '')
											const coordMatch = onclick.match(
												/openStreetView\([^,]*,\s*([-0-9.]+),\s*([-0-9.]+)/,
											);
											if (coordMatch) {
												const lat = parseFloat(coordMatch[1]);
												const lng = parseFloat(coordMatch[2]);
												if (!isNaN(lat) && !isNaN(lng)) {
													data.lat = lat;
													data.lng = lng;
													break;
												}
											}
										}
									}

									// 5. FALLBACK: Look for coordinates in "Find similar properties" link
									if (!data.lat) {
										const similarLink = document.querySelector(
											"a[href*='Latitude='], a[href*='latitude=']",
										);
										if (similarLink) {
											const href = similarLink.getAttribute("href");
											const latMatch = href.match(/Latitude=([-0-9.]+)/);
											const lngMatch = href.match(/Longitude=([-0-9.]+)/);
											if (latMatch && lngMatch) {
												const lat = parseFloat(latMatch[1]);
												const lng = parseFloat(lngMatch[1]); // Get second capture group, not first
												if (!isNaN(lat) && !isNaN(lng)) {
													data.lat = lat;
													data.lng = lng;
												}
											}
										}
									}

									return data;
								} catch (e) {
									return null;
								}
							});

							if (detailData && (detailData.price || detailData.address)) {
								const rawPrice = (detailData.price || "").toString();
								const numMatch = rawPrice.match(/[0-9][0-9,\.\s]*/);
								const priceClean = numMatch ? numMatch[0].replace(/[^0-9]/g, "") : "";

								const bedrooms = detailData.bedrooms || null;
								const address = detailData.address || property.title || "Property";

								const result = await updatePriceByPropertyURLOptimized(
									property.link.trim(),
									priceClean || null,
									address,
									bedrooms,
									AGENT_ID,
									isRental,
								);

								if (result.updated) {
									stats.totalSaved++;
								}

								if (!result.isExisting && !result.error) {
									await processPropertyWithCoordinates(
										property.link.trim(),
										priceClean || null,
										address,
										bedrooms,
										AGENT_ID,
										isRental,
										html,
										detailData.lat,
										detailData.lng,
									);
									stats.totalSaved++;
									stats.totalScraped++;
								}

								console.log(
									`✅ ${address.substring(0, 50)} - ${formatPrice(priceClean, isRental)} - Beds: ${
										bedrooms || "?"
									} - Coords: ${
										detailData.lat
											? `${detailData.lat.toFixed(4)}, ${detailData.lng.toFixed(4)}`
											: "N/A"
									}`,
								);
							}
						} catch (err) {
							console.log(`⚠️ Error processing ${property.link}: ${err.message}`);
						} finally {
							await detailPage.close();
						}
					}),
				);

				// Pause between batches to avoid server strain
				await new Promise((resolve) => setTimeout(resolve, 500));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue all pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Enqueuing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}&Page=${pg}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Darlows - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

(async () => {
	try {
		await scrapeDarlows();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
