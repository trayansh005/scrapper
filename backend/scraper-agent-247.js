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
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 247;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(num, isRental) {
	if (!num || isNaN(num)) return isRental ? "£0 pcm" : "£0";
	return "£" + Number(num).toLocaleString("en-GB") + (isRental ? " pcm" : "");
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

									// 1. Extract address from h1
									// Format: "X Bedroom ... ● Address ● £Price"
									// Skip cookie banner h1, find the property h1
									const h1s = Array.from(document.querySelectorAll("h1"));
									const propertyH1 = h1s.find(
										(h) =>
											h.textContent.includes("●") ||
											(!h.textContent.toLowerCase().includes("cookie") &&
												h.textContent.includes("Bedroom"))
									);
									if (propertyH1) {
										const text = propertyH1.textContent || "";
										const parts = text.split("●");
										if (parts.length >= 2) {
											// "X Bedrooms ● Address ● Price" format
											data.address = parts[1].trim();
										}
									}

									// 2. Extract price from h1 or page text
									// Darlows prices appear in various places, so search the whole visible text
									const pageText = document.body.innerText;
									const priceMatch = pageText.match(/£([0-9,]+)/);
									if (priceMatch) {
										data.price = priceMatch[0];
									}

									// 3. Extract bedrooms from page text (more robust)
									// Look for "X Bedrooms" pattern anywhere on the page
									const bedMatch = pageText.match(/(\d+)\s*Bed(?:room)?s?(?:\s|●|:|$)/i);
									if (bedMatch) {
										data.bedrooms = bedMatch[1];
									}

									// 4. Coordinate extraction - PRIMARY: onclick attributes
									// Format: onclick="openStreetView(this.id, lat, lng, '')"
									const onclickEls = Array.from(
										document.querySelectorAll("[onclick*='openStreetView']")
									);
									for (const el of onclickEls) {
										const onclick = el.getAttribute("onclick");
										if (onclick) {
											// Match: openStreetView(someId, 51.565594, -3.22999, '')
											const coordMatch = onclick.match(
												/openStreetView\([^,]*,\s*([-0-9.]+),\s*([-0-9.]+)/
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
											"a[href*='Latitude='], a[href*='latitude=']"
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

								await updatePriceByPropertyURL(
									property.link.trim(),
									priceClean || null,
									address,
									bedrooms,
									AGENT_ID,
									isRental,
									detailData.lat,
									detailData.lng
								);

								console.log(
									`✅ ${address.substring(0, 50)} - ${formatPrice(priceClean, isRental)} - Beds: ${
										bedrooms || "?"
									} - Coords: ${
										detailData.lat
											? `${detailData.lat.toFixed(4)}, ${detailData.lng.toFixed(4)}`
											: "N/A"
									}`
								);
								totalSaved++;
								totalScraped++;
							}
						} catch (err) {
							console.log(`⚠️ Error processing ${property.link}: ${err.message}`);
						} finally {
							await detailPage.close();
						}
					})
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
		`\n✅ Completed Darlows - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
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
