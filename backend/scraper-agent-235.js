// Fulford Properties scraper using Playwright with Crawlee
// Agent ID: 235
// Usage:
// node backend/scraper-agent-235.js

const { PlaywrightCrawler, log } = require("crawlee");
const { firefox } = require("playwright");
const { launchOptions } = require("camoufox-js");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 235;
let totalScraped = 0;
let totalSaved = 0;
const processedUrls = new Set();

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

// Sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.fulfords.co.uk/properties/sales/status-available/most-recent-first/",
	// 	totalPages: 53,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// },
	{
		urlBase: "https://www.fulfords.co.uk/properties/lettings/status-available/most-recent-first/",
		totalPages: 10,
		isRental: true,
		label: "TO LET",
	},
];

async function scrapeFulford() {
	console.log(`\n🚀 Starting Fulford scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 5,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: firefox,
			launchOptions: await launchOptions({
				headless: true,
			}),
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Add a random delay to avoid 429
			await page.waitForTimeout(Math.floor(Math.random() * 3000) + 2000);

			// Wait for listing anchor links
			await page
				.waitForSelector("a.card__link", { timeout: 15000 })
				.catch(() => console.log(`⚠️ No listing links found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const anchors = Array.from(document.querySelectorAll("a.card__link"));
					return anchors
						.map((a) => {
							try {
								const href = a.getAttribute("href");
								if (!href) return null;
								// Price is usually in a span inside the anchor
								const priceSpan = a.querySelector("span");
								const rawPrice = priceSpan ? priceSpan.textContent.trim() : "";
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = parseInt(m[0].replace(/,/g, "")).toLocaleString();
								}

								// Title/address appears after the span node (text node)
								let title = a.textContent || "";
								title = title.replace(priceSpan ? priceSpan.textContent : "", "").trim();

								// Construct absolute URL if necessary
								const link = href.startsWith("/") ? `https://www.fulfords.co.uk${href}` : href;

								// Attempt to get bedroom count from the nearest card spec list
								let bedrooms = null;
								try {
									const card = a.closest(".card");
									if (card) {
										const specItems = Array.from(
											card.querySelectorAll(".card-content__spec-list li")
										);
										specItems.forEach((li) => {
											// Bedroom icon has class 'icon-bedroom' on the svg
											if (li.querySelector(".icon-bedroom, svg.icon-bedroom")) {
												const span = li.querySelector("span, .card-content__spec-list-number");
												if (span) bedrooms = span.textContent.trim();
											}
										});
									}
								} catch (e) {
									bedrooms = null;
								}

								return { link, title, price, bedrooms };
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 1;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						// Global deduplication in the same run
						if (processedUrls.has(property.link)) {
							console.log(`⏩ Skipping duplicate URL: ${property.link.substring(0, 60)}...`);
							return;
						}
						processedUrls.add(property.link);

						let coords = { latitude: null, longitude: null };

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							// Try multiple strategies for extracting coordinates
							const scriptCoords = await detailPage.evaluate(() => {
								// 1) Look for data attributes on map container
								const mapEl = document.querySelector(
									"[data-lat][data-lng], [data-latitude][data-longitude]"
								);
								if (mapEl) {
									const lat = mapEl.getAttribute("data-lat") || mapEl.getAttribute("data-latitude");
									const lng =
										mapEl.getAttribute("data-lng") || mapEl.getAttribute("data-longitude");
									if (lat && lng) return { latitude: parseFloat(lat), longitude: parseFloat(lng) };
								}

								// 2) Common meta tags
								const metaLat = document.querySelector(
									'meta[property="place:location:latitude"], meta[name="latitude"]'
								);
								const metaLng = document.querySelector(
									'meta[property="place:location:longitude"], meta[name="longitude"]'
								);
								if (metaLat && metaLng) {
									return {
										latitude: parseFloat(metaLat.content),
										longitude: parseFloat(metaLng.content),
									};
								}

								// 3) Search scripts for patterns
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const script of scripts) {
									const text = script.textContent || "";

									// Google Maps LatLng
									const m1 = text.match(/new google\.maps\.LatLng\(([0-9.-]+),\s*([0-9.-]+)\)/);
									if (m1) return { latitude: parseFloat(m1[1]), longitude: parseFloat(m1[2]) };

									// const lat = parseFloat('51.781799'); const lng = parseFloat('0.660231');
									const mLat = text.match(/const\s+lat\s*=\s*parseFloat\(['\"]([0-9.-]+)['\"]\)/);
									const mLng = text.match(/const\s+lng\s*=\s*parseFloat\(['\"]([0-9.-]+)['\"]\)/);
									if (mLat && mLng)
										return { latitude: parseFloat(mLat[1]), longitude: parseFloat(mLng[1]) };

									// JSON-like object containing lat/lng keys
									const mJson = text.match(
										/\{[^{}]*\b(lat|latitude)\b[^{}]*\b(lng|longitude)\b[^{}]*\}/
									);
									if (mJson) {
										try {
											const jsonLike = mJson[0]
												.replace(/([a-zA-Z0-9_]+)\s*:/g, '"$1":')
												.replace(/'/g, '"');
											const parsed = JSON.parse(jsonLike);
											const latVal = parsed.lat || parsed.latitude;
											const lngVal = parsed.lng || parsed.longitude;
											if (latVal && lngVal)
												return { latitude: parseFloat(latVal), longitude: parseFloat(lngVal) };
										} catch (e) {
											// ignore
										}
									}
								}

								return null;
							});

							if (scriptCoords && scriptCoords.latitude && scriptCoords.longitude) {
								coords.latitude = scriptCoords.latitude;
								coords.longitude = scriptCoords.longitude;
								console.log(`  📍 Found coords: ${coords.latitude}, ${coords.longitude}`);
							}
						} catch (err) {
							// ignore detail page failures
						} finally {
							await detailPage.close();
						}

						try {
							const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;
							const priceNum = parseFloat(priceClean);

							await updatePriceByPropertyURL(
								property.link,
								priceClean,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								coords.latitude,
								coords.longitude
							);

							totalSaved++;
							totalScraped++;

							const priceDisplay = isNaN(priceNum) ? "N/A" : formatPrice(priceNum);
							if (coords.latitude && coords.longitude) {
								console.log(
									`✅ ${property.title} - ${priceDisplay} - ${coords.latitude}, ${coords.longitude}`
								);
							} else {
								console.log(`✅ ${property.title} - ${priceDisplay} - No coords`);
							}
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					})
				);

				await new Promise((resolve) =>
					setTimeout(resolve, Math.floor(Math.random() * 1000) + 1000)
				);
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			// Fulford pagination: base URL for page 1, then /page-N#/ for others
			const url = pg === 1 ? propertyType.urlBase : `${propertyType.urlBase}page-${pg}#/`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();

		// Small delay between property types
		await new Promise((resolve) => setTimeout(resolve, 5000));
	}

	console.log(
		`\n✅ Completed Fulford scraper - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeFulford();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
