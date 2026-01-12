// Manning Stainton scraper using Playwright with Crawlee
// Agent ID: 212
// Usage:
// node backend/scraper-agent-212.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 212;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

// Configuration for Manning Stainton
// 10 properties per page; sales 106 pages, rent 8 pages
const PROPERTY_TYPES = [
	// {
	// 	// Sales
	// 	urlBase: "https://manningstainton.co.uk/properties-for-sale/All?excludeSstc=1",
	// 	totalPages: 106,
	// 	recordsPerPage: 10,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		// Rentals
		urlBase: "https://manningstainton.co.uk/properties-to-rent/All?excludeSstc=1",
		totalPages: 8,
		recordsPerPage: 10,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeManningStainton() {
	console.log(`\n🚀 Starting Manning Stainton scraper (Agent ${AGENT_ID})...\n`);

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

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(1000);

			// Wait for listing cards
			await page
				.waitForSelector('[class*="SearchResultCard_searchItem"]', { timeout: 20000 })
				.catch(() => console.log(`⚠️ No search result cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll('[class*="SearchResultCard_searchItem"]')
					);
					return items
						.map((el) => {
							const linkEl = el.querySelector("a[href]");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://manningstainton.co.uk" + href
								: null;

							const title =
								el.querySelector(".SearchResultCard_title__STErD h3")?.textContent?.trim() ||
								el.querySelector("h3")?.textContent?.trim() ||
								"";
							const address =
								el.querySelector(".SearchResultCard_address__NMzbh")?.textContent?.trim() || "";
							const price =
								el.querySelector('[class*="SearchResultCard_price"]')?.textContent?.trim() || "";

							// bedrooms indicated by .htype1
							let bedrooms = null;
							const bedLi = el.querySelector(".htype1");
							if (bedLi) bedrooms = bedLi.textContent.replace(/\D+/g, "").trim();

							return { link, price, title: title || address || "", bedrooms, lat: null, lng: null };
						})
						.filter((p) => p.link);
				} catch (e) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						let coords = { latitude: property.lat || null, longitude: property.lng || null };

						if (!coords.latitude || !coords.longitude) {
							const detailPage = await page.context().newPage();
							try {
								await detailPage.goto(property.link, {
									waitUntil: "domcontentloaded",
									timeout: 30000,
								});
								await detailPage.waitForTimeout(500);

								const detailCoords = await detailPage.evaluate(() => {
									try {
										// Try application/ld+json first
										const scripts = Array.from(
											document.querySelectorAll('script[type="application/ld+json"]')
										);
										for (const s of scripts) {
											try {
												const data = JSON.parse(s.textContent);
												// some sites use geolocation or geo
												if (
													data &&
													data.geolocation &&
													(data.geolocation.latitude || data.geolocation.longitude)
												) {
													const lat = parseFloat(data.geolocation.latitude);
													const lng = parseFloat(data.geolocation.longitude);
													if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
												}
												if (data && data.geo && (data.geo.latitude || data.geo.longitude)) {
													const lat = parseFloat(data.geo.latitude);
													const lng = parseFloat(data.geo.longitude);
													if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
												}
												// handle @graph
												const graph = data["@graph"] || (Array.isArray(data) ? data : null);
												if (graph) {
													for (const node of graph) {
														if (
															node &&
															node.geolocation &&
															(node.geolocation.latitude || node.geolocation.longitude)
														) {
															const lat = parseFloat(node.geolocation.latitude);
															const lng = parseFloat(node.geolocation.longitude);
															if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
														}
														if (node && node.geo && (node.geo.latitude || node.geo.longitude)) {
															const lat = parseFloat(node.geo.latitude);
															const lng = parseFloat(node.geo.longitude);
															if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
														}
													}
												}
											} catch (e) {
												// continue
											}
										}

										// Fallback: search all scripts for geolocation/latitude/longitude pairs
										const all = Array.from(document.querySelectorAll("script"))
											.map((s) => s.textContent)
											.join("\n");
										const geoMatch = all.match(/"geolocation"\s*:\s*\{[^}]*\}/i);
										if (geoMatch) {
											const geoText = geoMatch[0];
											const latMatch = geoText.match(/"latitude"\s*:\s*([0-9.+-]+)/i);
											const lngMatch = geoText.match(/"longitude"\s*:\s*([0-9.+-]+)/i);
											if (latMatch && lngMatch) {
												let lat = parseFloat(latMatch[1]);
												let lng = parseFloat(lngMatch[1]);
												// If values look swapped (latitude outside [-90,90]) swap
												if (Math.abs(lat) > 90 && Math.abs(lng) <= 90) {
													const t = lat;
													lat = lng;
													lng = t;
												}
												if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
											}
										}

										// Regex fallback for generic latitude/longitude keys
										const latMatch =
											all.match(/"latitude"\s*:\s*([0-9.+-]+)/i) ||
											all.match(/"lat"\s*:\s*([0-9.+-]+)/i);
										const lngMatch =
											all.match(/"longitude"\s*:\s*([0-9.+-]+)/i) ||
											all.match(/"lng"\s*:\s*([0-9.+-]+)/i);
										if (latMatch && lngMatch) {
											let lat = parseFloat(latMatch[1]);
											let lng = parseFloat(lngMatch[1]);
											if (Math.abs(lat) > 90 && Math.abs(lng) <= 90) {
												const t = lat;
												lat = lng;
												lng = t;
											}
											return { lat, lng };
										}

										return null;
									} catch (e) {
										return null;
									}
								});

								if (detailCoords) {
									coords.latitude = detailCoords.lat;
									coords.longitude = detailCoords.lng;
								}
							} catch (err) {
								// ignore detail page errors
							} finally {
								await detailPage.close();
							}
						}

						try {
							// Extract the first numeric price occurrence (handles labels like "Asking Price...")
							const rawPrice = (property.price || "").toString();
							const numMatch = rawPrice.match(/[0-9][0-9,\.\s]*/);
							const priceClean = numMatch ? numMatch[0].replace(/[^0-9]/g, "") : "";

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

							const coordsStr =
								coords.latitude && coords.longitude
									? `${coords.latitude}, ${coords.longitude}`
									: "No coords";
							console.log(`✅ ${property.title} - ${formatPrice(priceClean)} - ${coordsStr}`);
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
						}
					})
				);

				// Small delay between batches
				await new Promise((resolve) => setTimeout(resolve, 300));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}&page=${pg}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Manning Stainton - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeManningStainton();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
