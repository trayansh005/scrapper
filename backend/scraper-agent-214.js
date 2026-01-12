// Mortimers scraper using Playwright with Crawlee
// Agent ID: 214
// Usage:
// node backend/scraper-agent-214.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 214;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

// Configuration for Mortimers
// 10 properties per page; sales 21 pages, rent 2 pages
const PROPERTY_TYPES = [
	// {
	// 	// Sales
	// 	urlBase: "https://mortimers-property.co.uk/properties-for-sale/All?excludeSstc=1",
	// 	totalPages: 21,
	// 	recordsPerPage: 10,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		// Rentals
		urlBase: "https://mortimers-property.co.uk/properties-to-rent/All?excludeSstc=1",
		totalPages: 2,
		recordsPerPage: 10,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeMortimers() {
	console.log(`\n🚀 Starting Mortimers scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(800);

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
									: "https://mortimers-property.co.uk" + href
								: null;

							const title =
								el.querySelector(".SearchResultCard_title__STErD h3")?.textContent?.trim() ||
								el.querySelector("h3")?.textContent?.trim() ||
								"";
							const address =
								el.querySelector(".SearchResultCard_address__NMzbh")?.textContent?.trim() || "";
							const price =
								el.querySelector('[class*="SearchResultCard_price"]')?.textContent?.trim() || "";

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
								await detailPage.waitForTimeout(400);

								const detailCoords = await detailPage.evaluate(() => {
									try {
										const scripts = Array.from(
											document.querySelectorAll('script[type="application/ld+json"]')
										);
										for (const s of scripts) {
											try {
												const data = JSON.parse(s.textContent);
												// geolocation or geo inside object
												if (
													data &&
													data.geolocation &&
													(data.geolocation.latitude || data.geolocation.longitude)
												) {
													let lat = parseFloat(data.geolocation.latitude);
													let lng = parseFloat(data.geolocation.longitude);
													if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
												}
												if (data && data.geo && (data.geo.latitude || data.geo.longitude)) {
													let lat = parseFloat(data.geo.latitude);
													let lng = parseFloat(data.geo.longitude);
													if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
												}
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

										// Fallback to searching all scripts for lat/lng pairs
										const all = Array.from(document.querySelectorAll("script"))
											.map((s) => s.textContent)
											.join("\n");
										const latMatch =
											all.match(/"latitude"\s*:\s*([0-9.+-]+)/i) ||
											all.match(/"lat"\s*:\s*([0-9.+-]+)/i);
										const lngMatch =
											all.match(/"longitude"\s*:\s*([0-9.+-]+)/i) ||
											all.match(/"lng"\s*:\s*([0-9.+-]+)/i);
										if (latMatch && lngMatch) {
											let lat = parseFloat(latMatch[1]);
											let lng = parseFloat(lngMatch[1]);
											// Heuristic: some sites invert lat/lng for UK values (example: latitude:-2.15, longitude:53.55)
											if (
												Math.abs(lat) <= 10 &&
												lng >= 49 &&
												lng <= 61 &&
												!(lat >= 49 && lat <= 61 && Math.abs(lng) <= 10)
											) {
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
									let lat = detailCoords.lat;
									let lng = detailCoords.lng;
									// Heuristic for inverted coordinates
									if (
										Math.abs(lat) <= 10 &&
										lng >= 49 &&
										lng <= 61 &&
										!(lat >= 49 && lat <= 61 && Math.abs(lng) <= 10)
									) {
										const t = lat;
										lat = lng;
										lng = t;
									}
									coords.latitude = lat;
									coords.longitude = lng;
								}
							} catch (err) {
								// ignore detail page errors
							} finally {
								await detailPage.close();
							}
						}

						try {
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
		`\n✅ Completed Mortimers - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeMortimers();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
