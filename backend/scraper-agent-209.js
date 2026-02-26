// FrostWeb scraper using Playwright with Crawlee
// Agent ID: 209
// Usage:
// node backend/scraper-agent-209.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 209;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

// Configuration for sales and lettings on FrostWeb
const PROPERTY_TYPES = [
	// {
	// 	// Sales
	// 	urlBase:
	// 		"https://www.frostweb.co.uk/search/?showstc=+off&showsold=off&department=%21commercial&instruction_type=Sale&ajax_polygon=&ajax_radius=&minprice=&maxprice=",
	// 	totalPages: 62, // 491 properties, 8 per page -> 62 pages
	// 	recordsPerPage: 8,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		// Lettings
		urlBase:
			"https://www.frostweb.co.uk/search/?showstc=+off&showsold=off&department=%21commercial&instruction_type=Letting&ajax_polygon=&ajax_radius=&minprice=&maxprice=",
		totalPages: 13, // 98 properties, 8 per page -> 13 pages
		recordsPerPage: 8,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeFrostWeb() {
	console.log(`\n🚀 Starting FrostWeb scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for page content
			await page.waitForTimeout(1200);
			await page.waitForSelector("#search-results", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No #search-results container on page ${pageNum}`);
			});

			// Extract property list from DOM
			const properties = await page.evaluate(() => {
				try {
					const container = document.querySelector("#search-results");
					if (!container) return [];
					const items = Array.from(container.querySelectorAll(".row.thing"));
					return items
						.map((el) => {
							const linkEl = el.querySelector(".col-sm-4 a");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.frostweb.co.uk" + href
								: null;

							const title = el.querySelector("h3")?.textContent?.trim() || "";

							// Price is inside h4 text (may include labels)
							const h4 = el.querySelector("h4");
							const priceText = h4 ? h4.textContent.replace(/\n|\r/g, " ").trim() : "";
							const priceMatch = priceText.match(/£[0-9,]+/);
							const price = priceMatch ? priceMatch[0] : priceText;

							const bedrooms = el.querySelector(".property-bedrooms")?.textContent?.trim() || null;

							return { link, price, title, bedrooms, lat: null, lng: null };
						})
						.filter((p) => p.link);
				} catch (e) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process properties in small batches
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						let coords = { latitude: property.lat || null, longitude: property.lng || null };

						// If no coords, visit detail page to extract JSON-LD geo
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
										const scripts = Array.from(
											document.querySelectorAll('script[type="application/ld+json"]')
										);
										for (const s of scripts) {
											try {
												const data = JSON.parse(s.textContent);
												if (data && data.geo && data.geo.latitude && data.geo.longitude) {
													return { lat: data.geo.latitude, lng: data.geo.longitude };
												}
											} catch (e) {
												// continue
											}
										}

										// Last resort: regex search for latitude/longitude in scripts
										const allScripts = Array.from(document.querySelectorAll("script"))
											.map((s) => s.textContent)
											.join("\n");
										const latMatch =
											allScripts.match(/"latitude"\s*:\s*"?([0-9.+-]+)"?/i) ||
											allScripts.match(/"lat"\s*:\s*([0-9.+-]+)/i);
										const lngMatch =
											allScripts.match(/"longitude"\s*:\s*"?([0-9.+-]+)"?/i) ||
											allScripts.match(/"lng"\s*:\s*([0-9.+-]+)/i);
										if (latMatch && lngMatch) {
											return { lat: parseFloat(latMatch[1]), lng: parseFloat(lngMatch[1]) };
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
							const priceClean = (property.price || "").replace(/[£,\s]/g, "").trim();

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
				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue listing pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 11; pg <= propertyType.totalPages; pg++) {
			// For FrostWeb page 1 uses the base URL with query string, subsequent pages use /search/{n}.html?
			let url;
			if (pg === 1) {
				url = propertyType.urlBase;
			} else {
				url = propertyType.urlBase.replace("/search/", `/search/${pg}.html?`);
			}

			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed FrostWeb - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeFrostWeb();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
