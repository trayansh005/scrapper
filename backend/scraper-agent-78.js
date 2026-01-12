// Robert Holmes scraper using Playwright with Crawlee
// Agent ID: 78
// Usage:
// node backend/scraper-agent-78.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 78;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://robertholmes.co.uk/search/page/",
	// 	params: "?address_keyword&department=residential-sales&availability=2",
	// 	totalPages: 8,
	// 	recordsPerPage: 12,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlBase: "https://robertholmes.co.uk/search/page/",
		params:
			"?address_keyword&department=residential-lettings&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&availability=6",
		totalPages: 2,
		recordsPerPage: 12,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeRobertHolmes() {
	console.log(`\n🚀 Starting Robert Holmes scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for page content to populate
			await page.waitForTimeout(1500);
			await page.waitForSelector(".grid-box-card", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			// Extract properties from listing page
			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll(".grid-box-card"));
					return items
						.map((el) => {
							try {
								const linkEl = el.querySelector("a");
								let link = linkEl ? linkEl.getAttribute("href") : null;
								if (link && !link.startsWith("http")) {
									link = "https://robertholmes.co.uk" + link;
								}

								const title =
									el.querySelector(".property-archive-title h4")?.textContent?.trim() || "";

								const bedroomsText =
									el.querySelector(".icons-list li span")?.textContent?.trim() || "";
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText =
									el.querySelector(".property-archive-price")?.textContent?.trim() || "";
								// Extract price: match £ followed by digits and commas only
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								return { link, price, title, bedrooms, lat: null, lng: null };
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p && p.link);
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

						// Visit detail page to extract coordinates from GeoCoordinates JSON
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							// Extract coordinates from GeoCoordinates JSON
							const detailCoords = await detailPage.evaluate(() => {
								try {
									// Look for GeoCoordinates JSON-LD data
									const scripts = Array.from(
										document.querySelectorAll('script[type="application/ld+json"]')
									);
									for (const s of scripts) {
										try {
											const data = JSON.parse(s.textContent);
											if (
												data &&
												data["@type"] === "GeoCoordinates" &&
												data.latitude &&
												data.longitude
											) {
												// Check for valid coordinates (not 0.000013, 0.000013 dummy values)
												if (Math.abs(data.latitude) > 0.1 && Math.abs(data.longitude) > 0.1) {
													return { lat: data.latitude, lng: data.longitude };
												}
											}
										} catch (e) {
											// continue
										}
									}

									// Regex search for GeoCoordinates pattern
									const allScripts = Array.from(document.querySelectorAll("script"))
										.map((s) => s.textContent)
										.join("\n");

									// Try multiple regex patterns for coordinates
									const geoMatch = allScripts.match(
										/"@type":"GeoCoordinates","latitude":([0-9e.-]+),"longitude":([0-9e.-]+)/
									);
									if (geoMatch) {
										const lat = parseFloat(geoMatch[1]);
										const lng = parseFloat(geoMatch[2]);
										// Check for valid coordinates
										if (Math.abs(lat) > 0.1 && Math.abs(lng) > 0.1) {
											return { lat, lng };
										}
									}

									// Try alternative pattern with spaces
									const geoMatch2 = allScripts.match(
										/"latitude"\s*:\s*([0-9e.-]+)\s*,\s*"longitude"\s*:\s*([0-9e.-]+)/
									);
									if (geoMatch2) {
										const lat = parseFloat(geoMatch2[1]);
										const lng = parseFloat(geoMatch2[2]);
										if (Math.abs(lat) > 0.1 && Math.abs(lng) > 0.1) {
											return { lat, lng };
										}
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
							console.log(`⚠️ Could not fetch detail page for ${property.link}: ${err.message}`);
						} finally {
							await detailPage.close();
						}

						// Determine which DB table to use
						const tableName = isRental ? "property_for_rent" : "property_for_sale";

						try {
							// Clean price: extract only numbers (e.g., "£47,666pcm" → "47666")
							const priceClean = property.price
								? property.price.replace(/[^0-9]/g, "").trim()
								: null;

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

	// Enqueue all listing pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}${pg}/${propertyType.params}`;
			await crawler.addRequests([
				{
					url,
					userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
				},
			]);
		}
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Robert Holmes - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeRobertHolmes();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
