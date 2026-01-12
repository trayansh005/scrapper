// Abbey Sales and Lettings Group scraper using Playwright with Crawlee
// Agent ID: 227
// Usage:
// node backend/scraper-agent-227.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 227;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Two searches: sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://abbeysalesandlettingsgroup.co.uk/search/page/",
	// 	// 133 properties / 10 per page = 14 pages (rounded up)
	// 	totalRecords: 133,
	// 	recordsPerPage: 10,
	// 	totalPages: 14,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	suffix:
	// 		"/?address_keyword&department=residential-sales&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&officeID&availability=2",
	// },
	{
		urlBase: "https://abbeysalesandlettingsgroup.co.uk/search/page/",
		// 65 properties / 10 per page = 7 pages (rounded up)
		totalRecords: 65,
		recordsPerPage: 10,
		totalPages: 7,
		isRental: true,
		label: "TO LET",
		suffix:
			"/?address_keyword=&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=&officeID=&availability=6",
	},
];

async function scrapeAbbeySalesLettings() {
	console.log(`\n🚀 Starting Abbey Sales and Lettings Group scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(2000);

			// Wait for property cards
			await page
				.waitForSelector(".properties-block .grid-box", { timeout: 15000 })
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll(".properties-block .grid-box"));

					return items
						.map((el) => {
							try {
								// Get the detail link
								const linkEl = el.querySelector("a[href*='/property/']");
								const link = linkEl ? linkEl.href : null;

								const title = el.querySelector("h4")?.textContent?.trim() || "";
								const rawPrice =
									el.querySelector("h5.property-archive-price")?.textContent?.trim() || "";

								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) {
										const num = m[0].replace(/,/g, "");
										// Format with commas
										price = parseInt(num).toLocaleString();
									}
								}

								// Extract bedrooms, receptions, bathrooms from ul.property-types li
								const typeItems = Array.from(el.querySelectorAll("ul.property-types li"));
								let bedrooms = null;
								let receptions = null;
								let bathrooms = null;
								typeItems.forEach((li) => {
									const span = li.querySelector("span");
									const icon = li.querySelector("i");
									if (icon && icon.classList.contains("fa-bed")) {
										bedrooms = span ? span.textContent.trim() : null;
									} else if (icon && icon.classList.contains("fa-couch")) {
										receptions = span ? span.textContent.trim() : null;
									} else if (icon && icon.classList.contains("fa-bath")) {
										bathrooms = span ? span.textContent.trim() : null;
									}
								});

								if (!link) return null;

								return { link, title, price, bedrooms, bathrooms, receptions };
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

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						let coords = { latitude: null, longitude: null };

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							// Extract latitude and longitude from script tag
							const scriptCoords = await detailPage.evaluate(() => {
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const script of scripts) {
									const text = script.textContent || "";
									// Pattern 1: Google Maps LatLng constructor
									const m1 = text.match(
										/var myLatlng = new google\.maps\.LatLng\(([0-9.-]+),\s*([0-9.-]+)\);/
									);
									if (m1) {
										return { latitude: parseFloat(m1[1]), longitude: parseFloat(m1[2]) };
									}

									// Pattern 2: Explicit parseFloat assignments e.g. const lat = parseFloat('51.781799'); const lng = parseFloat('0.660231');
									// Capture lat first
									const mLat = text.match(/const\s+lat\s*=\s*parseFloat\(['"]([0-9.-]+)['"]\)/);
									const mLng = text.match(/const\s+lng\s*=\s*parseFloat\(['"]([0-9.-]+)['"]\)/);
									if (mLat && mLng) {
										return { latitude: parseFloat(mLat[1]), longitude: parseFloat(mLng[1]) };
									}

									// Pattern 3: window/mapData JSON with lat/lng keys
									const mJsonBlock = text.match(
										/\{[^{}]*\b(lat|latitude)\b[^{}]*\b(lng|longitude)\b[^{}]*\}/
									);
									if (mJsonBlock) {
										try {
											const jsonLike = mJsonBlock[0]
												.replace(/([a-zA-Z0-9_]+)\s*:/g, '"$1":') // ensure quoted keys
												.replace(/'/g, '"');
											const parsed = JSON.parse(jsonLike);
											const latVal = parsed.lat || parsed.latitude;
											const lngVal = parsed.lng || parsed.longitude;
											if (latVal && lngVal) {
												return { latitude: parseFloat(latVal), longitude: parseFloat(lngVal) };
											}
										} catch (_) {
											// ignore JSON parse errors
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
							// ignore
						} finally {
							await detailPage.close();
						}

						try {
							const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;

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

							console.log(
								`✅ ${property.title} - ${formatPrice(priceClean)} - ${
									coords.latitude && coords.longitude
										? `${coords.latitude}, ${coords.longitude}`
										: "No coords"
								}`
							);
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					})
				);

				await new Promise((resolve) => setTimeout(resolve, 200));
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
			const url = `${propertyType.urlBase}${pg}${propertyType.suffix}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Abbey Sales and Lettings Group - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeAbbeySalesLettings();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
