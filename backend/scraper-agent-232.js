// Richard James scraper using Playwright with Crawlee
// Agent ID: 232
// Usage:
// node backend/scraper-agent-232.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 232;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

// Two searches:
// - For Sale: 408 properties, 18 per page => 23 pages
// - To Let: 28 properties, 18 per page => 2 pages
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://richardjames.uk/search-results/page", // append /{page}/?keyword&status%5B0%5D=for-sale
	// 	totalRecords: 408,
	// 	recordsPerPage: 18,
	// 	totalPages: 23,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	suffix: "/?keyword&status%5B0%5D=for-sale",
	// },
	{
		urlBase: "https://richardjames.uk/search-results/page", // append /{page}/?keyword&status[0]=to-let&...
		totalRecords: 28,
		recordsPerPage: 18,
		totalPages: 2,
		isRental: true,
		label: "TO LET",
		suffix:
			"/?keyword&status%5B0%5D=to-let&min-price=0&max-price=2500000&bathrooms&bedrooms&property_id",
	},
];

async function scrapeRichardJames() {
	console.log(`\n🚀 Starting RichardJames scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for listing cards
			await page
				.waitForSelector(".item-listing-wrap, .item-listing-wrap-v6, .item-listing-wrap-v6.card", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No listing container found on page ${pageNum}`));

			// Extract properties
			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(
							".item-listing-wrap, .item-listing-wrap-v6, .item-listing-wrap-v6.card"
						)
					);

					return items
						.map((el) => {
							try {
								// Prefer the title anchor for the detail URL (h2.item-title a). Fall back to thumbnail anchor.
								const linkEl =
									el.querySelector("h2.item-title a") ||
									el.querySelector(".item-title a") ||
									el.querySelector(".listing-image-wrap a") ||
									el.querySelector(".rh_list_card__thumbnail a");
								const link = linkEl ? linkEl.href : null;
								const title =
									el.querySelector("h2.item-title a, .item-title a")?.textContent?.trim() || "";
								const rawPrice =
									el.querySelector(".item-price, .item-price .price")?.textContent?.trim() || "";
								// Extract numeric-only price (remove currency symbols, text and commas)
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}
								const status =
									el.querySelector(".label-status, .status")?.textContent?.trim() || "";
								const beds =
									el.querySelector(".hz-figure, .figure, .hz-figure")?.textContent?.trim() || null;

								// Skip if link missing
								if (!link) return null;

								return { link, title, price, bedrooms: beds, status };
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

			// Batch processing
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

							// Extract relevant script content: search for any script containing coordinates
							const scriptText = await detailPage.evaluate(() => {
								// Priority order: houzez map, yoast schema, then any script with coords
								const houzez = document.getElementById("houzez-single-property-map-js-extra");
								if (houzez && houzez.textContent) return houzez.textContent;

								const yoast = document.querySelector("script.yoast-schema-graph");
								if (yoast && yoast.textContent) return yoast.textContent;

								// Search any script containing lat/lng or GeoCoordinates
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const s of scripts) {
									const t = s.textContent || "";
									if (
										t.includes('"lat"') ||
										t.includes('"latitude"') ||
										t.includes("GeoCoordinates")
									) {
										return t;
									}
								}
								return null;
							});

							if (scriptText) {
								// Try multiple coordinate extraction patterns
								let found = false;

								// Pattern 1: Houzez lat/lng "lat":"51.555667","lng":"-1.798813"
								let m = scriptText.match(
									/"lat"\s*:\s*"?([\-0-9.]+)"?\s*,\s*"lng"\s*:\s*"?([\-0-9.]+)"?/
								);
								if (m) {
									coords = { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };
									found = true;
								}

								// Pattern 2: Yoast GeoCoordinates
								if (!found) {
									m = scriptText.match(
										/"@type"\s*:\s*"GeoCoordinates"[\s\S]*?"latitude"\s*:\s*"?([\-0-9.]+)"?[\s\S]*?"longitude"\s*:\s*"?([\-0-9.]+)"?/
									);
									if (m) {
										coords = { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };
										found = true;
									}
								}

								// Pattern 3: Alternative lat/longitude (with colon) "latitude":"51.5337446"
								if (!found) {
									m = scriptText.match(
										/"latitude"\s*:\s*"?([\-0-9.]+)"?[\s\S]*?"longitude"\s*:\s*"?([\-0-9.]+)"?/
									);
									if (m) {
										coords = { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };
										found = true;
									}
								}

								// Pattern 4: Simple fallback for any lat,lng pattern (no quotes)
								if (!found) {
									m = scriptText.match(/lat[^\d]+([\-0-9.]+)[\s\S]{1,50}lng[^\d]+([\-0-9.]+)/i);
									if (m) {
										coords = { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };
										found = true;
									}
								}
							}
						} catch (err) {
							// ignore
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
		// Both For Sale and To Let use /page/{pg}/ + suffix pattern
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}/${pg}/${propertyType.suffix}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed RichardJames - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeRichardJames();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
