// Humberts scraper using Playwright with Crawlee
// Agent ID: 230
// Usage:
// node backend/scraper-agent-230.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 230;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

// Two searches: sales and lettings
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.humberts.com/search/page/",
		// 182 properties / 12 per page = 16 pages (rounded up)
		totalRecords: 182,
		recordsPerPage: 12,
		totalPages: 16,
		isRental: false,
		label: "FOR SALE",
		suffix:
			"/?country=GB&department=residential-sales&tenure&address_keyword&radius=25&commercial_for_sale_to_rent&property_type&minimum_bedrooms&minimum_price&maximum_price&lat&lng",
	},
	{
		urlBase: "https://www.humberts.com/search/",
		// 3 pages for lettings
		totalRecords: 3 * 12, // approx
		recordsPerPage: 12,
		totalPages: 3,
		isRental: true,
		label: "TO LET",
		suffix:
			"?country=GB&department=residential-lettings&tenure=&address_keyword=&radius=25&commercial_for_sale_to_rent=&property_type=&minimum_bedrooms=&minimum_price=&maximum_price=&lat=&lng=",
	},
];

async function scrapeHumberts() {
	console.log(`\n🚀 Starting Humberts scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(1500);

			// Wait for property list items
			await page
				.waitForSelector("li.type-property", { timeout: 15000 })
				.catch(() => console.log(`⚠️ No property items found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll("li.type-property"));

					return items
						.map((el) => {
							try {
								// Get the detail link from h3 anchor
								const titleAnchor = el.querySelector("h3 a");
								const link = titleAnchor ? titleAnchor.href : null;

								const title = titleAnchor ? titleAnchor.textContent.trim() : "";

								// Extract price from .price div
								const rawPrice = el.querySelector(".price")?.textContent?.trim() || "";
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}

								// Extract bedrooms, bathrooms, receptions from .room divs
								const rooms = Array.from(el.querySelectorAll(".room-count")).map((s) =>
									s.textContent.trim()
								);
								const bedrooms = rooms[0] || null;
								const bathrooms = rooms[1] || null;
								const receptions = rooms[2] || null;

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

							// Extract latitude and longitude from script tag (JSON format)
							const scriptCoords = await detailPage.evaluate(() => {
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const script of scripts) {
									const text = script.textContent || "";
									// Look for GeoCoordinates JSON: "latitude":51.113699,"longitude":-0.015198
									const latMatch = text.match(/"latitude"\s*:\s*([\-0-9.]+)/i);
									const lngMatch = text.match(/"longitude"\s*:\s*([\-0-9.]+)/i);
									if (latMatch && lngMatch) {
										return {
											latitude: parseFloat(latMatch[1]),
											longitude: parseFloat(lngMatch[1]),
										};
									}
								}
								return null;
							});
							if (scriptCoords && scriptCoords.latitude && scriptCoords.longitude) {
								coords.latitude = scriptCoords.latitude;
								coords.longitude = scriptCoords.longitude;
								console.log(`  📍 Found script coords: ${coords.latitude}, ${coords.longitude}`);
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
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = propertyType.urlBase.includes("/page/")
				? `${propertyType.urlBase}${pg}/${propertyType.suffix}`
				: `${propertyType.urlBase}${propertyType.suffix}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Humberts - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeHumberts();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
