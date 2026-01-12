// Taylforths scraper using Playwright with Crawlee
// Agent ID: 225
// Usage:
// node backend/scraper-agent-225.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 225;
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
	// 	urlBase: "https://www.taylforths.co.uk/find-a-property/page/",
	// 	// 58 properties / 50 per page = 2 pages (rounded up)
	// 	totalRecords: 58,
	// 	recordsPerPage: 50,
	// 	totalPages: 2,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	suffix:
	// 		"/?address_keyword&radius=20&minimum_bedrooms&maximum_rent&maximum_price&department=residential-sales",
	// },
	{
		urlBase: "https://www.discoverpm.co.uk/find-a-property/page/",
		// Assuming similar numbers, adjust if needed
		totalRecords: 50,
		recordsPerPage: 50,
		totalPages: 1,
		isRental: true,
		label: "TO LET",
		suffix: "/",
	},
];

async function scrapeTaylforthsAndDiscoverPM() {
	console.log(`\n🚀 Starting Taylforths and Discover PM scraper (Agent ${AGENT_ID})...\n`);

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
				.waitForSelector("li.type-property", { timeout: 15000 })
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll("li.type-property"));

					return items
						.map((el) => {
							try {
								// Get the detail link
								const linkEl = el.querySelector("h3 a");
								const link = linkEl ? linkEl.href : null;

								const title = el.querySelector("h3 a")?.textContent?.trim() || "";
								const rawPrice = el.querySelector("div.price")?.textContent?.trim() || "";

								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) {
										const num = m[0].replace(/,/g, "");
										// Format with commas
										price = parseInt(num).toLocaleString();
									}
								}

								// Extract bedrooms, bathrooms, receptions
								const bedrooms =
									el.querySelector(".room-bedrooms .room-count")?.textContent?.trim() || null;
								const bathrooms =
									el.querySelector(".room-bathrooms .room-count")?.textContent?.trim() || null;
								const receptions =
									el.querySelector(".room-receptions .room-count")?.textContent?.trim() || null;

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
							// Wait for the iframe to load
							await detailPage
								.waitForSelector('iframe[id="propertyhive_locrating_all_in_one_frame"]', {
									timeout: 10000,
								})
								.catch(() => console.log("Iframe not found"));

							// Extract latitude and longitude from iframe data attributes
							const scriptCoords = await detailPage.evaluate(() => {
								const iframe = document.querySelector(
									'iframe[id="propertyhive_locrating_all_in_one_frame"]'
								);
								if (iframe) {
									const lat = iframe.getAttribute("data-lat");
									const lng = iframe.getAttribute("data-lng");
									if (lat && lng) {
										return {
											latitude: parseFloat(lat),
											longitude: parseFloat(lng),
										};
									}
								}
								// Fallback to script
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const script of scripts) {
									const text = script.textContent || "";
									const match = text.match(/new google\.maps\.LatLng\(([0-9.-]+),\s*([0-9.-]+)\);/);
									if (match) {
										return {
											latitude: parseFloat(match[1]),
											longitude: parseFloat(match[2]),
										};
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
		`\n✅ Completed Taylforths and Discover PM - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeTaylforthsAndDiscoverPM();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
