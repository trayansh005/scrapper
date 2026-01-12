// Mistoria Estate Agents scraper using Playwright with Crawlee
// Agent ID: 224
// Usage:
// node backend/scraper-agent-224.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 224;
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
	{
		urlBase: "https://mistoriaestateagents.co.uk/property-search/page/",
		// 57 properties / 12 per page = 5 pages (rounded up)
		totalRecords: 57,
		recordsPerPage: 12,
		totalPages: 5,
		isRental: false,
		label: "FOR SALE",
		suffix:
			"/?address_keyword&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&department=residential-sales&availability&maximum_bedrooms",
	},
	{
		urlBase: "https://mistoriaestateagents.co.uk/property-search/page/",
		// 69 properties / 12 per page = 6 pages (rounded up)
		totalRecords: 69,
		recordsPerPage: 12,
		totalPages: 6,
		isRental: true,
		label: "TO LET",
		suffix:
			"/?address_keyword=&department=residential-lettings&availability=&minimum_bedrooms=&maximum_bedrooms=",
	},
];

async function scrapeMistoriaEstateAgents() {
	console.log(`\n🚀 Starting Mistoria Estate Agents scraper (Agent ${AGENT_ID})...\n`);

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
								// Check for Sold, Sold STC, Let Agreed flags
								const flagEl = el.querySelector(".flag");
								if (flagEl) {
									const flagText = flagEl.textContent.trim();
									if (flagText.includes("Sold") || flagText.includes("Let Agreed")) {
										return null; // Skip this property
									}
								}

								// Get the detail link
								const linkEl = el.querySelector(".thumbnail a");
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

							// Extract latitude and longitude from script tag
							const scriptCoords = await detailPage.evaluate(() => {
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
		`\n✅ Completed Mistoria Estate Agents - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeMistoriaEstateAgents();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
