// Remax scraper using Playwright with Crawlee
// Agent ID: 32
// Usage:
// node backend/scraper-agent-32.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 32;
let totalScraped = 0;
let totalSaved = 0;

// Two searches: sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://remax.co.uk/properties-for-sale/",
	// 	// 207 properties / 24 per page = 9 pages (rounded up)
	// 	totalRecords: 207,
	// 	recordsPerPage: 24,
	// 	totalPages: 9,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	suffix: "",
	// },
	{
		urlBase: "https://remax.co.uk/properties-for-rent/",
		// 59 properties / 24 per page = 3 pages (rounded up)
		totalRecords: 59,
		recordsPerPage: 24,
		totalPages: 3,
		isRental: true,
		label: "TO LET",
		suffix: "",
	},
];

async function scrapeRemax() {
	console.log(`\n🚀 Starting Remax scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 1,
		requestHandlerTimeoutSecs: 180,
		navigationTimeoutSecs: 90,

		launchContext: {
			launchOptions: {
				headless: false,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(2000);

			// Wait for property cards - grid containers grid0-grid23
			await page
				.waitForSelector("div[class*='grid'] .property-item", { timeout: 2000 })
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll("div[class*='grid'] .property-item"));

					return items
						.map((el) => {
							try {
								// Get the detail link - first link in the property item
								const linkEl = el.querySelector("a[href]");
								const link = linkEl ? "https://remax.co.uk" + linkEl.getAttribute("href") : null;

								const title = el.querySelector(".p-name a")?.textContent?.trim() || "";
								const rawPrice = el.querySelector(".f-price")?.textContent?.trim() || "";

								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) {
										const num = m[0].replace(/,/g, "");
										// Format with commas
										price = parseInt(num).toLocaleString();
									}
								}

								// Extract bedrooms, bathrooms from property-attr
								const attrText = el.querySelector(".property-attr")?.textContent?.trim() || "";
								const bedroomsMatch = attrText.match(/(\d+)\s*Bed/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;
								const bathroomsMatch = attrText.match(/(\d+)\s*Bath/);
								const bathrooms = bathroomsMatch ? bathroomsMatch[1] : null;
								const receptions = null; // Not present

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

			const batchSize = 3;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				for (const property of batch) {
					if (!property.link) continue;

					let coords = { latitude: null, longitude: null };

					try {
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 45000,
							});
							await detailPage.waitForTimeout(1500);

							// Extract latitude and longitude from script tag
							const scriptCoords = await detailPage.evaluate(() => {
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const script of scripts) {
									const text = script.textContent || "";
									// Try multiple patterns for lat/lng
									let match = text.match(
										/latLng\s*=\s*\{\s*lat:\s*([0-9.-]+),\s*lng:\s*([0-9.-]+)/
									);
									if (match) {
										return {
											latitude: parseFloat(match[1]),
											longitude: parseFloat(match[2]),
										};
									}
									match = text.match(/lat:\s*([0-9.-]+),\s*lng:\s*([0-9.-]+)/);
									if (match) {
										return {
											latitude: parseFloat(match[1]),
											longitude: parseFloat(match[2]),
										};
									}
									// Try pattern like { lat: 51.473281, lng: 0.135686 }
									match = text.match(/\{\s*lat:\s*([0-9.-]+),\s*lng:\s*([0-9.-]+)\s*\}/);
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
							await updatePriceByPropertyURL(
								property.link,
								property.price,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								coords.latitude,
								coords.longitude
							);

							totalSaved++;
							totalScraped++;

							if (coords.latitude && coords.longitude) {
								console.log(
									`✅ ${property.title} - ${property.price} - ${coords.latitude}, ${coords.longitude}`
								);
							} else {
								console.log(`✅ ${property.title} - ${property.price} - No coords`);
							}
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					} catch (err) {
						console.error(`❌ Error processing property ${property.link}: ${err?.message || err}`);
					}
				}

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
			let url;
			if (pg === 1) {
				url = propertyType.urlBase;
			} else {
				url = `${propertyType.urlBase}?page=${pg}`;
			}
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(`\n✅ Completed Remax - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

async function updateRemoveStatus(agent_id) {
	try {
		const remove_status = 1;
		await promisePool.query(
			`UPDATE property_for_sale SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
			[remove_status, agent_id]
		);
		console.log(`🧹 Removed old properties for agent ${agent_id}`);
	} catch (error) {
		console.error("Error updating remove status:", error.message);
	}
}

(async () => {
	try {
		await scrapeRemax();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
