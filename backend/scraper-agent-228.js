// Starkings and Watson scraper using Playwright with Crawlee
// Agent ID: 228
// Usage:
// node backend/scraper-agent-228.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 228;
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
	// 	urlBase: "https://www.starkingsandwatson.co.uk/buying/property-search/page/",
	// 	// ~844 properties / 15 per page = 57 pages
	// 	totalRecords: 844,
	// 	recordsPerPage: 15,
	// 	totalPages: 57,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	suffix: "?department=sales&location&lat&lng&radius=3&min-price&max-price&bedrooms",
	// },
	{
		urlBase: "https://www.starkingsandwatson.co.uk/letting/property-search/page/",
		// 45 properties / 15 per page = 3 pages
		totalRecords: 45,
		recordsPerPage: 15,
		totalPages: 3,
		isRental: true,
		label: "TO LET",
		suffix: "",
	},
];

async function scrapeStarkingsWatson() {
	console.log(`\n🚀 Starting Starkings and Watson scraper (Agent ${AGENT_ID})...\n`);

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
				.waitForSelector(".card.inview-trigger-animation-fade-in-up-sm", { timeout: 15000 })
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(".card.inview-trigger-animation-fade-in-up-sm")
					);

					return items
						.map((el) => {
							try {
								// Check for SOLD, SOLD STC, LET, or Let Agreed labels
								const statusLabel = el.querySelector(".card__label")?.textContent?.trim() || "";
								const imageFlash = el.querySelector(".image-flash")?.textContent?.trim() || "";

								const combinedLabel = (statusLabel + " " + imageFlash).toUpperCase();

								// Exclude based on "LET", "AGREED", "SOLD", "STC"
								// We are careful not to exclude "TO LET" when checking for "LET"
								if (
									combinedLabel.includes("SOLD") ||
									combinedLabel.includes("STC") ||
									combinedLabel.includes("AGREED") ||
									combinedLabel.includes("UNDER OFFER") ||
									(combinedLabel.includes("LET") && !combinedLabel.includes("TO LET"))
								) {
									return null; // Skip this property
								}

								// Get the detail link from the card CTA or image link
								const linkEl = el.querySelector("a[href*='/property/']");
								const link = linkEl ? linkEl.href : null;

								const title = el.querySelector(".card__title")?.textContent?.trim() || "";
								const rawPrice = el.querySelector(".card__text")?.textContent?.trim() || "";

								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}

								// Extract bedrooms, bathrooms, receptions from icons
								const iconItems = Array.from(el.querySelectorAll(".icons__item"));
								const iconTexts = iconItems.map(
									(item) => item.querySelector(".icons__text")?.textContent?.trim() || null
								);

								const bedrooms = iconTexts[0] || null;
								const bathrooms = iconTexts[1] || null;
								const receptions = iconTexts[2] || null;

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

							// Extract latitude and longitude from Google Maps iframe src
							const mapCoords = await detailPage.evaluate(() => {
								// Look for Google Maps iframe - multiple selectors
								let iframe = document.querySelector("iframe[src*='google.com/maps']");
								if (!iframe) iframe = document.querySelector("iframe[src*='maps.google.com']");
								if (!iframe) iframe = document.querySelector("iframe[src*='maps/embed']");
								if (!iframe) return null;

								const src = iframe.getAttribute("src") || "";

								// Try multiple patterns
								// Pattern 1: q=lat,lng (most common)
								let m = src.match(/[?&]q=([^&,]+),([^&,]+)/);
								if (m) {
									return {
										latitude: parseFloat(m[1]),
										longitude: parseFloat(m[2]),
									};
								}

								// Pattern 2: Direct coordinates in URL (lat,lng without q=)
								m = src.match(/place\/([^,\/]+),([^,\/\?&]+)/);
								if (m) {
									return {
										latitude: parseFloat(m[1]),
										longitude: parseFloat(m[2]),
									};
								}

								// Pattern 3: center parameter
								m = src.match(/[?&]center=([^&,]+),([^&,]+)/);
								if (m) {
									return {
										latitude: parseFloat(m[1]),
										longitude: parseFloat(m[2]),
									};
								}

								// Pattern 4: ll parameter
								m = src.match(/[?&]ll=([^&,]+),([^&,]+)/);
								if (m) {
									return {
										latitude: parseFloat(m[1]),
										longitude: parseFloat(m[2]),
									};
								}

								return null;
							});

							if (mapCoords && mapCoords.latitude && mapCoords.longitude) {
								coords.latitude = mapCoords.latitude;
								coords.longitude = mapCoords.longitude;
								console.log(`  📍 Found map coords: ${coords.latitude}, ${coords.longitude}`);
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
			const url = `${propertyType.urlBase}${pg}/${propertyType.suffix}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Starkings and Watson - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeStarkingsWatson();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
