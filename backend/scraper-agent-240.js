// Ashtons scraper using Playwright with Crawlee
// Agent ID: 240
// Usage:
// node backend/scraper-agent-240.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 240;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

// 358 records for sale, 12 per page => 30 pages (loads via "Show more" button)
// 30 records for letting, 12 per page => 3 pages
const PROPERTY_TYPES = [
	{
		url: "https://www.ashtons.co.uk/buy?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
		totalRecords: 358,
		recordsPerPage: 12,
		isRental: false,
		label: "FOR SALE",
	},
	{
		url: "https://www.ashtons.co.uk/rent?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
		totalRecords: 30,
		recordsPerPage: 12,
		isRental: true,
		label: "FOR LETTING",
	},
];

async function scrapeAshtons() {
	console.log(`\n🚀 Starting Ashtons scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 600,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { isRental, label, url } = request.userData;

			console.log(`📋 ${label} - ${url}`);

			await page.waitForTimeout(2000);

			// Wait for property cards to load
			await page
				.waitForSelector(".c-property-card", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No property cards found`));

			// Keep clicking "Show more" button until all properties are loaded
			let clickCount = 0;
			const maxClicks = 50; // Safety limit

			while (clickCount < maxClicks) {
				try {
					// Check if "Show more" button exists and is enabled
					const showMoreButton = await page.$(
						".c-property-search__list-action button.c-button--tertiary"
					);

					if (!showMoreButton) {
						console.log(`  ℹ️ No more "Show more" button found after ${clickCount} clicks`);
						break;
					}

					const isDisabled = await showMoreButton.evaluate((el) => el.disabled);
					if (isDisabled) {
						console.log(`  ℹ️ "Show more" button is disabled after ${clickCount} clicks`);
						break;
					}

					console.log(`  🔄 Clicking "Show more" button (click ${clickCount + 1})...`);

					// Click using JavaScript to avoid timeout issues
					await page.evaluate(() => {
						const button = document.querySelector(
							".c-property-search__list-action button.c-button--tertiary"
						);
						if (button) button.click();
					});

					// Wait for new properties to load
					await page.waitForTimeout(2000);

					// Wait for loading state to finish
					await page
						.waitForFunction(
							() => {
								const button = document.querySelector(
									".c-property-search__list-action button.c-button--tertiary"
								);
								return button && !button.classList.contains("is-waiting");
							},
							{ timeout: 10000 }
						)
						.catch(() => {});

					await page.waitForTimeout(500);
					clickCount++;
				} catch (err) {
					console.log(`  ⚠️ Error clicking "Show more": ${err.message}`);
					break;
				}
			}

			console.log(`  ✅ Finished loading properties after ${clickCount} clicks`);

			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll(".c-property-card"));
					return cards
						.map((card) => {
							try {
								// Extract property URL
								const linkEl = card.querySelector("a.c-property-card__anchor");
								if (!linkEl) return null;

								const href = linkEl.getAttribute("href");
								if (!href) return null;
								const link = href.startsWith("/") ? `https://www.ashtons.co.uk${href}` : href;

								// Extract price
								let price = "";
								const priceEl = card.querySelector(".c-property-price__value");
								if (priceEl) {
									const priceText = priceEl.textContent.trim();
									const m = priceText.match(/£([0-9,]+)/);
									if (m) {
										price = parseInt(m[1].replace(/,/g, "")).toLocaleString();
									}
								}

								// Extract address/title from h2
								let title = "";
								const titleEl = card.querySelector(".c-property-card__title");
								if (titleEl) {
									title = titleEl.textContent.trim().replace(/\s+/g, " ");
								}

								// Extract bedrooms
								let bedrooms = null;
								const bedroomsFeature = card.querySelector(
									".c-property-feature--bedrooms .c-property-feature__value"
								);
								if (bedroomsFeature) {
									const text = bedroomsFeature.textContent.trim();
									const match = text.match(/(\d+)/);
									if (match) {
										bedrooms = match[1];
									}
								}

								return { link, title, price, bedrooms };
							} catch (e) {
								console.error("Error parsing property card:", e);
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					console.error("Error in page evaluation:", err);
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties total`);

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
							await detailPage.waitForTimeout(800);

							// Wait for map div to load
							await detailPage
								.waitForSelector(".c-location-map.js-location-map", { timeout: 7000 })
								.catch(() => null);

							// Extract coordinates from data attributes
							const mapCoords = await detailPage.evaluate(() => {
								try {
									const mapDiv = document.querySelector(".c-location-map.js-location-map");
									if (!mapDiv) return null;

									const lat = mapDiv.getAttribute("data-lat");
									const lng = mapDiv.getAttribute("data-lng");

									if (lat && lng) {
										return {
											latitude: parseFloat(lat),
											longitude: parseFloat(lng),
										};
									}

									return null;
								} catch (e) {
									return null;
								}
							});

							if (mapCoords && mapCoords.latitude && mapCoords.longitude) {
								coords.latitude = mapCoords.latitude;
								coords.longitude = mapCoords.longitude;
								console.log(`  📍 Found coords: ${coords.latitude}, ${coords.longitude}`);
							}
						} catch (err) {
							console.error(`  ⚠️ Error loading detail page: ${err.message}`);
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
		console.log(`🏠 Processing ${propertyType.label}`);

		await crawler.addRequests([
			{
				url: propertyType.url,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
					url: propertyType.url,
				},
			},
		]);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Ashtons scraper - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeAshtons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
