// OpenRent scraper using Playwright with Crawlee
// Agent ID: 90
// Usage:
// node backend/scraper-agent-90.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 90;
let totalScraped = 0;
let totalSaved = 0;

// 6276 properties for rent in Greater London (pagination via skip parameter)
// Uses skip=0, skip=20, skip=40, etc. (20 properties per page)
const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.openrent.co.uk/properties-to-rent/greater-london?term=Greater%20London",
		totalRecords: 6276,
		propertiesPerPage: 20,
		isRental: true,
		label: "FOR RENT - GREATER LONDON",
	},
];

async function scrapeOpenRent() {
	console.log(`\n🚀 Starting OpenRent scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1, // Process one page at a time to avoid rate limiting
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		minConcurrency: 1,
		maxRequestsPerMinute: 2, // Max 2 requests per minute (30 seconds between requests)

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { isRental, label, pageNumber, totalPages } = request.userData;

			console.log(`📋 ${label} - Page ${pageNumber}/${totalPages}`);

			// Add delay before processing to respect rate limits (20+ seconds)
			await page.waitForTimeout(2000);

			// Wait for property list container to load
			await page
				.waitForSelector("#property-list", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ Property list container not found`));

			// Wait for property cards to load
			await page
				.waitForSelector("a.pli.search-property-card", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No property cards found`));

			// Give page time to fully render
			await page.waitForTimeout(1000);

			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll("a.pli.search-property-card"));
					console.log(`Found ${cards.length} cards in DOM`);
					return cards
						.map((card) => {
							try {
								// Extract property URL
								const href = card.getAttribute("href");
								if (!href) return null;
								const link = href.startsWith("/") ? `https://www.openrent.co.uk${href}` : href;

								// Extract price - try monthly first, then weekly
								let price = "";
								const priceMonthly = card.querySelector(".pim .fs-4");
								const priceWeekly = card.querySelector(".piw .fs-4");

								if (priceMonthly && priceMonthly.textContent) {
									const priceText = priceMonthly.textContent.trim();
									const m = priceText.match(/£([0-9,]+)/);
									if (m) {
										price = parseInt(m[1].replace(/,/g, "")).toLocaleString();
									}
								} else if (priceWeekly && priceWeekly.textContent) {
									const priceText = priceWeekly.textContent.trim();
									const m = priceText.match(/£([0-9,]+)/);
									if (m) {
										// Convert weekly to monthly (multiply by 52/12)
										const weeklyPrice = parseInt(m[1].replace(/,/g, ""));
										price = Math.round((weeklyPrice * 52) / 12).toLocaleString();
									}
								}

								// Extract address/title from .fs-3
								let title = "";
								const titleEl = card.querySelector(".fs-3");
								if (titleEl) {
									title = titleEl.textContent.trim().replace(/\s+/g, " ");
								}

								// Extract bedrooms/rooms from the features list
								let bedrooms = null;
								const featuresList = card.querySelector("ul.inline-list-divide");
								if (featuresList) {
									const text = featuresList.textContent;
									// Look for patterns like "1 Room Available", "2 Bed", etc.
									const roomMatch = text.match(/(\d+)\s+Room/i);
									const bedMatch = text.match(/(\d+)\s+Bed/i);
									if (roomMatch) {
										bedrooms = roomMatch[1];
									} else if (bedMatch) {
										bedrooms = bedMatch[1];
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

			console.log(`🔗 Found ${properties.length} properties on this page`);

			const batchSize = 3; // Reduced from 5 to process slower
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				console.log(
					`  Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(
						properties.length / batchSize
					)}...`
				);

				// Process properties sequentially instead of parallel to avoid rate limits
				for (const property of batch) {
					if (!property.link) continue;

					let coords = { latitude: null, longitude: null };

					const detailPage = await page.context().newPage();
					try {
						await detailPage.goto(property.link, {
							waitUntil: "domcontentloaded",
							timeout: 30000,
						});
						await detailPage.waitForTimeout(1500); // Increased delay

						// Wait for map div to load - try multiple selectors
						await detailPage
							.waitForSelector("#map, div[data-lat][data-lng]", { timeout: 7000 })
							.catch(() => null);

						// Extract coordinates from data attributes
						const mapCoords = await detailPage.evaluate(() => {
							try {
								// Try multiple selectors
								let mapDiv = document.querySelector("#map[data-lat][data-lng]");
								if (!mapDiv) {
									mapDiv = document.querySelector("#map");
								}
								if (!mapDiv) {
									mapDiv = document.querySelector("div[data-lat][data-lng]");
								}

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
						}
					} catch (err) {
						console.error(`  ⚠️ Error loading detail page: ${err.message}`);
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
								`✅ ${property.title} - £${property.price} - ${coords.latitude}, ${coords.longitude}`
							);
						} else {
							console.log(`✅ ${property.title} - £${property.price} - No coords`);
						}
					} catch (dbErr) {
						console.error(`❌ DB error for ${property.link}: ${dbErr?.message || dbErr}`);
					}
				}

			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label}`);

		const totalPages = Math.ceil(propertyType.totalRecords / propertyType.propertiesPerPage);
		console.log(`📄 Total pages to scrape: ${totalPages}`);

		// Generate all page URLs with skip parameter
		const requests = [];
		for (let page = 0; page < totalPages; page++) {
			const skip = page * propertyType.propertiesPerPage;
			const url = skip === 0 ? propertyType.baseUrl : `${propertyType.baseUrl}&skip=${skip}`;

			requests.push({
				url,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
					pageNumber: page + 1,
					totalPages,
				},
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed OpenRent scraper - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
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
		await scrapeOpenRent();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
