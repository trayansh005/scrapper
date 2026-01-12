// Nestseekers scraper using Playwright with Crawlee
// Agent ID: 241
// Usage:
// node backend/scraper-agent-241.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 241;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

// NestSeekers properties for United Kingdom
// Sales: 418 properties, 36 per page => 12 pages
// Rentals: 176 properties, 36 per page => 5 pages
const PROPERTY_TYPES = [
	// {
	// 	baseUrl: "https://www.nestseekers.com/Sales/united-kingdom/",
	// 	totalRecords: 418,
	// 	propertiesPerPage: 36,
	// 	isRental: false,
	// 	label: "FOR SALE - UNITED KINGDOM",
	// },
	{
		baseUrl: "https://www.nestseekers.com/Rentals/united-kingdom/",
		totalRecords: 176,
		propertiesPerPage: 36,
		isRental: true,
		label: "FOR RENT - UNITED KINGDOM",
	},
];

async function scrapeNestseekers() {
	console.log(`\n🚀 Starting Nestseekers scraper (Agent ${AGENT_ID})...\n`);

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
			const { isRental, label, pageNum } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum}`);

			await page.waitForTimeout(2000);

			// Wait for property rows to load
			await page
				.waitForSelector("tr[id]", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No property rows found`));

			await page.waitForTimeout(1000);

			const properties = await page.evaluate(() => {
				try {
					const rows = Array.from(document.querySelectorAll("tr[id]"));
					return rows
						.map((row) => {
							try {
								// Extract property ID from tr id attribute
								const propertyId = row.getAttribute("id");
								if (!propertyId) return null;

								// Extract URL from link
								const linkEl = row.querySelector("a[href]");
								if (!linkEl) return null;

								const href = linkEl.getAttribute("href");
								if (!href) return null;
								const link = href.startsWith("http") ? href : `https://www.nestseekers.com${href}`;

								// Extract title from strong tag inside link
								let title = "";
								const titleEl = row.querySelector("a strong");
								if (titleEl) {
									title = titleEl.textContent.trim();
								}

								// Extract address from h2
								let address = "";
								const addressEl = row.querySelector("h2");
								if (addressEl) {
									address = addressEl.textContent.trim().replace(/\s+/g, " ");
								}

								// Combine title and address
								const fullTitle = title + (address ? " - " + address : "");

								// Extract price - format with only numeric and commas
								let price = "";
								const priceEl = row.querySelector(".price");
								if (priceEl) {
									const priceText = priceEl.innerText.trim();
									// Extract GBP price: £413,889 or From £35,000,000 or £86,667 ($115,357)
									// We look for £ followed by numbers and commas
									const match = priceText.match(/£\s*([0-9,]{1,})/);
									if (match) {
										// Remove commas and parse as integer to reformat
										const numOnly = match[1].replace(/,/g, "");
										price = numOnly; // Keep as string of digits
									} else {
										// Fallback: try any sequence of numbers if £ is missing but requested text is there
										const fallbackMatch = priceText.match(/([0-9,]{4,})/);
										if (fallbackMatch) {
											price = fallbackMatch[1].replace(/,/g, "");
										} else if (priceText.toLowerCase().includes("request")) {
											price = "0"; // Flag for "on request"
										}
									}
								}

								// Extract bedrooms from info div
								let bedrooms = null;
								const infoDiv = row.querySelector(".info .tight");
								if (infoDiv) {
									const text = infoDiv.textContent;
									// Handle "4 BR" or "3+ bedroom" or "From 4 BR"
									const bedroomMatch = text.match(/(\d+)\+?\s*(?:BR|bedroom)/i);
									if (bedroomMatch) {
										bedrooms = bedroomMatch[1];
									}
								}

								return { link, title: fullTitle, price, bedrooms, propertyId };
							} catch (e) {
								console.error("Error parsing property row:", e);
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					console.error("Error in page evaluation:", err);
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			for (const property of properties) {
				if (!property.link) continue;

				let coords = { latitude: null, longitude: null };

				const detailPage = await page.context().newPage();
				try {
					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});
					await detailPage.waitForTimeout(1000);

					// Extract coordinates from geo attribute
					const geoCoords = await detailPage.evaluate(() => {
						try {
							// Look for element with geo attribute
							const geoEl = document.querySelector("[geo]");
							if (!geoEl) return null;

							const geoAttr = geoEl.getAttribute("geo");
							if (!geoAttr) return null;

							// Parse the geo JSON string
							const geoData = JSON.parse(geoAttr);

							if (geoData && geoData.lat && geoData.lon) {
								return {
									latitude: parseFloat(geoData.lat),
									longitude: parseFloat(geoData.lon),
								};
							}

							return null;
						} catch (e) {
							return null;
						}
					});

					if (geoCoords && geoCoords.latitude && geoCoords.longitude) {
						coords.latitude = geoCoords.latitude;
						coords.longitude = geoCoords.longitude;
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

				await new Promise((resolve) => setTimeout(resolve, 500));
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

		const requests = [];
		for (let page = 1; page <= totalPages; page++) {
			const url = page === 1 ? propertyType.baseUrl : `${propertyType.baseUrl}?page=${page}`;
			requests.push({
				url: url,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
					pageNum: page,
				},
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Nestseekers scraper - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeNestseekers();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
