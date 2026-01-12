// Foxtons scraper using Playwright with Crawlee
// Agent ID: 50
// Usage:
// node backend/scraper-agent-50.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 50;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for sales and rentals
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.foxtons.co.uk/properties-for-sale/south-east-england",
	// 	totalPages: 69, // 6812 properties / 100 per page = 68.12, round up to 69
	// 	recordsPerPage: 100,
	// 	isRental: false,
	// 	label: "SALES",
	// 	params: "?order_by=price_desc&radius=5&available_for_auction=0&sold=0",
	// },
	{
		urlBase: "https://www.foxtons.co.uk/properties-to-rent/south-east-england",
		totalPages: 22, // 2106 properties / 100 per page = 21.06, round up to 22
		recordsPerPage: 100,
		isRental: true,
		label: "RENTALS",
		params: "?order_by=price_desc&expand=5&sold=0",
	},
];

async function scrapeFoxtons() {
	console.log(`\n🚀 Starting Foxtons scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for page content to populate
			await page.waitForTimeout(2000);
			await page.waitForSelector("[data-id]", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No property container found on page ${pageNum}`);
			});

			// Extract properties from the DOM
			const properties = await page.evaluate((isRental) => {
				const containers = Array.from(document.querySelectorAll("[data-id]"));
				return containers.map((container) => {
					// Get property link
					const linkEl = container.querySelector("a[href*='/properties-']");
					const link = linkEl ? linkEl.href : null;

					// Get address
					const address = container.querySelector(".addressText")?.textContent?.trim() || "";

					// Get price
					let priceText = "";
					if (isRental) {
						// For rentals, try to get monthly rent first
						const monthlyRentEl = container.querySelector(".monthly-rent");
						if (monthlyRentEl) {
							priceText = monthlyRentEl.textContent?.trim() || "";
						} else {
							priceText = container.querySelector(".MuiTypography-h4")?.textContent?.trim() || "";
						}
					} else {
						priceText = container.querySelector(".MuiTypography-h4")?.textContent?.trim() || "";
					}

					// Format price: extract first numeric value with commas only
					let priceClean = "";
					if (priceText) {
						const priceMatch = priceText.match(/[\d,]+/);
						priceClean = priceMatch ? priceMatch[0] : "";
					}

					return {
						link,
						title: address,
						price: priceClean,
						bedrooms: null, // Will extract from detail page
					};
				});
			}, isRental);

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process properties in small batches
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						// Ensure absolute URL
						if (!property.link) return;

						let coords = { latitude: null, longitude: null };

						// Visit detail page to extract coordinates
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							// Extract details from detail page (Coordinates, Bedrooms, and Monthly Rent if applicable)
							const detailInfo = await detailPage.evaluate((isRental) => {
								let result = {
									coords: { latitude: null, longitude: null },
									bedrooms: null,
									monthlyRent: null,
								};

								try {
									// 1. Extract coordinates from structured data
									const content = document.documentElement.outerHTML;
									const geoMatch = content.match(
										/"@type":"GeoCoordinates","latitude":([\d.-]+),"longitude":([\d.-]+)/
									);
									if (geoMatch) {
										result.coords.latitude = parseFloat(geoMatch[1]);
										result.coords.longitude = parseFloat(geoMatch[2]);
									}

									// 2. Extract bedrooms
									const bedElements = document.querySelectorAll(".MuiTypography-body1.iconText");
									for (const bedEl of bedElements) {
										const bedText = bedEl.textContent?.trim() || "";
										if (bedText.includes("Bed")) {
											const bedMatch = bedText.match(/(\d+)/);
											if (bedMatch) {
												result.bedrooms = parseInt(bedMatch[1]);
												break;
											}
										}
									}

									// 3. Extract monthly rent for rentals
									if (isRental) {
										const monthlyRentEl = document.querySelector(".monthly-rent");
										if (monthlyRentEl) {
											result.monthlyRent = monthlyRentEl.textContent?.trim() || null;
										}
									}
								} catch (e) {
									// Silently fail within browser context
								}
								return result;
							}, isRental);

							coords = detailInfo.coords;

							if (detailInfo.bedrooms !== null) {
								property.bedrooms = detailInfo.bedrooms;
							}

							// If we found a monthly rent on the detail page, use it over the listing page price
							if (isRental && detailInfo.monthlyRent) {
								const rentMatch = detailInfo.monthlyRent.match(/[\d,]+/);
								if (rentMatch) {
									property.price = rentMatch[0];
								}
							}
						} catch (err) {
							console.error(`⚠️ Failed to extract details for ${property.link}: ${err.message}`);
						} finally {
							await detailPage.close();
						}

						// Format price: keep only numeric and commas
						const priceClean = (property.price || "").replace(/[^0-9,]/g, "").trim();

						// Determine which DB table to use
						const tableName = isRental ? "property_for_rent" : "property_for_sale";

						try {
							// Check if property exists for this agent
							const [existingRows] = await promisePool.query(
								`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
								[property.link.trim(), AGENT_ID]
							);

							if (existingRows.length > 0) {
								// Update existing record for this agent
								await promisePool.query(
									`UPDATE ${tableName} SET price = ?, bedrooms = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
									[
										priceClean || null,
										property.bedrooms || null,
										coords.latitude,
										coords.longitude,
										property.link.trim(),
										AGENT_ID,
									]
								);
								console.log(`✅ Updated existing property for agent ${AGENT_ID}: ${property.link}`);
							} else {
								// Create brand new record
								const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
								const logo = isRental ? "property_for_rent/logo.png" : "property_for_sale/logo.png";
								const currentTime = new Date();
								await promisePool.query(insertQuery, [
									property.title || "",
									AGENT_ID,
									priceClean || null,
									property.bedrooms || null,
									property.link.trim(),
									logo,
									coords.latitude,
									coords.longitude,
									currentTime,
									currentTime,
								]);
								console.log(`🆕 Inserted property for agent ${AGENT_ID}: ${property.link}`);
							}

							totalSaved++;
							totalScraped++;
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
						}
					})
				);

				// Small delay between batches
				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue all listing pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			// Construct page URL
			const url =
				pg === 1
					? `${propertyType.urlBase}${propertyType.params}`
					: `${propertyType.urlBase}${propertyType.params}&page=${pg}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Foxtons - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
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
		await scrapeFoxtons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
