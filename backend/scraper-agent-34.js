// Strutt & Parker scraper using Playwright with Crawlee
// Agent ID: 34
// Uses button-based navigation instead of URL page numbers
//
// Usage:
// node backend/scraper-agent-34.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 34;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for property types
const PROPERTY_TYPES = [
	{
		urlPath: "properties/residential/for-sale/london",
		isRental: false,
		label: "SALES",
	},
	{
		urlPath: "properties/residential/to-rent/london",
		isRental: true,
		label: "LETTINGS",
	},
];

// Number of pages to process (changed per request)
const PAGES_TO_DO = 2;

async function scrapeStruttAndParker() {
	console.log(`\n🚀 Starting Strutt & Parker scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
			},
		},

		async requestHandler({ page, request }) {
			const { isRental, label } = request.userData;

			// Processing listing with button-based pagination
			console.log(`📋 ${label} - Starting from: ${request.url}`);

			// Navigate to page 2 directly
			console.log(`🔗 Navigating to page 2...`);
			await page.goto(`${request.url}&page=2`, { waitUntil: "domcontentloaded", timeout: 30000 });
			await page.waitForTimeout(2000);

			let currentPage = 1;
			let hasNextPage = true;

			while (hasNextPage && currentPage <= PAGES_TO_DO) {
				console.log(`\n📄 Processing page ${currentPage}...`);

				// For page 2, use manual scrolling
				if (currentPage === 2) {
					console.log(`🔄 Page 2 - Using manual scrolling approach...`);

					// Scroll through the page to load all properties
					let previousHeight = 0;
					let scrolls = 0;
					const maxScrolls = 10;

					while (scrolls < maxScrolls) {
						const currentHeight = await page.evaluate(() => document.body.scrollHeight);

						if (currentHeight === previousHeight) {
							console.log(`✅ Reached end of page after ${scrolls} scrolls`);
							break;
						}

						console.log(`📜 Scroll ${scrolls + 1}/${maxScrolls} - Height: ${currentHeight}`);
						await page.evaluate(() => window.scrollBy(0, window.innerHeight));
						await page.waitForTimeout(1500); // Wait for lazy loading

						previousHeight = currentHeight;
						scrolls++;
					}
				}

				// Wait for properties to load
				await page.waitForTimeout(2000);

				try {
					await page.waitForSelector(".grid-columns--2 .grid-columns__item", {
						timeout: 30000,
					});
				} catch (e) {
					console.log(`⚠️ No properties found on page ${currentPage}`);
					break;
				}

				// Extract all properties from the page
				const properties = await page.$$eval(".grid-columns--2 .grid-columns__item", (cards) => {
					const results = [];
					cards.forEach((card) => {
						try {
							// Extract link from anchor tag
							const linkEl =
								card.querySelector('a[data-element="property-list-item"]') ||
								card.querySelector("a");
							let link = linkEl ? linkEl.getAttribute("href") : null;
							if (link && !link.startsWith("http")) {
								link = "https://www.struttandparker.com" + link;
							}

							// price element
							const priceEl = card.querySelector(
								".card__price, .card__price-container .card__price, .card__price"
							);

							// If price element has ::after content (non-empty), skip this property
							if (priceEl) {
								try {
									const afterContent = window
										.getComputedStyle(priceEl, "::after")
										.getPropertyValue("content");
									if (
										afterContent &&
										afterContent !== "none" &&
										afterContent !== '""' &&
										afterContent !== "''"
									) {
										// skip property
										return;
									}
								} catch (e) {
									// ignore computed style errors
								}
							}

							// Extract title
							const titleEl = card.querySelector(
								".card__heading, .card__title, h3, .card__text-content"
							);
							const title = titleEl ? titleEl.textContent.trim() : null;

							// Extract bedrooms
							const bedroomsEl = card.querySelector(".property-features__item--bed, .card__beds");
							let bedrooms = null;
							if (bedroomsEl) {
								const bedroomsText = bedroomsEl.textContent.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								if (bedroomsMatch) bedrooms = bedroomsMatch[0];
							}

							// Extract price
							let price = null;
							if (priceEl) {
								const priceText = priceEl.textContent.trim();
								const priceMatch = priceText.match(/[£€]\s*([\d,]+)/);
								if (priceMatch) price = priceMatch[1].replace(/,/g, "");
							}

							if (link && title && price) {
								results.push({ link: link, title: title, price, bedrooms });
							}
						} catch (err) {
							// Skip this card if error
						}
					});

					return results;
				});

				console.log(`🔗 Found ${properties.length} properties on page ${currentPage}`);

				// If no properties found, stop pagination
				if (properties.length === 0) {
					console.log(`⚠️ No properties found on page ${currentPage}, stopping`);
					break;
				}

				// Process each property one by one (navigate to detail page)
				for (let i = 0; i < properties.length; i++) {
					const property = properties[i];

					try {
						await page.goto(property.link, { waitUntil: "domcontentloaded", timeout: 30000 });
						await page.waitForTimeout(1000);

						let latitude = null;
						let longitude = null;

						// Try extracting coords from JSON-LD first
						try {
							const jsonLdStrings = await page.$$eval(
								'script[type="application/ld+json"]',
								(tags) => tags.map((t) => t.textContent)
							);
							for (const s of jsonLdStrings) {
								try {
									const parsed = JSON.parse(s);
									const items = Array.isArray(parsed) ? parsed : [parsed];
									for (const item of items) {
										if (!item) continue;
										if (typeof item.latitude === "number" && typeof item.longitude === "number") {
											latitude = item.latitude;
											longitude = item.longitude;
											break;
										}
										if (
											item.geo &&
											typeof item.geo.latitude === "number" &&
											typeof item.geo.longitude === "number"
										) {
											latitude = item.geo.latitude;
											longitude = item.geo.longitude;
											break;
										}
										if (
											item.address &&
											item.address.geo &&
											typeof item.address.geo.latitude === "number" &&
											typeof item.address.geo.longitude === "number"
										) {
											latitude = item.address.geo.latitude;
											longitude = item.address.geo.longitude;
											break;
										}
									}
									if (latitude && longitude) break;
								} catch (e) {
									// ignore JSON parse errors
								}
							}
						} catch (e) {
							// ignore
						}

						// Fallback: try HTML comments like other agents
						if (!latitude || !longitude) {
							const htmlContent = await page.content();
							const latMatch = htmlContent.match(/<!--property-latitude:\s*"?([0-9.-]+)"?-->/);
							const lngMatch = htmlContent.match(/<!--property-longitude:\s*"?([0-9.-]+)"?-->/);
							if (latMatch && lngMatch) {
								latitude = parseFloat(latMatch[1]);
								longitude = parseFloat(lngMatch[1]);
							}
						}

						// Debug log which table we're using
						const tableName = isRental ? "property_for_rent" : "property_for_sale";
						console.log(
							`🔍 Saving to table: ${tableName} | Rental: ${isRental} | URL: ${property.link.substring(
								0,
								60
							)}...`
						);

						// Check if property already exists in database with this agent_id
						const [existingRows] = await promisePool.query(
							`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
							[property.link.trim(), AGENT_ID]
						);

						// Also check if it exists with different agent_id
						const [otherAgentRows] = await promisePool.query(
							`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id != ?`,
							[property.link.trim(), AGENT_ID]
						);

						if (existingRows.length > 0) {
							console.log(`🔍 Property exists with our agent_id: ${AGENT_ID} - will update`);

							// Update existing property with our agent_id
							await promisePool.query(
								`UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
								[property.price, latitude, longitude, property.link.trim(), AGENT_ID]
							);
							console.log(
								`✅ Updated: ${property.link.substring(0, 50)}... | Price: £${
									property.price
								} | Coords: ${latitude}, ${longitude}`
							);
						} else if (otherAgentRows.length > 0) {
							console.log(
								`🔍 Property exists with different agent_id: ${otherAgentRows[0].agent_id} - will create new entry for agent ${AGENT_ID}`
							);

							// Create new property for our agent_id
							const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
							const logo = "property_for_sale/logo.png";
							const currentTime = new Date();

							await promisePool.query(insertQuery, [
								property.title,
								AGENT_ID,
								property.price,
								property.bedrooms,
								property.link.trim(),
								logo,
								latitude,
								longitude,
								currentTime,
								currentTime,
							]);
							console.log(
								`✅ Created: ${property.link.substring(0, 50)}... | Price: £${
									property.price
								} | Coords: ${latitude}, ${longitude}`
							);
						} else {
							console.log(`🔍 Property does not exist - will create new`);

							// Create new property
							const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
							const logo = "property_for_sale/logo.png";
							const currentTime = new Date();

							await promisePool.query(insertQuery, [
								property.title,
								AGENT_ID,
								property.price,
								property.bedrooms,
								property.link.trim(),
								logo,
								latitude,
								longitude,
								currentTime,
								currentTime,
							]);
							console.log(
								`✅ Created: ${property.link.substring(0, 50)}... | Price: £${
									property.price
								} | Coords: ${latitude}, ${longitude}`
							);
						}

						totalSaved++;
						totalScraped++;

						if (latitude && longitude) {
							console.log(`✅ ${property.title} - £${property.price} - ${latitude}, ${longitude}`);
						} else {
							console.log(`✅ ${property.title} - £${property.price} - No coords`);
						}
					} catch (error) {
						console.error(`❌ Error processing ${property.link}: ${error.message}`);
					}

					// Delay between properties
					await new Promise((resolve) => setTimeout(resolve, 500));
				}

				// Go back to listing page to click next button
				console.log(`\n⬅️ Going back to listing page...`);
				await page.goBack({ waitUntil: "domcontentloaded" });
				await page.waitForTimeout(1500);

				// Scroll to bottom to load pagination buttons
				console.log(`📜 Scrolling to bottom to load pagination...`);
				await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
				await page.waitForTimeout(1000);

				// Wait for pagination to load
				try {
					await page.waitForSelector('[data-element="listing-pager"]', { timeout: 10000 });
				} catch (e) {
					console.log(`⚠️ Pagination element not found`);
				}

				// Check if there's a next page button and click it
				if (currentPage < PAGES_TO_DO) {
					try {
						// Calculate next page number
						const nextPageId = currentPage + 1;
						console.log(`🔎 Looking for page ${nextPageId} button...`);

						// Look for button with data-id matching the next page number
						const nextButton = await page.$(`button[data-id="${nextPageId}"]`);

						if (nextButton) {
							console.log(`➡️ Clicking page ${nextPageId} button...`);
							await nextButton.click();
							await page.waitForTimeout(2000); // Wait for page to load
							currentPage++;
						} else {
							console.log(`⚠️ No page ${nextPageId} button found, stopping pagination`);
							hasNextPage = false;
						}
					} catch (error) {
						console.error(`❌ Error clicking next button: ${error.message}`);
						hasNextPage = false;
					}
				} else {
					hasNextPage = false;
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Process property types one by one
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`\n🏠 Processing ${propertyType.label} properties\n`);

		// Add the starting URL for this property type
		const requests = [
			{
				url: `https://www.struttandparker.com/${propertyType.urlPath}?rad=20`,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			},
		];

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Strutt & Parker - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
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
		await scrapeStruttAndParker();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
