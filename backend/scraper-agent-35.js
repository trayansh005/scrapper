// Guild Property scraper using Playwright with Crawlee
// Agent ID: 35
//
// Usage:
// node backend/scraper-agent-35.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 35;
let totalScraped = 0;
let totalSaved = 0;
// Limit for quick runs / testing

// Start page and number of pages to process (changed per request)
const START_PAGE = 1;

// Search URL templates (page number replaced). Provided search URLs in the request.
const PROPERTY_TYPES = [
	// {
	// 	// Sales (p_department=RS)
	// 	urlBase:
	// 		"https://www.guildproperty.co.uk/search?page=1&national=false&p_department=RS&p_division=&location=London&auto-lat=&auto-lng=&keywords=&minimumPrice=&minimumRent=&maximumPrice=&maximumRent=&rentFrequency=&minimumBedrooms=&maximumBedrooms=&searchRadius=50&recentlyAdded=&propertyIDs=&propertyType=&rentType=&orderBy=&networkID=&clientID=&officeID=&availability=1&propertyAge=&prestigeProperties=&includeDisplayAddress=Yes&videoettesOnly=0&360TourOnly=0&virtualTourOnly=0&country=&addressNumber=&equestrian=0&tag=&golfGroup=&coordinates=&priceAltered=&sfonly=0&openHouse=0&student=&isArea=false&limit=20",
	// 	totalRecords: 9535,
	// 	recordsPerPage: 20,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		// Rent (p_department=RL)
		urlBase:
			"https://www.guildproperty.co.uk/search?page=1&national=false&p_department=RL&p_division=&location=London&auto-lat=&auto-lng=&keywords=&minimumPrice=&minimumRent=&maximumPrice=&maximumRent=&rentFrequency=&minimumBedrooms=&maximumBedrooms=&searchRadius=50&recentlyAdded=&propertyIDs=&propertyType=&rentType=&orderBy=&networkID=&clientID=&officeID=&availability=1&propertyAge=&prestigeProperties=&includeDisplayAddress=Yes&videoettesOnly=0&360TourOnly=0&virtualTourOnly=0&country=&addressNumber=&equestrian=0&tag=&golfGroup=&coordinates=&priceAltered=&sfonly=0&openHouse=0&student=&isArea=false&limit=20",
		totalRecords: 1704,
		recordsPerPage: 20,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeGuildProperty() {
	console.log(`\n🚀 Starting GuildProperty scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
			},
		},

		async requestHandler({ page, request, crawler }) {
			const { pageNum, isRental, label, isDetailPage, propertyData } = request.userData || {};

			// If this is a detail page request, process and return
			if (isDetailPage) {
				const property = propertyData;
				try {
					console.log(`🔎 Detail - ${property.link}`);
					// We're already on the detail page
					const latitude = await (async () => {
						try {
							const dataLocation = await page.$eval(".google-map-embed", (el) =>
								el.getAttribute("data-location")
							);
							if (dataLocation) return parseFloat(dataLocation.split(",")[0].trim());
						} catch (e) {}
						try {
							const jsonLd = await page.$$eval('script[type="application/ld+json"]', (tags) =>
								tags.map((t) => t.textContent)
							);
							for (const s of jsonLd) {
								try {
									const parsed = JSON.parse(s);
									const items = Array.isArray(parsed) ? parsed : [parsed];
									for (const item of items) {
										if (item.geo && item.geo.latitude) return parseFloat(item.geo.latitude);
										if (item.latitude) return parseFloat(item.latitude);
									}
								} catch (e) {}
							}
						} catch (e) {}
						return null;
					})();

					const longitude = await (async () => {
						try {
							const dataLocation = await page.$eval(".google-map-embed", (el) =>
								el.getAttribute("data-location")
							);
							if (dataLocation) return parseFloat(dataLocation.split(",")[1].trim());
						} catch (e) {}
						try {
							const jsonLd = await page.$$eval('script[type="application/ld+json"]', (tags) =>
								tags.map((t) => t.textContent)
							);
							for (const s of jsonLd) {
								try {
									const parsed = JSON.parse(s);
									const items = Array.isArray(parsed) ? parsed : [parsed];
									for (const item of items) {
										if (item.geo && item.geo.longitude) return parseFloat(item.geo.longitude);
										if (item.longitude) return parseFloat(item.longitude);
									}
								} catch (e) {}
							}
						} catch (e) {}
						return null;
					})();

					const tableName = isRental ? "property_for_rent" : "property_for_sale";

					// Check DB for existing entries
					const [existingRows] = await promisePool.query(
						`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
						[property.link.trim(), AGENT_ID]
					);

					const [otherAgentRows] = await promisePool.query(
						`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id != ?`,
						[property.link.trim(), AGENT_ID]
					);

					if (existingRows.length > 0) {
						await promisePool.query(
							`UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
							[property.price, latitude, longitude, property.link.trim(), AGENT_ID]
						);
						console.log(
							`✅ Updated: ${property.link.substring(0, 60)}... | Price: £${
								property.price
							} | Coords: ${latitude}, ${longitude}`
						);
					} else if (otherAgentRows.length > 0) {
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
							`✅ Created: ${property.link.substring(0, 60)}... | Price: £${
								property.price
							} | Coords: ${latitude}, ${longitude}`
						);
					} else {
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
							`✅ Created: ${property.link.substring(0, 60)}... | Price: £${
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
				} catch (err) {
					console.error(`❌ Error processing ${property.link}: ${err.message}`);
				}

				return;
			}

			// Otherwise this is a listing page
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Wait for listing container
			await page.waitForTimeout(2000);
			await page
				.waitForSelector(".search-list-11-results, .panel.panel-default", { timeout: 30000 })
				.catch(() => {
					console.log(`⚠️ No properties found on page ${pageNum}`);
				});

			// Extract properties from panels
			const properties = await page.$$eval(".panel.panel-default", (panels) => {
				const results = [];
				panels.forEach((panel) => {
					try {
						// Link - prefer h4.card-title a
						const linkEl =
							panel.querySelector("h4.card-title a") ||
							panel.querySelector(".embed-responsive a") ||
							panel.querySelector("a");
						let link = linkEl ? linkEl.getAttribute("href") : null;
						if (!link) return;
						if (!link.startsWith("http")) link = "https://www.guildproperty.co.uk" + link;

						// Title
						const titleEl = panel.querySelector("h4.card-title a");
						const title = titleEl ? titleEl.textContent.trim() : null;

						// Price - look for .h4 or .h4.m-0
						let price = null;
						const priceEl = panel.querySelector(".h4.m-0, .h4");
						if (priceEl) {
							const txt = priceEl.textContent.trim();
							const m = txt.match(/[£€]\s*([\d,]+)/);
							if (m) price = m[1].replace(/,/g, "");
						}

						// Bedrooms - search paragraph text for 'Bedroom'
						let bedrooms = null;
						const pText = panel.querySelector("p") ? panel.querySelector("p").textContent : "";
						const bedMatch = pText.match(/(\d+)\s*Bedroom/);
						if (bedMatch) bedrooms = bedMatch[1];

						if (link && title && price) results.push({ link, title, price, bedrooms });
					} catch (e) {
						// skip
					}
				});
				return results;
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			if (properties.length === 0) return;

			// Process properties in batches of 5 (open 5 new pages concurrently per batch)
			const chunkSize = 2;
			for (let start = 0; start < properties.length; start += chunkSize) {
				const chunk = properties.slice(start, start + chunkSize);

				// Process chunk concurrently
				await Promise.all(
					chunk.map(async (p) => {
						const newPage = await page.context().newPage();
						try {
							await newPage.goto(p.link, { waitUntil: "domcontentloaded", timeout: 30000 });
							await newPage.waitForTimeout(1000);

							let latitude = null;
							let longitude = null;

							// Try data-location
							try {
								const dataLocation = await newPage.$eval(".google-map-embed", (el) =>
									el.getAttribute("data-location")
								);
								if (dataLocation) {
									const parts = dataLocation.split(",").map((s) => s.trim());
									if (parts.length >= 2) {
										latitude = parseFloat(parts[0]);
										longitude = parseFloat(parts[1]);
									}
								}
							} catch (e) {}

							// Fallback JSON-LD
							if (!latitude || !longitude) {
								try {
									const jsonLd = await newPage.$$eval(
										'script[type="application/ld+json"]',
										(tags) => tags.map((t) => t.textContent)
									);
									for (const s of jsonLd) {
										try {
											const parsed = JSON.parse(s);
											const items = Array.isArray(parsed) ? parsed : [parsed];
											for (const item of items) {
												if (!item) continue;
												if (item.geo && item.geo.latitude && item.geo.longitude) {
													latitude = parseFloat(item.geo.latitude);
													longitude = parseFloat(item.geo.longitude);
													break;
												}
												if (item.latitude && item.longitude) {
													latitude = parseFloat(item.latitude);
													longitude = parseFloat(item.longitude);
													break;
												}
											}
											if (latitude && longitude) break;
										} catch (e) {}
									}
								} catch (e) {}
							}

							const tableName = isRental ? "property_for_rent" : "property_for_sale";

							// DB operations (same pattern)
							const [existingRows] = await promisePool.query(
								`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
								[p.link.trim(), AGENT_ID]
							);
							const [otherAgentRows] = await promisePool.query(
								`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id != ?`,
								[p.link.trim(), AGENT_ID]
							);

							if (existingRows.length > 0) {
								await promisePool.query(
									`UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
									[p.price, latitude, longitude, p.link.trim(), AGENT_ID]
								);
								console.log(
									`✅ Updated: ${p.link.substring(0, 60)}... | Price: £${
										p.price
									} | Coords: ${latitude}, ${longitude}`
								);
							} else if (otherAgentRows.length > 0) {
								const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
								const logo = "property_for_sale/logo.png";
								const currentTime = new Date();
								await promisePool.query(insertQuery, [
									p.title,
									AGENT_ID,
									p.price,
									p.bedrooms,
									p.link.trim(),
									logo,
									latitude,
									longitude,
									currentTime,
									currentTime,
								]);
								console.log(
									`✅ Created: ${p.link.substring(0, 60)}... | Price: £${
										p.price
									} | Coords: ${latitude}, ${longitude}`
								);
							} else {
								const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
								const logo = "property_for_sale/logo.png";
								const currentTime = new Date();
								await promisePool.query(insertQuery, [
									p.title,
									AGENT_ID,
									p.price,
									p.bedrooms,
									p.link.trim(),
									logo,
									latitude,
									longitude,
									currentTime,
									currentTime,
								]);
								console.log(
									`✅ Created: ${p.link.substring(0, 60)}... | Price: £${
										p.price
									} | Coords: ${latitude}, ${longitude}`
								);
							}

							totalSaved++;
							totalScraped++;

							if (latitude && longitude) {
								console.log(`✅ ${p.title} - £${p.price} - ${latitude}, ${longitude}`);
							} else {
								console.log(`✅ ${p.title} - £${p.price} - No coords`);
							}
						} catch (err) {
							console.error(`❌ Error processing ${p.link}: ${err.message}`);
						} finally {
							await newPage.close();
						}
					})
				);

				// small pause between batches
				await new Promise((r) => setTimeout(r, 500));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Queue pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
		console.log(`🏠 Queueing ${propertyType.label} pages: ${totalPages} pages`);
		const requests = [];
		// Start from START_PAGE and queue through to the end (process pages sequentially: 98, 99, 100...)
		const startPage = Math.max(1, START_PAGE);
		for (let page = startPage; page <= totalPages; page++) {
			const url = propertyType.urlBase.replace("page=1", `page=${page}`);
			requests.push({
				url,
				userData: { pageNum: page, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		// Add and run in batches to avoid huge queue memory use
		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed GuildProperty - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
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
		await scrapeGuildProperty();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
