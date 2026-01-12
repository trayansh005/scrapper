// Harrods Estates scraper using Playwright with Crawlee
// Agent ID: 215
// Usage:
// node backend/scraper-agent-215.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 215;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for Harrods Estates
// 9 properties per page
const PROPERTY_TYPES = [
	{
		// Sales
		urlBase: "https://www.harrodsestates.com/properties/sales/status-available",
		totalPages: Math.ceil(37 / 9),
		recordsPerPage: 9,
		isRental: false,
		label: "SALES",
	},
	// {
	// 	// Rentals
	// 	urlBase: "https://www.harrodsestates.com/properties/lettings/status-available",
	// 	totalPages: Math.ceil(54 / 9),
	// 	recordsPerPage: 9,
	// 	isRental: true,
	// 	label: "RENTALS",
	// },
];

async function scrapeHarrods() {
	console.log(`\n🚀 Starting Harrods Estates scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(700);

			// Cards use data-page-marker attribute; fallback to links under /properties/
			await page
				.waitForSelector('[data-page-marker], a[href^="/properties/"]', { timeout: 20000 })
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll("[data-page-marker]"));
					if (!items.length) {
						// fallback: find anchors under listings
						return Array.from(document.querySelectorAll('a[href^="/properties/"]'))
							.map((a) => {
								const href = a.getAttribute("href");
								const link = href
									? href.startsWith("http")
										? href
										: "https://www.harrodsestates.com" + href
									: null;
								const title = (
									a.getAttribute("title") ||
									a.querySelector(".font-serif")?.textContent ||
									""
								).trim();
								const address = a.querySelector("div")?.textContent?.trim() || "";
								return {
									link,
									price: null,
									title: title || address || "",
									bedrooms: null,
									lat: null,
									lng: null,
								};
							})
							.filter((p) => p.link);
					}

					return items
						.map((el) => {
							const linkEl = el.querySelector("a[href]");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.harrodsestates.com" + href
								: null;

							const title =
								linkEl?.querySelector(".font-serif")?.textContent?.trim() ||
								linkEl?.getAttribute("title")?.trim() ||
								"";
							// address is often in the next div under the anchor
							const address = linkEl?.querySelector("div")?.textContent?.trim() || "";

							// Price text contains 'Guide price' or just a price span
							let price = null;
							const priceEl =
								el.querySelector("div.font-sans span") ||
								el.querySelector('div:contains("Guide price")');
							if (priceEl) price = priceEl.textContent.trim();
							// bedrooms typically in a span with class text-custom-5 near icons
							let bedrooms = null;
							const smallSpans = el.querySelectorAll("span.text-custom-5");
							if (smallSpans && smallSpans.length) bedrooms = smallSpans[0].textContent.trim();

							return {
								link,
								price,
								title: title || address || "",
								bedrooms: bedrooms || null,
								lat: null,
								lng: null,
							};
						})
						.filter((p) => p.link);
				} catch (e) {
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

						let coords = { latitude: property.lat || null, longitude: property.lng || null };

						if (!coords.latitude || !coords.longitude) {
							const detailPage = await page.context().newPage();
							try {
								await detailPage.goto(property.link, {
									waitUntil: "domcontentloaded",
									timeout: 30000,
								});
								await detailPage.waitForTimeout(400);

								const detailCoords = await detailPage.evaluate(() => {
									try {
										// Look for lat/lng in scripts
										const scripts = Array.from(document.querySelectorAll("script"))
											.map((s) => s.textContent || "")
											.join("\n");

										// JSON-style lat/lng pairs
										const m1 = scripts.match(/"lat"\s*:\s*([0-9.+-]+)/i);
										const m2 = scripts.match(/"lng"\s*:\s*([0-9.+-]+)/i);
										if (m1 && m2) return { lat: parseFloat(m1[1]), lng: parseFloat(m2[1]) };

										const m3 = scripts.match(/"latitude"\s*:\s*([0-9.+-]+)/i);
										const m4 = scripts.match(/"longitude"\s*:\s*([0-9.+-]+)/i);
										if (m3 && m4) return { lat: parseFloat(m3[1]), lng: parseFloat(m4[1]) };

										// fallback: try to parse application/ld+json
										const ld = Array.from(
											document.querySelectorAll('script[type="application/ld+json"]')
										)
											.map((s) => s.textContent)
											.join("\n");
										try {
											const parsed = JSON.parse(ld);
											const arr = Array.isArray(parsed) ? parsed : parsed["@graph"] || [parsed];
											for (const node of arr) {
												if (!node) continue;
												if (node.geo && (node.geo.latitude || node.geo.longitude)) {
													const lat = parseFloat(node.geo.latitude);
													const lng = parseFloat(node.geo.longitude);
													if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
												}
												if (
													node.geolocation &&
													(node.geolocation.latitude || node.geolocation.longitude)
												) {
													const lat = parseFloat(node.geolocation.latitude);
													const lng = parseFloat(node.geolocation.longitude);
													if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
												}
											}
										} catch (e) {
											// ignore
										}

										return null;
									} catch (e) {
										return null;
									}
								});

								if (detailCoords) {
									let lat = detailCoords.lat;
									let lng = detailCoords.lng;
									// Heuristic for inverted coordinates (UK region)
									if (
										Math.abs(lat) <= 10 &&
										lng >= 49 &&
										lng <= 61 &&
										!(lat >= 49 && lat <= 61 && Math.abs(lng) <= 10)
									) {
										const t = lat;
										lat = lng;
										lng = t;
									}
									coords.latitude = lat;
									coords.longitude = lng;
								}
							} catch (err) {
								// ignore detail page errors
							} finally {
								await detailPage.close();
							}
						}

						const tableName = isRental ? "property_for_rent" : "property_for_sale";

						try {
							const [existingRows] = await promisePool.query(
								`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
								[property.link.trim(), AGENT_ID]
							);

							const [otherAgentRows] = await promisePool.query(
								`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id != ?`,
								[property.link.trim(), AGENT_ID]
							);

							const rawPrice = (property.price || "").toString();
							const numMatch = rawPrice.match(/[0-9][0-9,\.\s]*/);
							const priceClean = numMatch ? numMatch[0].replace(/[^0-9]/g, "") : "";

							if (existingRows.length > 0) {
								await promisePool.query(
									`UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
									[
										priceClean || null,
										coords.latitude,
										coords.longitude,
										property.link.trim(),
										AGENT_ID,
									]
								);
								console.log(`✅ Updated existing property for agent ${AGENT_ID}: ${property.link}`);
							} else if (otherAgentRows.length > 0) {
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
								console.log(`🆕 Created property for agent ${AGENT_ID}: ${property.link}`);
							} else {
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

				await new Promise((resolve) => setTimeout(resolve, 300));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			// Harrods uses path-based pagination like /page-2#/
			const url = pg === 1 ? `${propertyType.urlBase}/#/` : `${propertyType.urlBase}/page-${pg}#/`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Harrods Estates - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
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
		await scrapeHarrods();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
