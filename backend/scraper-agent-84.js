// White & Sons scraper using Playwright with Crawlee
// Agent ID: 84
// Usage:
// node backend/scraper-agent-84.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 84;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.whiteandsons.co.uk/properties/sales/status-available",
		totalPages: 19,
		recordsPerPage: 12,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.whiteandsons.co.uk/properties/lettings/status-available",
		totalPages: 1,
		recordsPerPage: 12,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeWhiteAndSons() {
	console.log(`\n🚀 Starting White & Sons scraper (Agent ${AGENT_ID})...\n`);

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
			await page.waitForTimeout(1500);
			await page.waitForSelector(".flex.grid", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			// Try to extract properties from client-side JS variables (relatedProperties / Homeflow)
			const properties = await page.evaluate(() => {
				try {
					// Try window.relatedProperties (some pages set this)
					if (
						typeof window.relatedProperties !== "undefined" &&
						Array.isArray(window.relatedProperties)
					) {
						return window.relatedProperties.map((p) => ({
							link:
								p.property_url && p.property_url.startsWith("http")
									? p.property_url
									: "https://www.whiteandsons.co.uk" + p.property_url,
							price: p.price || "",
							title: p.primary_address_display || p.display_address || "",
							bedrooms: p.bedrooms || null,
							lat: p.lat || null,
							lng: p.lng || null,
							property_id: p.property_id || null,
						}));
					}

					// Try Homeflow.get('properties') if available
					if (window.Homeflow && typeof window.Homeflow.get === "function") {
						const hf = window.Homeflow.get("properties");
						if (hf && Array.isArray(hf)) {
							return hf.map((p) => ({
								link:
									p.property_url && p.property_url.startsWith("http")
										? p.property_url
										: "https://www.whiteandsons.co.uk" + p.property_url,
								price: p.price || "",
								title: p.primary_address_display || p.display_address || "",
								bedrooms: p.bedrooms || null,
								lat: p.lat || null,
								lng: p.lng || null,
								property_id: p.property_id || null,
							}));
						}
					}

					// Fallback: parse DOM nodes
					const container = document.querySelector(".flex.grid");
					if (!container) return [];
					const items = Array.from(container.querySelectorAll(".item"));
					return items
						.map((el) => {
							const linkEl = el.querySelector("a.property-card__link-img, a.card-link");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.whiteandsons.co.uk" + href
								: null;
							const price = el.querySelector(".price-value")?.textContent?.trim() || "";
							const title =
								el.querySelector(".property-card__details h4")?.textContent?.trim() || "";
							const bedroomsMatch = title.match(/(\d+) bedroom/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;
							return { link, price, title, bedrooms, lat: null, lng: null, property_id: null };
						})
						.filter((p) => p.link);
				} catch (e) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process properties in small batches
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						// Ensure absolute URL
						if (!property.link) return;

						let coords = { latitude: property.lat || null, longitude: property.lng || null };

						// If no coords, visit detail page to try to extract coordinates from scripts or JSON-LD
						if (!coords.latitude || !coords.longitude) {
							const detailPage = await page.context().newPage();
							try {
								await detailPage.goto(property.link, {
									waitUntil: "domcontentloaded",
									timeout: 30000,
								});
								await detailPage.waitForTimeout(500);

								// Try to read global Homeflow properties on detail page or a relatedProperties var
								const detailCoords = await detailPage.evaluate(() => {
									try {
										if (
											typeof window.relatedProperties !== "undefined" &&
											Array.isArray(window.relatedProperties)
										) {
											const p = window.relatedProperties.find((x) => x.lat && x.lng);
											if (p) return { lat: p.lat, lng: p.lng };
										}

										if (window.Homeflow && typeof window.Homeflow.get === "function") {
											const hf = window.Homeflow.get("properties");
											if (hf && Array.isArray(hf)) {
												const p = hf.find((x) => x.lat && x.lng);
												if (p) return { lat: p.lat, lng: p.lng };
											}
										}

										// JSON-LD geo data
										const scripts = Array.from(
											document.querySelectorAll('script[type="application/ld+json"]')
										);
										for (const s of scripts) {
											try {
												const data = JSON.parse(s.textContent);
												if (data && data.geo && data.geo.latitude && data.geo.longitude) {
													return { lat: data.geo.latitude, lng: data.geo.longitude };
												}
											} catch (e) {
												// continue
											}
										}

										// Last resort: regex search for "lat": number in scripts
										const allScripts = Array.from(document.querySelectorAll("script"))
											.map((s) => s.textContent)
											.join("\n");
										const latMatch = allScripts.match(/"lat"\s*:\s*([0-9.+-]+)/);
										const lngMatch = allScripts.match(/"lng"\s*:\s*([0-9.+-]+)/);
										if (latMatch && lngMatch) {
											return { lat: parseFloat(latMatch[1]), lng: parseFloat(lngMatch[1]) };
										}

										return null;
									} catch (e) {
										return null;
									}
								});

								if (detailCoords) {
									coords.latitude = detailCoords.lat;
									coords.longitude = detailCoords.lng;
								}
							} catch (err) {
								// ignore detail page errors
							} finally {
								await detailPage.close();
							}
						}

						// Determine which DB table to use
						const tableName = isRental ? "property_for_rent" : "property_for_sale";

						try {
							// Clean price: extract only numbers (e.g., "£47,666pcm" → "47666")
							const priceClean = property.price
								? property.price.replace(/[^0-9]/g, "").trim()
								: null;

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

							const coordsStr =
								coords.latitude && coords.longitude
									? `${coords.latitude}, ${coords.longitude}`
									: "No coords";
							console.log(`✅ ${property.title} - ${formatPrice(priceClean)} - ${coordsStr}`);
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
			// Construct page URL using /page-N#/ pagination (page-2#/)
			// Page 1 uses the base URL with a trailing slash.
			const url = pg === 1 ? `${propertyType.urlBase}/` : `${propertyType.urlBase}/page-${pg}#/`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed White & Sons - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

// Updated scraper for Snellers
const PROPERTY_TYPES_SNELLERS = [
	{
		urlBase: "https://www.snellers.co.uk/properties/lettings/status-available",
		totalPages: 2, // Adjust based on the number of pages
		recordsPerPage: 12,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeSnellers() {
	console.log(`\n🚀 Starting Snellers scraper (Agent ${AGENT_ID})...\n`);

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
			await page.waitForTimeout(1500);
			await page.waitForSelector(".property-card", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			// Extract properties from the DOM
			const properties = await page.evaluate(() => {
				const cards = Array.from(document.querySelectorAll(".property-card"));
				return cards.map((card) => {
					const linkEl = card.querySelector("a.no-decoration");
					const link = linkEl ? linkEl.href : null;
					const title = linkEl ? linkEl.title : "";
					const price = card.querySelector(".price .money")?.textContent.trim() || "";
					const bedrooms =
						card.querySelector(".bed-baths li:nth-child(1)")?.textContent.trim() || "";
					const bathrooms =
						card.querySelector(".bed-baths li:nth-child(2)")?.textContent.trim() || "";
					const reception =
						card.querySelector(".bed-baths li:nth-child(3)")?.textContent.trim() || "";
					const description =
						card.querySelector(".property-card-description")?.textContent.trim() || "";
					return { link, title, price, bedrooms, bathrooms, reception, description };
				});
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			for (const property of properties) {
				if (!property.link) continue;

				let coords = { latitude: null, longitude: null };

				// Visit detail page to extract coordinates
				const detailPage = await page.context().newPage();
				try {
					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});
					await detailPage.waitForTimeout(500);

					coords = await detailPage.evaluate(() => {
						const mapEl = document.querySelector("#propertyShowStreetview.map");
						if (mapEl) {
							const lat = mapEl.getAttribute("data-lat");
							const lng = mapEl.getAttribute("data-lng");
							return { latitude: parseFloat(lat), longitude: parseFloat(lng) };
						}
						return { latitude: null, longitude: null };
					});
				} catch (err) {
					console.error(`❌ Failed to extract details for ${property.link}: ${err.message}`);
				} finally {
					await detailPage.close();
				}

				// Save property to the database
				const tableName = isRental ? "property_for_rent" : "property_for_sale";
				try {
					const priceClean = (property.price || "").replace(/[£,\s]/g, "").trim();
					const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, bathrooms, reception, description, property_url, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
					const currentTime = new Date();
					await promisePool.query(insertQuery, [
						property.title,
						AGENT_ID,
						priceClean || null,
						property.bedrooms || null,
						property.bathrooms || null,
						property.reception || null,
						property.description || null,
						property.link.trim(),
						coords.latitude,
						coords.longitude,
						currentTime,
						currentTime,
					]);
					console.log(`🆕 Inserted property for agent ${AGENT_ID}: ${property.link}`);
				} catch (dbErr) {
					console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue all listing pages
	for (const propertyType of PROPERTY_TYPES_SNELLERS) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = pg === 1 ? `${propertyType.urlBase}` : `${propertyType.urlBase}/page-${pg}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Snellers - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeWhiteAndSons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
