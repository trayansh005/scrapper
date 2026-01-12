// Hamptons lettings scraper using Playwright with Crawlee
// Agent ID: 108
// Usage:
// node backend/scraper-agent-108.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 108;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(num) {
	if (!num || isNaN(num)) return "£0";
	return "£" + Number(num).toLocaleString("en-GB");
}

// Configuration: Hamptons lettings & sales
// Lettings: 1192 properties, 12 per page => 100 pages
// Sales: 2525 properties, 12 per page => 211 pages
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.hamptons.co.uk/properties/lettings/status-available",
		totalPages: 100,
		recordsPerPage: 12,
		isRental: true,
		label: "HAMPTONS_LETTINGS",
		params: "",
	},
	// {
	// 	urlBase: "https://www.hamptons.co.uk/properties/sales/status-available",
	// 	totalPages: 211,
	// 	recordsPerPage: 12,
	// 	isRental: false,
	// 	label: "HAMPTONS_SALES",
	// 	params: "",
	// },
];

async function scrapeHamptons() {
	console.log(`\n🚀 Starting Hamptons scraper (Agent ${AGENT_ID})...\n`);

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
			await page.waitForSelector("article.property-card", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No property cards found on page ${pageNum}`);
			});

			// Extract properties from the listing page and deduplicate by property id/link
			const properties = await page.evaluate(() => {
				const containers = Array.from(document.querySelectorAll("article.property-card"));
				const map = new Map();

				for (const container of containers) {
					const linkEl = container.querySelector("a.property-card__link");
					const rawHref = linkEl ? linkEl.getAttribute("href") : null;
					const link = rawHref ? new URL(rawHref, "https://www.hamptons.co.uk").href : null;

					// Prefer data-property-id if available to dedupe
					const propId = linkEl ? linkEl.getAttribute("data-property-id") || null : null;

					const price = container.querySelector(".property-card__price")?.textContent?.trim() || "";
					const title = container.querySelector(".property-card__title")?.textContent?.trim() || "";

					let bedrooms = null;
					const bedEl = container.querySelector(
						".property-card__bedbath .property-card__bedbath-item"
					);
					if (bedEl) {
						const bedText = bedEl.textContent?.trim() || "";
						const m = bedText.match(/(\d+)/);
						if (m) bedrooms = parseInt(m[1]);
					}

					const key = propId || link;
					if (!key) continue;

					if (!map.has(key)) {
						map.set(key, { id: propId, link, title, price, bedrooms });
					}
				}

				return Array.from(map.values());
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process properties one by one with rate limiting
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
					// Wait for scripts (esp ga4/dataLayer) to be fully loaded
					try {
						await detailPage.waitForFunction(
							() =>
								window.dataLayer && Array.isArray(window.dataLayer) && window.dataLayer.length > 0,
							{ timeout: 3000 }
						);
					} catch (e) {
						// dataLayer may not exist; continue anyway
					}
					await detailPage.waitForTimeout(300);

					coords = await detailPage.evaluate(() => {
						try {
							// 1) Check for dataLayer objects (common for ga4 vars)
							try {
								if (Array.isArray(window.dataLayer)) {
									for (const obj of window.dataLayer) {
										if (!obj) continue;
										if (obj.ga4_property_latitude || obj.ga4_property_longitude) {
											return {
												latitude: obj.ga4_property_latitude
													? parseFloat(obj.ga4_property_latitude)
													: null,
												longitude: obj.ga4_property_longitude
													? parseFloat(obj.ga4_property_longitude)
													: null,
											};
										}
									}
								}
							} catch (e) {
								// ignore
							}

							const html = document.documentElement.outerHTML;

							// 2) Look for ga4_property_latitude / ga4_property_longitude in JS objects
							const latMatch1 = html.match(/ga4_property_latitude\s*[:=]\s*['\"]?([\d.-]+)['\"]?/i);
							const lonMatch1 = html.match(
								/ga4_property_longitude\s*[:=]\s*['\"]?([\d.-]+)['\"]?/i
							);
							if (latMatch1 && lonMatch1) {
								return {
									latitude: parseFloat(latMatch1[1]),
									longitude: parseFloat(lonMatch1[1]),
								};
							}

							// 3) JSON-LD GeoCoordinates
							try {
								const scripts = Array.from(
									document.querySelectorAll('script[type="application/ld+json"]')
								);
								for (const s of scripts) {
									try {
										const json = JSON.parse(s.textContent || "{}");
										if (json && json["@type"] === "GeoCoordinates") {
											return {
												latitude: json.latitude ? parseFloat(json.latitude) : null,
												longitude: json.longitude ? parseFloat(json.longitude) : null,
											};
										}
										// sometimes json is an array or contains graph
										if (Array.isArray(json)) {
											for (const it of json) {
												if (it && it["@type"] === "GeoCoordinates") {
													return {
														latitude: it.latitude ? parseFloat(it.latitude) : null,
														longitude: it.longitude ? parseFloat(it.longitude) : null,
													};
												}
											}
										}
									} catch (e) {
										// continue
									}
								}
							} catch (e) {
								// ignore
							}

							// 4) fallback - search any script tag contents for lat/lng pattern
							const scriptsAll = Array.from(document.scripts || []);
							for (const s of scriptsAll) {
								const txt = s.textContent || "";

								// Try ga4_property_latitude / ga4_property_longitude
								const latMatch = txt.match(/ga4_property_latitude\s*[:=]\s*['\"]?([\d.-]+)['\"]?/i);
								const lonMatch = txt.match(
									/ga4_property_longitude\s*[:=]\s*['\"]?([\d.-]+)['\"]?/i
								);
								if (latMatch && lonMatch) {
									return {
										latitude: parseFloat(latMatch[1]),
										longitude: parseFloat(lonMatch[1]),
									};
								}

								// Try lat: and lng: pattern
								const latLngLat = txt.match(/lat\s*:\s*[\'"(]?([\d.-]+)[\'")?]/i);
								const latLngLng = txt.match(/lng\s*:\s*[\'"(]?([\d.-]+)[\'")?]/i);
								if (latLngLat && latLngLng) {
									return {
										latitude: parseFloat(latLngLat[1]),
										longitude: parseFloat(latLngLng[1]),
									};
								}
							}

							return { latitude: null, longitude: null };
						} catch (e) {
							return { latitude: null, longitude: null };
						}
					});
				} catch (err) {
					console.error(`⚠️ Failed to open detail ${property.link}: ${err.message}`);
				} finally {
					await detailPage.close();
				}

				try {
					const priceClean = (property.price || "").replace(/[^0-9]/g, "").trim();

					await updatePriceByPropertyURL(
						property.link,
						priceClean || null,
						property.title || "",
						property.bedrooms || null,
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

				// Rate limiting delay between properties
				await new Promise((resolve) => setTimeout(resolve, 500));
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
		`\n✅ Completed Hamptons - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeHamptons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
