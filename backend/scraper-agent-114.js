// Jackson-Stops scraper using Playwright with Crawlee
// Agent ID: 114
// Usage:
// node backend/scraper-agent-114.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 114;
let totalScraped = 0;
let totalSaved = 0;


// Configuration for Jackson-Stops
// Sales: ~160 properties / 48 per page => 4 pages
// Lettings: ~382 properties / 48 per page => 8 pages
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.jackson-stops.co.uk/london/sales",
		totalPages: 11,
		recordsPerPage: 48,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.jackson-stops.co.uk/properties/lettings",
		totalPages: 28,
		recordsPerPage: 48,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeJacksonStops() {
	console.log(`\n🚀 Starting Jackson-Stops scraper (Agent ${AGENT_ID})...\n`);

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

			await page
				.waitForSelector(".property-single__grid, .section.property-single__grid", {
					timeout: 20000,
				})
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate((isRental) => {
				try {
					const items = Array.from(document.querySelectorAll(".property-single__grid"));
					return items
						.map((el) => {
							// Check status and exclude Sold/SSTC/Let
							const statusEl = el.querySelector(".property-single__grid__status__main");
							const status = statusEl?.textContent?.trim().toUpperCase() || "";
							if (status === "SOLD" || status === "SSTC" || status === "LET") {
								return null;
							}
							const linkEl = el.querySelector("a.property-single__grid__link-wrapper");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.jackson-stops.co.uk" + href
								: null;
							// Extract price and remove "Guideprice" prefix
							let price =
								el.querySelector(".property-single__grid__price")?.textContent?.trim() || "";
							price = price.replace(/^Guideprice\s*/i, "").trim();

							// For rental properties, extract PM value (e.g., "£5,295 PM, (£1,222 PW)" -> "£5,295" or "24917PM(5750PW)" -> "24917")
							if (isRental) {
								const pmMatch = price.match(/^([^P]+?)PM/i);
								if (pmMatch) {
									price = pmMatch[1].trim();
								}
							}
							const title =
								el.querySelector(".property-single__grid__address")?.textContent?.trim() || "";

							// Extract bedrooms from the rooms div
							const roomsDiv = el.querySelector(".property-single__grid__rooms");
							let bedrooms = null;
							if (roomsDiv) {
								const bedroomSpan = Array.from(roomsDiv.querySelectorAll("span")).find((span) =>
									/Bedrooms?/i.test(span.textContent)
								);
								if (bedroomSpan) {
									const match = bedroomSpan.textContent.match(/(\d+)\s*Bedrooms?/i);
									bedrooms = match ? match[1] : null;
								}
							}

							return { link, price, title, bedrooms, lat: null, lng: null };
						})
						.filter((p) => p && p.link);
				} catch (e) {
					return [];
				}
			}, isRental);
			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);
			const batchSize = 1;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
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

								// Try to read global properties on detail page or a relatedProperties var
								const detailCoords = await detailPage.evaluate(() => {
									try {
										// First, try to find lat/lng in loadLocratingPlugin call
										const allScripts = Array.from(document.querySelectorAll("script"))
											.map((s) => s.textContent)
											.join("\n");

										// Look for loadLocratingPlugin with lat and lng parameters
										const locratingMatch = allScripts.match(
											/loadLocratingPlugin\s*\(\s*\{[^}]*lat\s*:\s*([0-9.+-]+)[^}]*lng\s*:\s*([0-9.+-]+)/
										);
										if (locratingMatch) {
											return {
												lat: parseFloat(locratingMatch[1]),
												lng: parseFloat(locratingMatch[2]),
											};
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

						try {
							// Skip sold properties (extra safety)
							if (isSoldProperty(property.title)) {
								console.log(`⏭ Skipping sold: ${property.title}`);
								return;
							}

							// Format UK price properly
							const formattedPrice = formatPriceUk(property.price);

							// Safe title fallback
							property.title =
								property.title?.trim() ||
								property.link.split("/").pop().replace(/-/g, " ");

							// If coordinates still missing, use shared extractor
							if (!coords.latitude || !coords.longitude) {
								const detailPage = await page.context().newPage();
								try {
									await detailPage.goto(property.link, {
										waitUntil: "domcontentloaded",
										timeout: 30000,
									});
									await detailPage.waitForTimeout(500);

									const html = await detailPage.content();
									const extracted = extractCoordinatesFromHTML(html);

									if (extracted) {
										coords.latitude = extracted.latitude;
										coords.longitude = extracted.longitude;
									}
								} catch (err) {
									// ignore
								} finally {
									await detailPage.close();
								}
							}

							// Optimized update (smart change detection)
							await updatePriceByPropertyURLOptimized({
								link: property.link,
								price: formattedPrice,
								title: property.title,
								bedrooms: property.bedrooms,
								agentId: AGENT_ID,
								isRental,
								latitude: coords?.latitude || null,
								longitude: coords?.longitude || null,
							});

							// Fallback safety update
							await updatePriceByPropertyURL(
								property.link,
								formattedPrice,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								coords?.latitude || null,
								coords?.longitude || null
							);

							totalSaved++;
							totalScraped++;

							console.log(
								`✅ ${property.title} | ${formattedPrice} | ${coords?.latitude && coords?.longitude
									? `${coords.latitude}, ${coords.longitude}`
									: "No coords"
								}`
							);
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
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

	// Enqueue pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url =
				pg === 1
					? `${propertyType.urlBase}/?page_size=48#grid`
					: `${propertyType.urlBase}/page-${pg}?page_size=48#grid`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Jackson-Stops - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeJacksonStops();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
