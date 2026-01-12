// Map Estate Agents scraper using Playwright with Crawlee
// Agent ID: 231
// Usage:
// node backend/scraper-agent-231.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 231;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

// Two searches: sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.mapestateagents.com/property-sales/properties-for-sale?start=",
	// 	totalRecords: 20 * 12, // approx placeholder (20 pages x 12 per page)
	// 	recordsPerPage: 12,
	// 	totalPages: 20,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// },
	{
		urlBase: "https://www.mapestateagents.com/property-lettings/properties-to-let?start=",
		totalRecords: 1 * 12,
		recordsPerPage: 12,
		totalPages: 1,
		isRental: true,
		label: "TO LET",
	},
];

async function scrapeMapEstateAgents() {
	console.log(`\n🚀 Starting Map Estate Agents scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(1500);

			// Wait for listing cards
			await page
				.waitForSelector(
					".span4.eapow-row0.eapow-overview-row, .span4.eapow-row1.eapow-overview-row",
					{ timeout: 15000 }
				)
				.catch(() => console.log(`⚠️ No listing container found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(
							".span4.eapow-row0.eapow-overview-row, .span4.eapow-row1.eapow-overview-row"
						)
					);

					return items
						.map((el) => {
							try {
								// Check for sold / sold stc banners
								const soldBannerImg = el.querySelector(
									'img[src*="banner_sold"], img[src*="banner_soldstc"], img[alt*="Sold"]'
								);
								if (soldBannerImg) return null;

								const thumbAnchor =
									el.querySelector(".eapow-property-thumb-holder a") || el.querySelector("a");
								const relativeLink = thumbAnchor ? thumbAnchor.getAttribute("href") : null;
								const link = relativeLink
									? new URL(relativeLink, "https://www.mapestateagents.com").href
									: null;

								const title =
									el.querySelector(".eapow-overview-title h3")?.textContent?.trim() || "";
								const rawPrice =
									el.querySelector(".eapow-overview-price.propPrice")?.textContent?.trim() || "";
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}

								// icons show numbers in .IconNum spans; extract first three if present
								const iconNums = Array.from(el.querySelectorAll(".IconNum")).map((s) =>
									s.textContent.trim()
								);
								const bedrooms = iconNums[0] || null;
								const bathrooms = iconNums[1] || null;
								const receptions = iconNums[2] || null;

								if (!link) return null;

								return { link, title, price, bedrooms, bathrooms, receptions };
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
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

						let coords = { latitude: null, longitude: null };

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							// First: try to extract from eapowmapoptions script
							const eapowCoords = await detailPage.evaluate(() => {
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const script of scripts) {
									const text = script.textContent || "";
									if (text.includes("eapowmapoptions")) {
										// Try to extract lat and lon from: lat: "50.11914", lon: "-5.161167"
										const latMatch = text.match(/['"]*lat['"]*\s*:\s*['"]([\-0-9.]+)['"]/i);
										const lonMatch = text.match(/['"]*lon['"]*\s*:\s*['"]([\-0-9.]+)['"]/i);
										if (latMatch && lonMatch) {
											return {
												latitude: parseFloat(latMatch[1]),
												longitude: parseFloat(lonMatch[1]),
											};
										}
									}
								}
								return null;
							});

							if (eapowCoords && eapowCoords.latitude && eapowCoords.longitude) {
								coords.latitude = eapowCoords.latitude;
								coords.longitude = eapowCoords.longitude;
								console.log(
									`  📍 Found eapowmapoptions coords: ${coords.latitude}, ${coords.longitude}`
								);
							} else {
								// Fallback: Try to find a Google Maps street view / maps link
								const gmHref = await detailPage.evaluate(() => {
									// First look for the gm-iv-address-link anchor
									const gm = document.querySelector(".gm-iv-address-link a");
									if (gm && gm.href) return gm.href;

									// Look for any anchor linking to Google Maps
									const anchors = Array.from(document.querySelectorAll("a"));
									for (const a of anchors) {
										const href = a.href || "";
										if (href.includes("google.com/maps") || href.includes("maps.google.com")) {
											// Prefer links that contain explicit coordinates or place/@ patterns
											if (
												href.includes("/@") ||
												href.includes("maps?ll=") ||
												/[?&](ll|center|q)=/.test(href) ||
												href.includes("/maps/place")
											) {
												return href;
											}
											// Otherwise return the first maps link found
											return href;
										}
									}

									// Some pages expose a tab that when clicked loads the map; try to find iframe inside the streetview plug
									const streetTab = document.querySelector(
										'a[data-toggle][href="#eapowstreetviewplug"]'
									);
									if (streetTab) {
										const iframe = document.querySelector("#eapowstreetviewplug iframe");
										if (iframe && iframe.src) return iframe.src;
									}

									return null;
								});

								if (gmHref) {
									// Try multiple coordinate extraction patterns from URL
									let m = null;

									// Pattern 1: @lat,long, (common in google maps URLs)
									m = gmHref.match(/@([\-0-9.]+),([\-0-9.]+)[,\/]/);
									if (m) {
										coords.latitude = parseFloat(m[1]);
										coords.longitude = parseFloat(m[2]);
									}

									// Pattern 2: query param ll=lat,long or center=lat,long
									if (!coords.latitude || !coords.longitude) {
										m = gmHref.match(/[?&](?:ll|center)=([\-0-9.]+),([\-0-9.]+)/);
										if (m) {
											coords.latitude = parseFloat(m[1]);
											coords.longitude = parseFloat(m[2]);
										}
									}

									// Pattern 3: q=lat,long (sometimes used)
									if (!coords.latitude || !coords.longitude) {
										m = gmHref.match(/[?&]q=([\-0-9.]+),([\-0-9.]+)/);
										if (m) {
											coords.latitude = parseFloat(m[1]);
											coords.longitude = parseFloat(m[2]);
										}
									}

									// Pattern 4: fallback - look for coordinate pair preceded by =, @, or space
									if (!coords.latitude || !coords.longitude) {
										m = gmHref.match(/[=@\s]([\-0-9.]+),([\-0-9.]+)/);
										if (m) {
											coords.latitude = parseFloat(m[1]);
											coords.longitude = parseFloat(m[2]);
										}
									}
								}
							}
						} catch (err) {
							// ignore
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
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 0; pg < propertyType.totalPages; pg++) {
			const start = pg * propertyType.recordsPerPage;
			const url = `${propertyType.urlBase}${start}`;
			requests.push({
				url,
				userData: { pageNum: pg + 1, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed MapEstateAgents - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeMapEstateAgents();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
