// Galbraithgroup scraper using Playwright with Crawlee and Camoufox
// Agent ID: 223
// Website: galbraithgroup.com
// Usage:
// node backend/scraper-agent-223.js

const { PlaywrightCrawler, log } = require("crawlee");
const { launchOptions } = require("camoufox-js");
const { firefox } = require("playwright");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 223;
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
		urlBase:
			"https://www.galbraithgroup.com/sales-and-lettings/search/?sq.BuyOrLet=true&sq.MaxDistance=30&sq.sq_stc=true&sq.Sort=newest",
		totalPages: 5, // TEST: 5 pages only
		recordsPerPage: 10,
		isRental: false,
		label: "SALES",
	},
];

async function scrapeGalbraithgroup() {
	console.log(`\n🚀 Starting Galbraithgroup scraper (Agent ${AGENT_ID})...`);
	console.log("⚠️ Running in DEBUG MODE: Browser will stay open if an error occurs.");

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 600,
		navigationTimeoutSecs: 120,
		useSessionPool: true,
		persistCookiesPerSession: true,

		launchContext: {
			launcher: firefox,
			launchOptions: await launchOptions({
				headless: false,
				args: ["--start-maximized"],
			}),
		},

		browserPoolOptions: {
			// Disable the default fingerprint spoofing to avoid conflicts with Camoufox
			useFingerprints: false,
		},

		preNavigationHooks: [
			async ({ page }, gotoOptions) => {
				await page.setViewportSize({ width: 1366, height: 768 });
				await page.setExtraHTTPHeaders({
					"Accept-Language": "en-US,en;q=0.9",
					"Upgrade-Insecure-Requests": "1",
				});
				gotoOptions.waitUntil = "domcontentloaded";
			},
		],

		async failedRequestHandler({ request }) {
			console.log(`⚠️ Forcing request handler despite status code for: ${request.url}`);
			// Do NOT throw an error here.
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;
			console.log(`\n📋 ${label} - Page ${pageNum} - ${request.url}`);

			try {
				// --- 🛡️ CLOUDFLARE BYPASS LOGIC 🛡️ ---
				console.log("⏳ Waiting for Cloudflare/Page Load (Max 30s)...");

				// Wait for the title to change from the Cloudflare challenge
				await page.waitForFunction(
					() => {
						const title = document.title;
						return !title.includes("Just a moment") && !title.includes("Cloudflare");
					},
					{ timeout: 30000 }
				);

				await page.waitForTimeout(3000); // Wait for final rendering/redirect

				// Random delay to avoid rate limiting (3-8 seconds)
				const delay = Math.floor(Math.random() * 5000) + 3000;
				await page.waitForTimeout(delay);
				console.log("✅ Cloudflare cleared. Waiting for content...");

				// Wait for property cards to load
				await page.waitForSelector("div[class*='carousel']", { timeout: 30000 });
				console.log("✅ Carousel selector found.");

				// Extract properties from the DOM
				const properties = await page.evaluate(() => {
					try {
						// Get all generic divs that contain property data - they are carousel containers
						const containers = Array.from(
							document.querySelectorAll("div[class*='generic']")
						).filter((el) => {
							// Look for elements containing bedroom icons (property cards)
							return el.querySelector("img[alt*='Bedroom Count']") !== null;
						});

						if (containers.length === 0) {
							console.log("No property containers found");
							return [];
						}

						return containers
							.map((container) => {
								try {
									// Extract title/heading
									const titleEl = container.querySelector("h2 a, h3 a");
									const title = titleEl ? titleEl.textContent.trim() : "";
									const link = titleEl ? titleEl.getAttribute("href") : null;
									const fullLink = link
										? link.startsWith("http")
											? link
											: "https://www.galbraithgroup.com" + link
										: null;

									// Extract location
									const locationEl = Array.from(container.querySelectorAll("p")).find(
										(p) =>
											p.textContent.includes("Scottish") ||
											p.textContent.includes("Northumberland") ||
											p.textContent.includes("Aberdeenshire") ||
											p.textContent.includes("Ayrshire") ||
											p.textContent.includes("Stirlingshire") ||
											p.textContent.includes("Highland") ||
											p.textContent.includes("Perthshire") ||
											p.textContent.includes("Galloway") ||
											p.textContent.includes("Borders")
									);
									const location = locationEl ? locationEl.textContent.trim() : "";

									// Extract price
									const priceTexts = Array.from(container.querySelectorAll("p")).map((p) =>
										p.textContent.trim()
									);
									const priceEl = priceTexts.find(
										(t) => t.includes("£") || t.includes("Offers Over")
									);
									const price = priceEl || "";

									// Extract bedrooms, receptions, bathrooms
									const bedroomImg = container.querySelector("img[alt*='Bedroom Count']");
									const bedroomEl =
										bedroomImg?.nextElementSibling || bedroomImg?.parentElement?.querySelector("p");
									const bedrooms = bedroomEl ? bedroomEl.textContent.trim() : null;

									const receptionImg = container.querySelector("img[alt*='Reception Count']");
									const receptionEl =
										receptionImg?.nextElementSibling ||
										receptionImg?.parentElement?.querySelector("p");
									const receptions = receptionEl ? receptionEl.textContent.trim() : null;

									const bathroomImg = container.querySelector("img[alt*='Bathroom Count']");
									const bathroomEl =
										bathroomImg?.nextElementSibling ||
										bathroomImg?.parentElement?.querySelector("p");
									const bathrooms = bathroomEl ? bathroomEl.textContent.trim() : null;

									// Extract acres/land info
									const acresEl = Array.from(container.querySelectorAll("p")).find((p) =>
										p.textContent.includes("acres")
									);
									const acres = acresEl ? acresEl.textContent.trim() : null;

									if (!fullLink || !title) {
										return null;
									}

									return {
										link: fullLink,
										title,
										location,
										price,
										bedrooms,
										receptions,
										bathrooms,
										acres,
									};
								} catch (e) {
									console.log("Error extracting property details:", e);
									return null;
								}
							})
							.filter((p) => p); // Remove null entries
					} catch (e) {
						console.log("Error extracting properties:", e);
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
							if (!property.link) return;

							// Random delay between detail page visits (1-3 seconds)
							await new Promise((resolve) => setTimeout(resolve, Math.random() * 2000 + 1000));

							let coords = { latitude: null, longitude: null };

							// Visit detail page to extract coordinates from GeoCoordinates JSON
							// KEY: Use page.context().newPage() to share the Cloudflare clearance cookie
							const detailPage = await page.context().newPage();
							try {
								await detailPage.goto(property.link, {
									waitUntil: "domcontentloaded",
									timeout: 40000,
								});
								await detailPage.waitForTimeout(1500);

								// Check for Cloudflare on detail page just in case
								await detailPage
									.waitForFunction(
										() => {
											const title = document.title;
											return !title.includes("Just a moment") && !title.includes("Cloudflare");
										},
										{ timeout: 10000 }
									)
									.catch(() => {}); // Don't crash if this times out

								const detailCoords = await detailPage.evaluate(() => {
									try {
										// Extract GeoCoordinates from script JSON-LD
										const scripts = Array.from(document.querySelectorAll("script"));
										for (const script of scripts) {
											const content = script.textContent;
											if (content.includes("GeoCoordinates")) {
												const geoMatch = content.match(/{\s*"@type"\s*:\s*"GeoCoordinates"[^}]*}/);
												if (geoMatch) {
													const geo = JSON.parse(geoMatch[0]);
													if (geo.latitude && geo.longitude) {
														return {
															lat: parseFloat(geo.latitude),
															lng: parseFloat(geo.longitude),
														};
													}
												}
											}
										}
										return { lat: null, lng: null };
									} catch (e) {
										return { lat: null, lng: null };
									}
								});

								if (detailCoords.lat && detailCoords.lng) {
									coords = { latitude: detailCoords.lat, longitude: detailCoords.lng };
								}
							} catch (err) {
								console.log(`⚠️ Error visiting detail page: ${property.link}`);
							} finally {
								await detailPage.close();
							}

							// Save property to database
							try {
								const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;

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

								console.log(
									`✅ ${property.title} - ${formatPrice(priceClean)} - ${
										coords.latitude && coords.longitude
											? `${coords.latitude}, ${coords.longitude}`
											: "No coords"
									}`
								);
							} catch (dbErr) {
								console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
							}
						})
					);
				}
			} catch (error) {
				console.error(`\n🛑 FATAL ERROR ON PAGE ${pageNum}: ${error.message}`);
				// This is the CRITICAL part for debugging: keep the browser open!
				console.log(
					"👀 Browser is PAUSED so you can inspect the page (e.g., check for a CAPTCHA)."
				);
				console.log("👉 Close the browser window manually when you are done.");

				await page.pause(); // Keeps the browser open indefinitely
				throw error; // Re-throw to inform Crawlee the request failed
			}
		},
	});

	try {
		// Add requests for each page
		for (const propType of PROPERTY_TYPES) {
			for (let pageNum = 1; pageNum <= propType.totalPages; pageNum++) {
				const pageSize = propType.recordsPerPage;
				const pageUrl =
					pageNum === 1
						? propType.urlBase
						: `${propType.urlBase}&sq.Page=${pageNum}&sq.PageSize=${pageSize}`;

				await crawler.addRequests([
					{
						url: pageUrl,
						uniqueKey: `${propType.label}-${pageNum}`,
						userData: {
							pageNum,
							isRental: propType.isRental,
							label: propType.label,
						},
					},
				]);
			}
		}

		// Run the crawler
		await crawler.run();

		console.log(`\n✨ Galbraithgroup scraper finished!`);
		console.log(`📊 Total scraped: ${totalScraped}`);
		console.log(`💾 Total saved: ${totalSaved}\n`);
	} catch (error) {
		// This catches high-level crawler errors (like failing to launch the browser)
		console.error("Crawler initiation/run error:", error);
	} finally {
		await crawler.teardown();
	}
}

// Run the scraper
(async () => {
	try {
		await scrapeGalbraithgroup();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
