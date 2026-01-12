// Linley & Simpson scraper using Playwright with Crawlee
// Agent ID: 249
// Usage:
// node backend/scraper-agent-249.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 249;
let totalScraped = 0;
let totalSaved = 0;
let savedSales = 0;
let savedRentals = 0;
const processedUrls = new Set();

function formatPrice(num, isRental) {
	if (!num || isNaN(num)) return isRental ? "£0 pcm" : "£0";
	return "£" + Number(num).toLocaleString("en-GB") + (isRental ? " pcm" : "");
}

// Configuration for Linley & Simpson
// To-rent: 444 properties (25 pages), For-sale: TBD (will discover)
const PROPERTY_TYPES = [
	{
		// To-rent (Lettings)
		urlBase: "https://www.linleyandsimpson.co.uk/property/to-rent/in-yorkshire/exclude-let-agreed",
		totalPages: 25,
		isRental: true,
		label: "LETTINGS",
	},
	// {
	// 	// For-sale
	// 	urlBase:
	// 		"https://www.linleyandsimpson.co.uk/property/for-sale/in-yorkshire/exclude-sale-agreed",
	// 	totalPages: 17,
	// 	isRental: false,
	// 	label: "SALES",
	// },
];

async function scrapeLinleyAndSimpson() {
	console.log(`\n🚀 Starting Linley & Simpson scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
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

			// IMPORTANT: Linley & Simpson loads properties dynamically via JS
			// We need to wait for the property listings to appear
			await page
				.waitForSelector('a[href*="/property-to-rent/"], a[href*="/property-for-sale/"]', {
					timeout: 10000,
				})
				.catch(() => console.log(`⚠️ No properties found on page ${pageNum}`));

			// Wait additional time for all properties to render
			await page.waitForTimeout(2000);

			const properties = await page.evaluate((isRental) => {
				try {
					// Linley & Simpson property links: /property-to-rent/[description]-[id]/ or /property-for-sale/[description]-[id]/
					const linkPattern = isRental ? "/property-to-rent/" : "/property-for-sale/";
					const items = Array.from(document.querySelectorAll(`a[href*="${linkPattern}"]`));
					const seenLinks = new Set();
					const results = [];

					for (const el of items) {
						let href = el.getAttribute("href");
						if (!href) continue;

						const link = href.startsWith("http")
							? href
							: new URL(href, window.location.origin).href;

						// Skip if we've seen this link or if it's a booking/viewing link
						if (seenLinks.has(link) || link.includes("/book-a-viewing/")) continue;
						seenLinks.add(link);

						// Get title from the link or nearby heading
						const title =
							el.querySelector("h3")?.textContent?.trim() || el.textContent?.trim() || "Property";

						results.push({ link, title });
					}
					return results;
				} catch (e) {
					return [];
				}
			}, isRental);

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 3;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						// Global deduplication in the same run
						if (processedUrls.has(property.link)) {
							console.log(`⏩ Skipping duplicate URL: ${property.link.substring(0, 60)}...`);
							return;
						}
						processedUrls.add(property.link);

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 90000,
							});

							// Wait for page to fully load (including iframe)
							await detailPage.waitForTimeout(1500);

							// Location map iframe lazy-loads; scroll to it and wait for src to contain lat/lng
							try {
								const locationAnchor = detailPage
									.locator("#map-holder, iframe#location-map")
									.first();
								if ((await locationAnchor.count()) > 0) {
									await locationAnchor.scrollIntoViewIfNeeded({ timeout: 8000 });
								} else {
									await detailPage.evaluate(() => {
										const scrollEl = document.scrollingElement || document.documentElement;
										if (scrollEl) window.scrollTo(0, scrollEl.scrollHeight);
									});
								}

								await detailPage.waitForTimeout(1500);
								await detailPage.waitForFunction(
									() => {
										const iframe = document.querySelector("iframe#location-map");
										const src = iframe && (iframe.getAttribute("src") || iframe.src);
										return !!src && src.includes("lat=") && src.includes("lng=");
									},
									{ timeout: 10000 }
								);
							} catch (e) {
								console.log(`⚠️ Location iframe not ready: ${e?.message || e}`);
							}

							// Linley & Simpson stores coordinates in iframe src URL
							const detailData = await detailPage.evaluate(() => {
								try {
									const data = {
										price: null,
										bedrooms: null,
										address: null,
										lat: null,
										lng: null,
									};

									// 0. Prefer the Location section iframe (contains lat/lng in src)
									const locationIframe = document.querySelector("iframe#location-map");
									if (locationIframe) {
										const src = locationIframe.getAttribute("src") || locationIframe.src;
										if (src) {
											const latMatch = src.match(/[?&]lat=([0-9.-]+)/);
											const lngMatch = src.match(/[?&]lng=([0-9.-]+)/);
											if (latMatch) data.lat = parseFloat(latMatch[1]);
											if (lngMatch) data.lng = parseFloat(lngMatch[1]);
										}
									}

									// 0. Extract coordinates from UI elements (User suggestion: .streetview_toggle)
									const mapElements = Array.from(
										document.querySelectorAll(".streetview_toggle, .map_toggle, [data-lat]")
									);
									for (const el of mapElements) {
										// Check data attributes
										let lat = el.getAttribute("data-lat") || el.getAttribute("lat");
										let lng = el.getAttribute("data-lng") || el.getAttribute("lng");

										// Check onclick if no attributes
										if (!lat) {
											const onclick = el.getAttribute("onclick");
											if (onclick) {
												const matches = onclick.match(/([0-9.-]{4,}),\s*([0-9.-]{4,})/);
												if (matches) {
													lat = matches[1];
													lng = matches[2];
												}
											}
										}

										if (lat && lng) {
											data.lat = parseFloat(lat);
											data.lng = parseFloat(lng);
											break;
										}
									}

									// 1. Extract coordinates from ANY iframe src
									if (!data.lat) {
										const iframes = Array.from(document.querySelectorAll("iframe"));
										for (const iframe of iframes) {
											const src = iframe.src;
											if (src && (src.includes("lat=") || src.includes("maps?q="))) {
												// Try lat= pattern
												const latMatch = src.match(/lat=([0-9.-]+)/);
												const lngMatch = src.match(/lng=([0-9.-]+)/);
												if (latMatch) data.lat = parseFloat(latMatch[1]);
												if (lngMatch) data.lng = parseFloat(lngMatch[1]);

												// Try q=lat,lng pattern
												if (!data.lat) {
													const qMatch = src.match(/q=([0-9.-]+),([0-9.-]+)/);
													if (qMatch) {
														data.lat = parseFloat(qMatch[1]);
														data.lng = parseFloat(qMatch[2]);
													}
												}

												if (data.lat) break;
											}
										}
									}

									// 2. Check JSON-LD for price, address and other details
									const scripts = Array.from(
										document.querySelectorAll("script[type='application/ld+json']")
									);
									for (const script of scripts) {
										try {
											const json = JSON.parse(script.textContent);

											// Look for @graph array
											if (json["@graph"] && Array.isArray(json["@graph"])) {
												for (const item of json["@graph"]) {
													if (item["@type"] === "Place" && item.address) {
														const addr = item.address;
														if (addr.streetAddress) {
															data.address = `${addr.streetAddress}, ${
																addr.addressLocality || ""
															} ${addr.postalCode || ""}`.trim();
														}
													}
													if (item["@type"] === "Offer" && item.price) {
														data.price = item.price.toString();
													}
												}
											}
											// Look for direct Residence/Offer
											if (
												(json["@type"] === "Residence" ||
													json["@type"] === "SingleFamilyResidence") &&
												json.name
											) {
												if (!data.address) data.address = json.name;
											}
											if (
												(json["@type"] === "Offer" || json["@type"] === "Product") &&
												json.price
											) {
												data.price = json.price.toString();
											}
										} catch (e) {}
									}

									// 3. Extract address from h1 if not found
									if (!data.address) {
										const h1 = document.querySelector("h1");
										if (h1) data.address = h1.textContent.trim();
									}

									// 4. Robust Price Pattern Matching
									if (!data.price) {
										// Helper to normalize text
										const getText = (el) => (el ? el.innerText || el.textContent : "");

										// Try specific price containers if they exist (common patterns)
										const checkSelectors = [
											"div[class*='price']",
											"span[class*='price']",
											"h2",
											"h3",
											".banner-text",
											".overlay-text",
										];

										for (const sel of checkSelectors) {
											const els = document.querySelectorAll(sel);
											for (const el of els) {
												const txt = getText(el);
												// Strictly look for £ followed by numbers and pcm
												const match = txt.match(/£\s*([\d,]+)\s*p\.?c\.?m\.?/i);
												if (match) {
													data.price = match[1].replace(/,/g, "");
													break;
												}
											}
											if (data.price) break;
										}

										// Fallback: search entire body text with regex
										if (!data.price) {
											const bodyText = `${document.body?.innerText || ""}\n${
												document.body?.textContent || ""
											}`;
											// Look for £xxx pcm
											const pcmMatch = bodyText.match(/£\s*([\d,]+)\s*p\.?c\.?m\.?/i);
											if (pcmMatch) {
												data.price = pcmMatch[1].replace(/,/g, "");
											} else {
												// Look for just £xxx if it's prominently displayed (risky but needed)
												// We look for £ followed by 3-4 digits, maybe comma
												// Avoid "Deposit: £xxx"
												const rawMatches = bodyText.matchAll(/(?:^|\s|>)£\s*([\d,]+)(?:\s|<|$)/g);
												for (const m of rawMatches) {
													const val = parseInt(m[1].replace(/,/g, ""));
													// Filter out small amounts (holding deposits) and huge amounts (sale prices if mixed?)
													// Rent usually 300 - 10000.
													if (val > 300 && val < 20000) {
														data.price = m[1].replace(/,/g, "");
														break;
													}
												}
											}
										}
									}

									// 5. Extract bedrooms
									const h4 = document.querySelector("h4");
									if (h4) {
										const bedMatch = h4.textContent.match(/(\d+)\s*bedroom/i);
										if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
									}
									if (!data.bedrooms) {
										const text = document.body.innerText;
										const bedMatch = text.match(/(\d+)\s*bedroom/i);
										if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
									}

									return data;
								} catch (e) {
									return { error: e.toString() };
								}
							});

							if (detailData && !detailData.error) {
								const rawPrice = (detailData.price || "").toString();
								const numMatch = rawPrice.match(/[0-9][0-9,\.\s]*/);
								const priceClean = numMatch ? numMatch[0].replace(/[^0-9]/g, "") : "";

								const bedrooms = detailData.bedrooms || null;
								const address = detailData.address || property.title || "Property";

								if (priceClean) {
									await updatePriceByPropertyURL(
										property.link.trim(),
										priceClean,
										address,
										bedrooms,
										AGENT_ID,
										isRental,
										detailData.lat,
										detailData.lng
									);

									const categoryLabel = isRental ? "LETTINGS" : "SALES";
									console.log(
										`✅ [${categoryLabel}] ${address.substring(0, 40)} - ${formatPrice(
											priceClean,
											isRental
										)} - ${property.link}`
									);
									totalSaved++;
									if (isRental) savedRentals++;
									else savedSales++;
									totalScraped++;
								} else {
									console.log(`⚠️ Skipping update (no price found): ${property.link}`);
								}
							}
						} catch (err) {
							console.log(`⚠️ Error processing ${property.link}: ${err.message}`);
						} finally {
							await detailPage.close();
						}
					})
				);

				await new Promise((resolve) => setTimeout(resolve, 500));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue all pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Enqueuing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			// Linley & Simpson pagination: append /page-N/ to base URL
			const url = pg === 1 ? `${propertyType.urlBase}/` : `${propertyType.urlBase}/page-${pg}/`;

			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Linley & Simpson - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
	console.log(`📊 Breakdown — SALES: ${savedSales}, LETTINGS: ${savedRentals}`);
}

(async () => {
	try {
		await scrapeLinleyAndSimpson();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
