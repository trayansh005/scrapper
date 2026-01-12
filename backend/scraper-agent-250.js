// Charters Estate Agents scraper using Playwright with Crawlee
// Agent ID: 250
// Usage:
// node backend/scraper-agent-250.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 250;
let totalScraped = 0;
let totalSaved = 0;
const processedUrls = new Set();

function formatPrice(num) {
	if (!num || isNaN(num)) return "£0";
	return "£" + Number(num).toLocaleString("en-GB");
}

// Charters uses a JS-rendered listing, similar to other Starberry/Lomond sites.
// If you notice it stopping early/late, tweak totalPages.
const PROPERTY_TYPES = [
	{
		urlBase:
			"https://www.chartersestateagents.co.uk/property/for-sale/in-hampshire-and-surrey/exclude-sale-agreed/",
		totalPages: 52,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase:
			"https://www.chartersestateagents.co.uk/property/to-rent/in-hampshire-and-surrey/exclude-let-agreed/",
		totalPages: 11,
		isRental: true,
		label: "RENTALS",
	},
];

function buildPagedUrl(urlBase, pageNum) {
	if (pageNum === 1) return urlBase;
	// Charters pagination: append /page-N/ to base URL
	return `${urlBase.endsWith("/") ? urlBase : urlBase + "/"}page-${pageNum}/`;
}

async function scrapeCharters() {
	console.log(`\n🚀 Starting Charters scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for the JS-rendered property links to appear
			await page
				.waitForSelector('a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]', {
					timeout: 20000,
				})
				.catch(() => console.log(`⚠️ No properties found on page ${pageNum}`));

			// Small extra wait to let cards fully render
			await page.waitForTimeout(1500);

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(
							'a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]'
						)
					);
					const seenLinks = new Set();
					const results = [];

					for (const el of items) {
						let href = el.getAttribute("href");
						if (!href) continue;

						const link = href.startsWith("http")
							? href
							: new URL(href, window.location.origin).href;

						// Skip non-property actions
						if (link.includes("/book-a-viewing/") || link.includes("/myaccount")) continue;
						if (seenLinks.has(link)) continue;
						seenLinks.add(link);

						// Attempt to skip sale-agreed style cards if they slip through
						const cardText = (el.closest("article") || el.closest("li") || el).innerText || "";
						if (
							/sale agreed|sold stc|under offer|let agreed/i.test(cardText) ||
							(/\blet\b/i.test(cardText) && !/\bto let\b/i.test(cardText))
						) {
							continue;
						}

						const title =
							el.querySelector("h3")?.textContent?.trim() || el.textContent?.trim() || "Property";

						results.push({ link, title });
					}

					return results;
				} catch (e) {
					return [];
				}
			});

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
								// Charters details often hydrate price/JSON-LD after DOMContentLoaded
								waitUntil: "networkidle",
								timeout: 90000,
							});

							// Give late-hydrating price/JSON-LD a chance to render
							await detailPage
								.waitForFunction(
									() => {
										const hasJsonLd = !!document.querySelector(
											"script[type='application/ld+json']"
										);
										const text = document.body?.innerText || "";
										const hasPrice = /£\s*[\d,]{3,}/.test(text);
										return hasJsonLd || hasPrice;
									},
									{ timeout: 10000 }
								)
								.catch(() => {});

							// Scroll to the Location section so the map iframe populates lat/lng
							try {
								await detailPage.evaluate(() => {
									const locationHeading = Array.from(document.querySelectorAll(".h3, h2, h3")).find(
										(el) => (el.textContent || "").trim().toLowerCase() === "location"
									);
									const target =
										locationHeading ||
										document.querySelector("#map-holder") ||
										document.querySelector("iframe#location-map");
									if (target && target.scrollIntoView) {
										target.scrollIntoView({ block: "center", inline: "nearest" });
									} else {
										window.scrollTo(0, document.body.scrollHeight);
									}
								});
								await detailPage.waitForTimeout(1500);
								await detailPage
									.waitForSelector('iframe#location-map[src*="lat="]', { timeout: 7000 })
									.catch(() => {});
							} catch (e) {}

							const detailData = await detailPage.evaluate(() => {
								try {
									const data = {
										price: null,
										bedrooms: null,
										address: null,
										lat: null,
										lng: null,
									};

									// 1) Coordinates from the Location iframe (preferred)
									const locIframe = document.querySelector("iframe#location-map");
									const locSrc = locIframe?.getAttribute("src") || "";
									if (locSrc.includes("lat=") && locSrc.includes("lng=")) {
										const latMatch = locSrc.match(/lat=([0-9.-]+)/);
										const lngMatch = locSrc.match(/lng=([0-9.-]+)/);
										if (latMatch) data.lat = parseFloat(latMatch[1]);
										if (lngMatch) data.lng = parseFloat(lngMatch[1]);
									}

									// Fallback: other iframes with lat/lng
									if (!data.lat) {
										const iframes = Array.from(document.querySelectorAll("iframe"));
										for (const iframe of iframes) {
											const src = iframe.getAttribute("src") || "";
											if (!src.includes("lat=") || !src.includes("lng=")) continue;
											const latMatch = src.match(/lat=([0-9.-]+)/);
											const lngMatch = src.match(/lng=([0-9.-]+)/);
											if (latMatch) data.lat = parseFloat(latMatch[1]);
											if (lngMatch) data.lng = parseFloat(lngMatch[1]);
											if (data.lat) break;
										}
									}

									// 2) JSON-LD often contains Offer/Place
									const scripts = Array.from(
										document.querySelectorAll("script[type='application/ld+json']")
									);
									for (const script of scripts) {
										try {
											const json = JSON.parse(script.textContent);
											if (json["@graph"] && Array.isArray(json["@graph"])) {
												for (const item of json["@graph"]) {
													if (item["@type"] === "Offer") {
														const p = item.price ?? item.priceSpecification?.price;
														if (p != null) data.price = String(p);
													}
													if (item["@type"] === "Place" && item.address) {
														const addr = item.address;
														if (addr.streetAddress) {
															data.address = `${addr.streetAddress}, ${
																addr.addressLocality || ""
															} ${addr.postalCode || ""}`.trim();
														}
													}
												}
											}
											if (
												(json["@type"] === "Offer" || json["@type"] === "Product") &&
												json.price
											) {
												data.price = json.price.toString();
											}
											if (
												(json["@type"] === "Place" || json["@type"] === "Residence") &&
												json.name
											) {
												if (!data.address) data.address = json.name;
											}
										} catch (e) {}
									}

									// 3) Address fallback
									if (!data.address) {
										const h1 = document.querySelector("h1");
										if (h1) data.address = h1.textContent.trim();
									}

									// 4) Price fallback from visible text (sales)
									if (!data.price) {
										const text = document.body.innerText || "";
										// Prefer things like "Guide Price £450,000" or "£450,000"
										const m = text.match(/£\s*([\d,]{3,})/);
										if (m) data.price = m[1].replace(/,/g, "");
									}

									// 5) Bedrooms fallback
									if (!data.bedrooms) {
										const h4 = document.querySelector("h4");
										const h4Text = h4?.textContent || "";
										const bedMatch = h4Text.match(/(\d+)\s*bedroom/i);
										if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
									}
									if (!data.bedrooms) {
										const text = document.body.innerText || "";
										const bedMatch = text.match(/(\d+)\s*bedroom/i);
										if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
									}

									return data;
								} catch (e) {
									return null;
								}
							});

							if (detailData) {
								const rawPrice = (detailData.price || "").toString();
								const numMatch = rawPrice.match(/[0-9][0-9,\.\s]*/);
								const priceClean = numMatch ? numMatch[0].replace(/[^0-9]/g, "") : "";

								const address = detailData.address || property.title || "Property";
								const bedrooms = detailData.bedrooms || null;

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

									console.log(
										`✅ [${label}] ${address.substring(0, 40)} - ${formatPrice(priceClean)} - ${
											property.link
										}`
									);
									totalSaved++;
								} else {
									console.log(`⚠️ Skipping update (no price found): ${property.link}`);
								}

								totalScraped++;
							}
						} catch (err) {
							console.log(`⚠️ Error processing ${property.link}: ${err.message}`);
						} finally {
							await detailPage.close();
						}
					})
				);
			}
		},
	});

	const requests = [];
	for (const cfg of PROPERTY_TYPES) {
		console.log(`🏠 Enqueuing ${cfg.label} (${cfg.totalPages} pages)`);
		for (let pageNum = 1; pageNum <= cfg.totalPages; pageNum++) {
			requests.push({
				url: buildPagedUrl(cfg.urlBase, pageNum),
				userData: {
					pageNum,
					isRental: cfg.isRental,
					label: cfg.label,
				},
			});
		}
	}

	await crawler.run(requests);

	console.log(`✅ Completed Charters - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
	await updateRemoveStatus(AGENT_ID);
	console.log("\n✅ All done!\n");
}

scrapeCharters().catch((err) => {
	console.error("❌ Scraper failed:", err);
	process.exit(1);
});
