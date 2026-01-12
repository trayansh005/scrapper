// Newton Fallowell scraper using Playwright with Crawlee
// Agent ID: 248
// Usage:
// node backend/scraper-agent-248.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 248;
let totalScraped = 0;
let totalSaved = 0;
let savedSales = 0;
let savedRentals = 0;

function formatPrice(num, isRental) {
	if (!num || isNaN(num)) return isRental ? "£0 pcm" : "£0";
	return "£" + Number(num).toLocaleString("en-GB") + (isRental ? " pcm" : "");
}

// Configuration for Newton Fallowell
// Sales: 139 pages, Lettings: ~18 pages (rough estimates)
const PROPERTY_TYPES = [
	{
		// Sales
		urlBase: "https://www.newtonfallowell.co.uk/properties/sales",
		totalPages: 139,
		isRental: false,
		label: "SALES",
	},
	{
		// Rentals
		urlBase: "https://www.newtonfallowell.co.uk/properties/lettings",
		totalPages: 18,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeNewtonFallowell() {
	console.log(`\n🚀 Starting Newton Fallowell scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 5,
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

			// Wait for the property cards to load
			await page
				.waitForSelector("a[href*='/property/']", { timeout: 30000 })
				.catch(() => console.log(`⚠️ No properties found on page ${pageNum}`));

			const properties = await page.evaluate((isRental) => {
				try {
					// Newton Fallowell property links are in cards like /property/[slug]-P[id]-[branch]/
					const items = Array.from(document.querySelectorAll("a[href*='/property/']"));
					const seenLinks = new Set();
					const results = [];

					for (const el of items) {
						let href = el.getAttribute("href");
						if (!href) continue;

						const link = href.startsWith("http")
							? href
							: new URL(href, window.location.origin).href;

						if (seenLinks.has(link)) continue;
						seenLinks.add(link);

						// Skip if it doesn't look like a property page
						if (!link.includes("/property/")) continue;

						// Attempt to get title from the card heading
						const container =
							el.closest("div[class*='property']") || el.closest("article") || el.closest("div");
						const title = el.querySelector("h")?.textContent?.trim() || "Property";

						// Check for "Sale Agreed" or "Let Agreed" status
						const cardHtml = container?.innerHTML || el.innerHTML;
						if (
							cardHtml.includes("Sale Agreed") ||
							cardHtml.includes("Let Agreed") ||
							cardHtml.includes("Sold STC")
						) {
							continue;
						}

						results.push({ link, title });
					}
					return results;
				} catch (e) {
					return [];
				}
			}, isRental);

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 90000,
							});

							// Newton Fallowell uses JSON-LD structured data with geo coordinates
							const detailData = await detailPage.evaluate(() => {
								try {
									const data = {
										price: null,
										bedrooms: null,
										address: null,
										lat: null,
										lng: null,
									};

									// 1. JSON-LD extraction
									const scripts = Array.from(
										document.querySelectorAll("script[type='application/ld+json']")
									);
									for (const script of scripts) {
										try {
											const json = JSON.parse(script.textContent);

											// Check if this is a RealEstateAgent schema (likely the main schema)
											if (json["@type"] === "RealEstateAgent" && json.geo) {
												// This is the property schema with coordinates
												data.lat = json.geo.latitude;
												data.lng = json.geo.longitude;
											}

											// Look for Offer schema with price and bedrooms
											if (
												json["@type"] === "Offer" ||
												(json["@graph"] && Array.isArray(json["@graph"]))
											) {
												const findOffer = (obj) => {
													if (!obj) return null;
													if (Array.isArray(obj)) {
														for (const item of obj) {
															const found = findOffer(item);
															if (found) return found;
														}
													}
													if (obj["@type"] === "Offer") return obj;
													if (obj["@graph"]) return findOffer(obj["@graph"]);
													if (obj.itemOffered) return findOffer(obj.itemOffered);
													return null;
												};

												const offerObj = findOffer(json);
												if (offerObj) {
													const item = offerObj.itemOffered || offerObj;
													if (item.numberOfBedrooms) data.bedrooms = item.numberOfBedrooms;
													if (item.address) {
														if (typeof item.address === "string") data.address = item.address;
														else if (item.address.streetAddress) {
															data.address = `${item.address.streetAddress}, ${
																item.address.addressLocality || ""
															} ${item.address.postalCode || ""}`.trim();
														}
													}
													if (offerObj.price) data.price = offerObj.price;
												}
											}
										} catch (e) {}
									}

									// 2. Extract from page headings if JSON-LD doesn't have all info
									if (!data.address) {
										const h1 = document.querySelector("h1");
										if (h1) {
											const parts = h1.textContent
												.split("\n")
												.map((p) => p.trim())
												.filter((p) => p);
											if (parts.length >= 2) {
												data.address = parts.slice(0, 2).join(", ");
											} else {
												data.address = h1.textContent.trim();
											}
										}
									}

									// 3. Extract price from heading if not in JSON-LD
									if (!data.price) {
										const priceEl = document.querySelector("[class*='price']");
										if (priceEl) data.price = priceEl.textContent;
									}

									// 4. Extract bedrooms from page content or spec elements
									if (!data.bedrooms) {
										// Try finding bedroom count from bedrooms spec element
										const bedroomEl = document.querySelector('[class*="bedroom"]');
										if (bedroomEl) {
											const bedText = bedroomEl.textContent.trim();
											const bedNum = bedText.match(/^\d+/);
											if (bedNum) data.bedrooms = parseInt(bedNum[0]);
										}

										// Fallback: search in body text
										if (!data.bedrooms) {
											const text = document.body.innerText;
											const bedMatch = text.match(/(\d+)\s*Bedroom/i);
											if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
										}
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

								const bedrooms = detailData.bedrooms || null;
								const address = detailData.address || property.title || "Property";

								await updatePriceByPropertyURL(
									property.link.trim(),
									priceClean || null,
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
			// Newton Fallowell uses pagination with per_page parameter
			// Sales URL structure verified: /properties/sales/?per_page=11&...&pg=N
			const url = `${
				propertyType.urlBase
			}/?per_page=11&drawMap=&address=&address_lat_lng=&price_min=&price_max=&bedrooms_min=-1${
				!propertyType.isRental ? "&hide_under_offer=on" : "&hide_let_agreed=on"
			}&yield_min=&yield_max=&pg=${pg}`;

			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Newton Fallowell - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
	console.log(`📊 Breakdown — SALES: ${savedSales}, LETTINGS: ${savedRentals}`);
}

(async () => {
	try {
		await scrapeNewtonFallowell();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
