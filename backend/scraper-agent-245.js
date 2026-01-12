// Beresfords scraper using Playwright with Crawlee
// Agent ID: 245
// Usage:
// node backend/scraper-agent-245.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 245;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(num, isRental) {
	if (!num || isNaN(num)) return isRental ? "£0 pcm" : "£0";
	return "£" + Number(num).toLocaleString("en-GB") + (isRental ? " pcm" : "");
}

// Configuration for Beresfords
// Sales: 66 pages, Lettings: 9 pages
const PROPERTY_TYPES = [
	{
		// Sales
		urlBase: "https://www.beresfords.co.uk/find-a-property/for-sale",
		totalPages: 66,
		isRental: false,
		label: "SALES",
	},
	{
		// Rentals
		urlBase: "https://www.beresfords.co.uk/find-a-property/to-rent",
		totalPages: 9,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeBeresfords() {
	console.log(`\n🚀 Starting Beresfords scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for the property grid to load
			await page
				.waitForSelector("a[href*='/property/']", { timeout: 30000 })
				.catch(() => console.log(`⚠️ No properties found on page ${pageNum}`));

			const properties = await page.evaluate((isRental) => {
				try {
					// Properties are usually inside links that contain /property/
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

						// Attempt to get title and price from the card
						const container = el.closest("div"); // Adjust if structure is different
						const title = el.querySelector("h2")?.textContent?.trim() || "Property";

						// Some cards might have "Sold STC" or "Let Agreed" tags
						const cardHtml = container?.innerHTML || el.innerHTML;
						if (
							cardHtml.includes("Sold STC") ||
							cardHtml.includes("Let Agreed") ||
							cardHtml.includes("Sale Agreed")
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
								timeout: 45000,
							});

							// Beresfords uses JSON-LD and Google Maps links
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
										} catch (e) {}
									}

									// 2. Fallback for price/bedrooms from DOM if JSON-LD failed
									if (!data.price) {
										const priceSelector = ".property-price, .asking-price, h3.price";
										const priceEl = document.querySelector(priceSelector);
										if (priceEl) data.price = priceEl.textContent;
									}
									if (!data.bedrooms) {
										// Look for the "X Bed" string in common containers
										const text = document.body.innerText;
										const bedMatch = text.match(/(\d+)\s*Bed/i);
										if (bedMatch) data.bedrooms = bedMatch[1];
									}

									// 3. Coordinate extraction
									// First, check the LocRating plugin script
									const allScripts = Array.from(document.querySelectorAll("script"));
									for (const s of allScripts) {
										const content = s.textContent;
										if (content.includes("loadLocratingPlugin")) {
											const latMatch = content.match(/lat:\s*([-0-9.]+)/);
											const lngMatch = content.match(/lng:\s*([-0-9.]+)/);
											if (latMatch && lngMatch) {
												const parsedLat = parseFloat(latMatch[1]);
												const parsedLng = parseFloat(lngMatch[1]);
												data.lat = isNaN(parsedLat) ? null : parsedLat;
												data.lng = isNaN(parsedLng) ? null : parsedLng;
												break;
											}
										}
									}

									// Second, check Google Maps links
									if (!data.lat || isNaN(data.lat)) {
										const mapsLink = document.querySelector("a[href*='maps.google.com/maps?ll=']");
										if (mapsLink) {
											const href = mapsLink.getAttribute("href");
											const match = href.match(/ll=([-0-9.]+),([-0-9.]+)/);
											if (match) {
												const parsedLat = parseFloat(match[1]);
												const parsedLng = parseFloat(match[2]);
												data.lat = isNaN(parsedLat) ? null : parsedLat;
												data.lng = isNaN(parsedLng) ? null : parsedLng;
											}
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

								console.log(
									`✅ ${address.substring(0, 30)} - ${formatPrice(priceClean, isRental)} - ${
										property.link
									}`
								);
								totalSaved++;
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
			// Using the URL structure verified: /page/1/
			const url = `${propertyType.urlBase}/page/${pg}/?location=&radius=0&bedsMin=0&priceMin=0&priceMax=0&type=all&branch=&tag=&showUnavailable=false&order=highest-to-lowest`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Beresfords - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeBeresfords();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
