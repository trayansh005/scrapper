// Kinleigh Folkard & Hayward (KFH) scraper using Playwright with Crawlee
// Agent ID: 75
// Modeled after agent 84 with full URL pagination (one by one)
//
// Usage:
// node backend/scraper-agent-75.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce logging noise
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 75;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Configuration for sales and rentals — full KFH URLs
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.kfh.co.uk/property/for-sale/in-london/exclude-sale-agreed/",
	// 	totalRecords: 1689,
	// 	totalPages: 94,
	// 	recordsPerPage: 18,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlBase: "https://www.kfh.co.uk/property/to-rent/in-london/exclude-let-agreed/",
		totalRecords: 691,
		totalPages: 39,
		recordsPerPage: 18,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeKFH() {
	console.log(`\n🚀 Starting KFH scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for page to load
			await page.waitForTimeout(6000);

			// Accept cookies if banner is present
			const cookieButton = await page.$("#onetrust-accept-btn-handler");
			if (cookieButton) {
				await cookieButton.click();
				await page.waitForTimeout(2000);
			}

			// Try to wait for a known listing container
			await page
				.waitForSelector(
					".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0, .property-card, .result-card",
					{
						timeout: 40000,
					}
				)
				.catch(() => {
					console.log(`⚠️ No listing container found on page ${pageNum}`);
				});

			// Extract properties — try client-side JS first, then DOM
			const properties = await page.evaluate(() => {
				try {
					// Fallback: parse DOM
					const cardSelector = ".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0";
					const cards = Array.from(document.querySelectorAll(cardSelector));

					return cards
						.map((card) => {
							try {
								// Link: prefer property detail links
								let link = null;
								const anchors = card.querySelectorAll("a[href]");
								for (const a of anchors) {
									const href = a.getAttribute("href");
									if (
										href &&
										(href.includes("/property-for-sale/") || href.includes("/property-to-rent/"))
									) {
										link = a.href;
										break;
									}
									if (href && href.includes("/property")) {
										link = a.href;
										break;
									}
								}

								// Title: extract from h3 or PropertyCard__StyledAddressLink
								let title =
									card.querySelector("h3")?.textContent?.trim() ||
									card
										.querySelector("a[class*='PropertyCard__StyledAddressLink']")
										?.textContent?.trim() ||
									null;

								// Price: extract from .highlight-text or h2 within PropertyPriceAndStatus__StyledPrice
								let price =
									card.querySelector(".highlight-text")?.textContent?.trim() ||
									card
										.querySelector(".PropertyPriceAndStatus__StyledPrice-sc-1dv7ovq-0 h2")
										?.textContent?.trim() ||
									card
										.querySelector("p[class*='PropertyPriceAndStatus__StyledPrice']")
										?.textContent?.trim() ||
									card.querySelector(".PropertyCard__StyledPrice")?.textContent?.trim() ||
									"";

								// Bedrooms: extract from icons or spans
								let bedrooms = null;

								// New site icons: .p-bed, .p-bath, etc. Usually text is in the parent or sibling
								const bedSpan = card.querySelector(".p-bed");
								if (bedSpan) {
									// Often the text "N bedrooms" is in the parent anchor or next sibling
									const text = bedSpan.parentElement?.textContent?.trim() || "";
									const match = text.match(/(\d+)\s+bedrooms?/i);
									if (match) bedrooms = match[1];
								}

								if (!bedrooms) {
									const bedIcon = card.querySelector(".icon-bed");
									if (bedIcon) {
										bedrooms = bedIcon.nextElementSibling?.textContent?.trim();
									}
								}

								if (!bedrooms) {
									const bedSpans = card.querySelectorAll(
										"span[class*='PropertyMeta__StyledMetaItem'], .property-meta-item"
									);
									for (const span of bedSpans) {
										const text = span.textContent?.trim() || "";
										if (text.toLowerCase().includes("bedroom")) {
											const match = text.match(/(\d+)/);
											bedrooms = match ? match[1] : null;
											break;
										}
									}
								}

								if (link && title) {
									// Fallback: extract bedrooms from URL if still not found
									if (!bedrooms && link) {
										const urlBedMatch = link.match(/(\d+)-bedroom/i);
										if (urlBedMatch) bedrooms = urlBedMatch[1];
									}
									return { link, title, price, bedrooms };
								}
								return null;
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

			// Process properties in batches (concurrent processing like agent 84)
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						let coords = { latitude: null, longitude: null };

						// Open detail page in a new context to extract coordinates
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(2000);

							// Extract coordinates from JSON-LD, iframe, or window objects
							const detailCoords = await detailPage.evaluate(() => {
								// 1. Try all JSON-LD scripts
								const ldScripts = document.querySelectorAll('script[type="application/ld+json"]');
								for (const script of ldScripts) {
									try {
										const data = JSON.parse(script.textContent);
										const items = Array.isArray(data) ? data : [data];
										for (const item of items) {
											if (item?.geo?.latitude && item?.geo?.longitude) {
												return {
													latitude: parseFloat(item.geo.latitude),
													longitude: parseFloat(item.geo.longitude),
												};
											}
											if (item?.latitude && item?.longitude) {
												return {
													latitude: parseFloat(item.latitude),
													longitude: parseFloat(item.longitude),
												};
											}
										}
									} catch (e) {}
								}

								// 2. Try iframe src (location-map)
								const iframe = document.querySelector(
									"#location-map, .location-map, iframe[src*='maps']"
								);
								if (iframe && iframe.src) {
									const latMatch = iframe.src.match(/lat=([0-9.-]+)/);
									const lngMatch = iframe.src.match(/lng=([0-9.-]+)/);
									if (latMatch && lngMatch) {
										return {
											latitude: parseFloat(latMatch[1]),
											longitude: parseFloat(lngMatch[1]),
										};
									}
								}

								// 3. Try window objects (Gatsby / React states)
								const possibleState =
									window.__initialState || window.__PRELOADED_STATE__ || window.___INITIAL_STATE__;
								if (possibleState && possibleState.property) {
									const p = possibleState.property;
									if (p.latitude && p.longitude) {
										return {
											latitude: parseFloat(p.latitude),
											longitude: parseFloat(p.longitude),
										};
									}
								}

								// 4. Try searching for lat/lng in all text if everything else fails (last resort)
								const html = document.documentElement.innerHTML;
								const latRegex = /"latitude":\s*([0-9.-]+)/;
								const lngRegex = /"longitude":\s*([0-9.-]+)/;
								const latM = html.match(latRegex);
								const lngM = html.match(lngRegex);
								if (latM && lngM) {
									return {
										latitude: parseFloat(latM[1]),
										longitude: parseFloat(lngM[1]),
									};
								}

								return null;
							});

							if (detailCoords && detailCoords.latitude && detailCoords.longitude) {
								coords = detailCoords;
							}
						} catch (err) {
							// ignore detail page errors
						} finally {
							await detailPage.close();
						}

						// Save to database
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

	// Enqueue all listing pages one by one
	for (const propertyType of PROPERTY_TYPES) {
		console.log(
			`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages, 18 per page)`
		);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			// Construct full URL with page path based on propertyType.urlBase
			const url = `${propertyType.urlBase}page-${pg}/`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(`\n✅ Completed KFH - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

(async () => {
	try {
		await scrapeKFH();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
