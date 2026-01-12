// Simon Blyth scraper using Playwright with Crawlee
// Agent ID: 246
// Usage:
// node backend/scraper-agent-246.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 246;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(num, isRental) {
	if (!num || isNaN(num)) return isRental ? "£0 pcm" : "£0";
	return "£" + Number(num).toLocaleString("en-GB") + (isRental ? " pcm" : "");
}

// Configuration for Simon Blyth
const PROPERTY_TYPES = [
	{
		// Sales
		urlBase: "https://www.simonblyth.co.uk/properties/",
		// We'll enqueue pages 1 to 22 based on current search results for radius=1.
		totalPages: 22,
		isRental: false,
		label: "SALES",
	},
];

async function scrapeSimonBlyth() {
	console.log(`\n🚀 Starting Simon Blyth scraper (Agent ${AGENT_ID})...\n`);

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
				.waitForSelector(".property", { timeout: 30000 })
				.catch(() => console.log(`⚠️ No properties found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll(".property"));
					const results = [];

					for (const el of items) {
						// Status check: Exclude SOLD STC, Under Offer, Let Agreed
						const statusEl =
							el.querySelector(".status .darker-grey p") || el.querySelector(".status");
						const statusText = statusEl ? statusEl.innerText : "";

						if (statusText.match(/Sold STC|Under Offer|Let Agreed/i)) {
							continue;
						}

						const linkEl =
							el.querySelector("a.button.on-white[href]") ||
							el.querySelector("a[href*='/property/']");
						if (!linkEl) continue;

						let href = linkEl.getAttribute("href");
						const link = href.startsWith("http")
							? href
							: new URL(href, window.location.origin).href;

						const title = el.querySelector("h3.property_title")?.textContent?.trim() || "Property";

						results.push({ link, title });
					}
					return results;
				} catch (e) {
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

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 45000,
							});

							const detailData = await detailPage.evaluate(async () => {
								try {
									const data = {
										price: null,
										bedrooms: null,
										address: null,
										lat: null,
										lng: null,
									};

									// 1. Address from h1 inside #single-property
									const h1 =
										document.querySelector("#single-property h1") || document.querySelector("h1");
									if (h1) data.address = h1.textContent.trim();

									// 2. Price
									const priceEl =
										document.querySelector("#single-property .price") ||
										document.querySelector(".price");
									if (priceEl) data.price = priceEl.textContent.trim();

									// 3. Bedrooms from specs string
									const specs =
										document.querySelector("#single-property")?.innerText ||
										document.body.innerText;
									const bedMatch = specs.match(/Bedrooms:\s*(\d+)/i);
									if (bedMatch) {
										data.bedrooms = bedMatch[1];
									} else {
										const altBedMatch = specs.match(/(\d+)\s*Bedroom/i);
										if (altBedMatch) data.bedrooms = altBedMatch[1];
									}

									// 4. Coordinates from hidden div attributes
									const latEl = document.getElementById("lat");
									const lngEl = document.getElementById("lng");

									if (latEl && lngEl) {
										const latVal = latEl.getAttribute("value");
										const lngVal = lngEl.getAttribute("value");

										if (latVal && lngVal) {
											data.lat = parseFloat(latVal);
											data.lng = parseFloat(lngVal);
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
			// URL Structure: https://www.simonblyth.co.uk/properties/?radius=1&pagen=2&sort=-created_at&type=buy
			const url = `${propertyType.urlBase}?radius=1&pagen=${pg}&sort=-created_at&type=buy`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Simon Blyth - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeSimonBlyth();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
