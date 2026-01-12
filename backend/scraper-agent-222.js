// Pattinson scraper using Playwright with Crawlee and Camoufox
// Agent ID: 222
// Website: pattinson.co.uk
// Usage:
// node backend/scraper-agent-222.js

const { PlaywrightCrawler, log } = require("crawlee");
const { launchOptions } = require("camoufox-js");
const { firefox } = require("playwright");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 222;
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
		urlBase: "https://www.pattinson.co.uk/buy/property-search",
		totalPages: 101, // 2007 properties / 20 per page = 101 pages
		recordsPerPage: 20,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.pattinson.co.uk/rent/property-search",
		totalPages: 1, // TEST: 1 page only
		recordsPerPage: 20,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapePattinson() {
	console.log(`\n🚀 Starting Pattinson scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 5,
		requestHandlerTimeoutSecs: 600,

		launchContext: {
			launcher: firefox,
			launchOptions: await launchOptions({
				headless: true,
			}),
		},

		browserPoolOptions: {
			// Disable the default fingerprint spoofing to avoid conflicts with Camoufox
			useFingerprints: false,
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Random delay to avoid rate limiting (3-8 seconds)
			const delay = Math.floor(Math.random() * 5000) + 3000;
			await page.waitForTimeout(delay);

			await page.waitForSelector("a.row.m-0.bg-white", { timeout: 30000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			// Extract properties from the DOM
			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll("a.row.m-0.bg-white"));
					return cards
						.map((card) => {
							// Check for Let/Let Agreed badge
							const badge = card.querySelector(".badge");
							if (badge && badge.textContent.includes("Let")) {
								return null; // Skip Let/Let Agreed properties
							}

							const href = card.getAttribute("href");
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.pattinson.co.uk" + href
								: null;

							// Extract price and title from the card content
							const priceEl = card.querySelector("dt.display-5.text-primary");
							const price = priceEl ? priceEl.textContent.trim() : "";

							const titleEl = card.querySelector(".text-primary-dark.fw-medium");
							const title = titleEl ? titleEl.textContent.trim() : "";

							// Extract bedrooms from first bed icon
							const bedEl = card.querySelector(".tabler-icon-bed + .fs-14");
							const bedrooms = bedEl ? bedEl.textContent.trim() : null;

							return { link, title, price, bedrooms };
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
						// Ensure absolute URL
						if (!property.link) return;

						// Random delay between detail page visits (1-3 seconds)
						await new Promise((resolve) => setTimeout(resolve, Math.random() * 2000 + 1000));

						let coords = { latitude: null, longitude: null };

						// Visit detail page to extract coordinates from GeoCoordinates JSON
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 40000,
							});
							await detailPage.waitForTimeout(1500);

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
													return { lat: parseFloat(geo.latitude), lng: parseFloat(geo.longitude) };
												}
											}
										}
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

						try {
							// Format price: extract only digits
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

				// Random delay between batches (2-5 seconds)
				await new Promise((resolve) => setTimeout(resolve, Math.random() * 3000 + 2000));
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
			// Construct page URL with pagination - page 1 has no param, rest use ?p=N
			let url;
			if (propertyType.isRental) {
				url = pg === 1 ? propertyType.urlBase : `${propertyType.urlBase}?p=${pg}`;
			} else {
				// Sales URL already has query params, so use & for additional params
				url = pg === 1 ? propertyType.urlBase : `${propertyType.urlBase}&p=${pg}`;
			}
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Pattinson - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapePattinson();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
