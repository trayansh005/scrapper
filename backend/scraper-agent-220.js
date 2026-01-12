// Rook Matthews Sayer scraper using Playwright with Crawlee
// Agent ID: 220
// Website: rookmatthewssayer.co.uk
// Usage:
// node backend/scraper-agent-220.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 220;
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
	// {
	// 	urlBase: "https://www.rookmatthewssayer.co.uk/for-sale",
	// 	totalPages: 123, // 1098 properties / 9 per page = 122 pages
	// 	recordsPerPage: 9,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlBase: "https://www.rookmatthewssayer.co.uk/for-rent",
		totalPages: 17, // 151 properties / 9 per page = 17 pages
		recordsPerPage: 9,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeRookMatthewsSayer() {
	console.log(`\n🚀 Starting Rook Matthews Sayer scraper (Agent ${AGENT_ID})...\n`);

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
			await page.waitForSelector(".properties-grid-col", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			// Extract properties from the DOM
			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(
						document.querySelectorAll(".col-lg-4.col-md-12.col-sm-12.properties-grid-col")
					);
					return cards
						.map((card) => {
							// Check for status labels (Sold, Sold STC, Let, Let STC)
							const statusLabel = card.querySelector(
								".listing-custom-label-sold, .listing-custom-label-soldstc, .listing-custom-label-let, .listing-custom-label-letstc"
							);
							if (statusLabel) {
								return null; // Skip sold/let properties
							}

							const linkEl = card.querySelector("a.rwsp-grid-link");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.rookmatthewssayer.co.uk" + href
								: null;
							const titleEl = card.querySelector("h2.property-title");
							const title = titleEl ? titleEl.textContent.trim() : "";
							const priceEl = card.querySelector("span.item-price");
							const price = priceEl ? priceEl.textContent.trim() : "";

							// Extract bedrooms, living rooms, and bathrooms from detail-icons
							const detailIcons = Array.from(card.querySelectorAll(".detail-icons ul li"));
							let bedrooms = null;
							let reception = null;
							let bathrooms = null;

							if (detailIcons.length >= 1) {
								bedrooms = detailIcons[0].textContent.trim().split(/\s+/).pop();
							}
							if (detailIcons.length >= 2) {
								reception = detailIcons[1].textContent.trim().split(/\s+/).pop();
							}
							if (detailIcons.length >= 3) {
								bathrooms = detailIcons[2].textContent.trim().split(/\s+/).pop();
							}

							return { link, title, price, bedrooms, reception, bathrooms };
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

						let coords = { latitude: null, longitude: null };

						// Visit detail page to extract coordinates from comments
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							const detailCoords = await detailPage.evaluate(() => {
								try {
									const html = document.documentElement.outerHTML;
									const latMatch = html.match(/<!--property-latitude:"([^"]+)"-->/);
									const lngMatch = html.match(/<!--property-longitude:"([^"]+)"-->/);
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

				// Small delay between batches
				await new Promise((resolve) => setTimeout(resolve, 200));
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
			// Construct page URL
			const url =
				pg === 1
					? `${propertyType.urlBase}/?sortby=d_date`
					: `${propertyType.urlBase}/page/${pg}/?sortby=d_date`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Rook Matthews Sayer - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeRookMatthewsSayer();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
