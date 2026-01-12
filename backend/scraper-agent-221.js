// Andrew Craig scraper using Playwright with Crawlee
// Agent ID: 221
// Website: andrewcraig.co.uk
// Usage:
// node backend/scraper-agent-221.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 221;
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
	// 	urlBase: "https://andrewcraig.co.uk/property-for-sale/property/any-bed/all-location",
	// 	totalPages: 12, // 282 properties / 24 per page = 12 pages
	// 	recordsPerPage: 24,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlBase: "https://andrewcraig.co.uk/property-to-rent/property/any-bed/all-location",
		totalPages: 2, // 34 properties / 24 per page = 2 pages
		recordsPerPage: 24,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeAndrewCraig() {
	console.log(`\n🚀 Starting Andrew Craig scraper (Agent ${AGENT_ID})...\n`);

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
			await page.waitForSelector(".card[data-id]", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			// Extract properties from the DOM
			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll(".card[data-id]"));
					return cards
						.map((card) => {
							const linkEl = card.querySelector("a.card-image-container");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://andrewcraig.co.uk" + href
								: null;
							const titleEl = card.querySelector(".card-content > a");
							const title = titleEl ? titleEl.textContent.trim() : "";
							const priceEl = card.querySelector("span.price-value");
							const price = priceEl ? priceEl.textContent.trim() : "";

							// Extract bedrooms from .card-content__detail__left (first number after bed icon)
							const detailLeft = card.querySelector(".card-content__detail__left");
							let bedrooms = null;
							if (detailLeft) {
								const numbers = Array.from(detailLeft.querySelectorAll(".number"));
								if (numbers.length >= 1) {
									bedrooms = numbers[0].textContent.trim();
								}
							}

							return { link, title, price, bedrooms };
						})
						.filter((p) => p.link);
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

						// Visit detail page to extract coordinates from script
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							const detailCoords = await detailPage.evaluate(() => {
								try {
									// Extract lat/lng from script content
									const scripts = Array.from(document.querySelectorAll("script"));
									for (const script of scripts) {
										const content = script.textContent;
										if (content.includes("lat:") && content.includes("lng:")) {
											const latMatch = content.match(/lat:\s*([0-9.-]+)/);
											const lngMatch = content.match(/lng:\s*([0-9.-]+)/);
											if (latMatch && lngMatch) {
												return { lat: parseFloat(latMatch[1]), lng: parseFloat(lngMatch[1]) };
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
			// Construct page URL with exclude=1 and page parameter
			const url =
				pg === 1
					? `${propertyType.urlBase}?exclude=1`
					: `${propertyType.urlBase}?exclude=1&page=${pg}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Andrew Craig - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeAndrewCraig();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
