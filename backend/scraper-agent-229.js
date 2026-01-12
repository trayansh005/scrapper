// Howards scraper using Playwright with Crawlee
// Agent ID: 229
// Usage:
// node backend/scraper-agent-229.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 229;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Two searches: sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://howards.co.uk/listings",
	// 	// 315 properties / 12 per page = 27 pages (rounded up)
	// 	totalRecords: 315,
	// 	recordsPerPage: 12,
	// 	totalPages: 27,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	params: {
	// 		viewType: "gallery",
	// 		sortby: "dateListed-desc",
	// 		saleOrRental: "Sale",
	// 		rental_period: "week",
	// 		status: "available",
	// 	},
	// },
	{
		urlBase: "https://howards.co.uk/listings",
		// 16 properties / 12 per page = 2 pages (rounded up)
		totalRecords: 16,
		recordsPerPage: 12,
		totalPages: 2,
		isRental: true,
		label: "TO LET",
		params: {
			viewType: "gallery",
			sortby: "dateListed-desc",
			saleOrRental: "Rental",
			rental_period: "month",
			status: "available",
		},
	},
];

async function scrapeHowards() {
	console.log(`\n🚀 Starting Howards scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: false,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(3000);

			// Wait for property cards to load - try multiple selectors
			const loaded = await Promise.race([
				page.waitForSelector("h4 a", { timeout: 20000 }).then(() => true),
				page.waitForSelector("a[href*='residential_']", { timeout: 20000 }).then(() => true),
			]).catch(() => false);

			if (!loaded) {
				console.log(`⚠️ No property cards found on page ${pageNum}`);
			}

			const properties = await page.evaluate(() => {
				try {
					// Find all links to property listings - try multiple patterns
					let titleLinks = Array.from(document.querySelectorAll("a[href*='residential_']"));

					// If no residential_ links found, try h4 a tags
					if (titleLinks.length === 0) {
						titleLinks = Array.from(document.querySelectorAll("h4 a"));
					}

					console.log(`Found ${titleLinks.length} title links`);

					return titleLinks
						.map((titleLink) => {
							try {
								const link = titleLink.href;

								// Skip if not a property listing
								if (!link.includes("/listings/")) return null;

								// Find the parent card container
								let cardEl = titleLink.closest("div[class*='v2-flex']");
								if (!cardEl) {
									cardEl = titleLink.parentElement?.parentElement?.parentElement;
								}
								if (!cardEl) return null;

								const title = titleLink.textContent?.trim() || "";

								// Extract price - look for strong or any element with price
								let rawPrice = cardEl.querySelector("strong")?.textContent?.trim() || "";
								if (!rawPrice) {
									// Try finding price with £ symbol
									const allText = cardEl.textContent || "";
									const priceMatch = allText.match(/£[\d,]+/);
									rawPrice = priceMatch ? priceMatch[0] : "";
								}

								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}

								// Extract bedrooms, bathrooms from the room divs
								const roomTexts = Array.from(cardEl.querySelectorAll("p"))
									.map((p) => p.textContent.trim())
									.filter((t) => t.includes("Bed") || t.includes("Bath"));
								const bedrooms = roomTexts.find((t) => t.includes("Bed")) || null;
								const bathrooms = roomTexts.find((t) => t.includes("Bath")) || null;

								if (!link || !title) return null;

								return { link, title, price, bedrooms, bathrooms };
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

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						let coords = { latitude: null, longitude: null };

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							// Extract latitude and longitude from script tag (JSON format with quotes around values)
							const scriptCoords = await detailPage.evaluate(() => {
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const script of scripts) {
									const text = script.textContent || "";
									// Look for "latitude":"52.46994340","longitude":"1.72935920"
									const latMatch = text.match(/"latitude"\s*:\s*"([\-0-9.]+)"/);
									const lngMatch = text.match(/"longitude"\s*:\s*"([\-0-9.]+)"/);
									if (latMatch && lngMatch) {
										return {
											latitude: parseFloat(latMatch[1]),
											longitude: parseFloat(lngMatch[1]),
										};
									}
								}
								return null;
							});

							if (scriptCoords && scriptCoords.latitude && scriptCoords.longitude) {
								coords.latitude = scriptCoords.latitude;
								coords.longitude = scriptCoords.longitude;
								console.log(`  📍 Found script coords: ${coords.latitude}, ${coords.longitude}`);
							}
						} catch (err) {
							// ignore
						} finally {
							await detailPage.close();
						}

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
							console.error(`❌ DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					})
				);

				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			// Build URL with query params
			const params = new URLSearchParams(propertyType.params);
			params.append("page", pg);
			const url = `${propertyType.urlBase}?${params.toString()}`;

			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Howards - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeHowards();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
