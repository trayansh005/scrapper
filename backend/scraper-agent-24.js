// Haboodle scraper using Playwright with Crawlee
// Agent ID: 24
//
// Usage:
// node backend/scraper-agent-24.js

const { PlaywrightCrawler, log } = require("crawlee");
const { firefox } = require("playwright");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 24;
let totalScraped = 0;
let totalSaved = 0;

async function scrapeHaboodle() {
	console.log(`\n🚀 Starting Haboodle scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: firefox,
			launchOptions: {
				headless: true,
			},
		},

		async requestHandler({ page, request }) {
			const { isDetailPage, propertyData, pageNum } = request.userData;

			if (isDetailPage) {
				// Processing detail page to get coordinates
				try {
					await page.waitForTimeout(2000);

					let coords = { latitude: null, longitude: null };

					// Extract coordinates from the page script
					try {
						const htmlContent = await page.content();
						// Look for: var myLatlng = new google.maps.LatLng(51.4191054, -0.1656255);
						const coordMatch = htmlContent.match(
							/google\.maps\.LatLng\s*\(\s*(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\s*\)/
						);
						if (coordMatch) {
							coords.latitude = parseFloat(coordMatch[1]);
							coords.longitude = parseFloat(coordMatch[2]);
						}
					} catch (err) {
						// Coordinates not found
					}

					await updatePriceByPropertyURL(
						propertyData.link,
						propertyData.price,
						propertyData.title,
						propertyData.bedrooms,
						AGENT_ID,
						false,
						coords.latitude,
						coords.longitude
					);

					totalSaved++;
					totalScraped++;

					if (coords.latitude && coords.longitude) {
						console.log(
							`✅ ${propertyData.title} - £${propertyData.price} - ${coords.latitude}, ${coords.longitude}`
						);
					} else {
						console.log(`✅ ${propertyData.title} - £${propertyData.price} - No coords`);
					}
				} catch (error) {
					console.error(`❌ Error saving property: ${error.message}`);
				}
			} else {
				// Processing listing page
				console.log(`📋 Page ${pageNum} - ${request.url}`);

				// Wait for properties to load
				await page.waitForTimeout(3000);
				await page.waitForSelector("ul.properties", { timeout: 30000 }).catch(() => {
					console.log(`⚠️ No properties found on page ${pageNum}`);
				});

				// Extract all properties from the page
				const { properties, debug } = await page.$$eval("li.type-property", (listings) => {
					const results = [];
					const debugData = { total: listings.length, processed: 0 };

					const formatPrice = (raw) => {
						if (!raw) return null;
						const digits = raw.replace(/[^0-9]/g, "");
						if (!digits) return null;
						return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
					};

					listings.forEach((listing) => {
						try {
							debugData.processed++;

							// Get the link from h3 a
							const linkEl = listing.querySelector("h3 a");
							const link = linkEl ? linkEl.getAttribute("href") : null;
							const title = linkEl ? linkEl.textContent.trim() : null;

							// Get the price and sanitize
							const priceEl = listing.querySelector(".price");
							const priceText = priceEl ? priceEl.textContent.trim() : "";

							// Skip if marked as "SOLD STC"
							if (priceText.includes("SOLD STC")) {
								return;
							}

							// Format price - keep only digits and commas
							const price = formatPrice(priceText);

							// Get bedrooms
							const bedroomsEl = listing.querySelector(".room-bedrooms .room-count");
							const bedrooms = bedroomsEl ? bedroomsEl.textContent.trim() : null;

							// Store debug info for first property
							if (results.length === 0) {
								debugData.firstProperty = {
									hasLink: !!link,
									hasTitle: !!title,
									hasPrice: !!price,
									priceText,
									title: title ? title.substring(0, 50) : null,
								};
							}

							if (link && price && title) {
								results.push({
									link: link,
									title: title,
									price,
									bedrooms,
								});
							}
						} catch (err) {
							debugData.errors = (debugData.errors || 0) + 1;
						}
					});

					return { properties: results, debug: debugData };
				});

				console.log(`🔍 Extraction debug:`, debug);
				console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

				// Add detail page requests to the queue
				const detailRequests = properties.map((property) => ({
					url: property.link,
					userData: {
						isDetailPage: true,
						propertyData: property,
					},
				}));

				await crawler.addRequests(detailRequests);

				// Check for next page
				const nextPageExists = await page.$("a.next.page-numbers");
				if (nextPageExists) {
					const nextUrl = await nextPageExists.getAttribute("href");
					if (nextUrl) {
						await crawler.addRequests([
							{
								url: nextUrl,
								userData: {
									isDetailPage: false,
									pageNum: pageNum + 1,
								},
							},
						]);
						console.log(`   ➡️  Queued page ${pageNum + 1}`);
					}
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Add initial listing page URL
	const requests = [
		{
			url: "https://www.haboodle.co.uk/find-a-property/?department=residential-sales&address_keyword=&radius=&minimum_bedrooms=&maximum_rent=&maximum_price=",
			userData: { isDetailPage: false, pageNum: 1 },
		},
	];

	await crawler.addRequests(requests);
	await crawler.run();

	console.log(
		`\n✅ Completed Haboodle - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

// Main execution
(async () => {
	try {
		await scrapeHaboodle();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
