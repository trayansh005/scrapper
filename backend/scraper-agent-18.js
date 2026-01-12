// Moveli scraper using Playwright with Crawlee
// Agent ID: 18
//
// Usage:
// node backend/scraper-agent-18.js

const { PlaywrightCrawler, log } = require("crawlee");
const { firefox } = require("playwright");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 18;
let totalScraped = 0;
let totalSaved = 0;

// Extract coordinates from Google Maps initialization script
function extractCoordinatesFromHTML(html) {
	// Look for: const location = { lat: 51.5728027, lng: -0.1638948}
	const locationMatch = html.match(
		/const location = \{\s*lat:\s*([0-9.-]+),\s*lng:\s*([0-9.-]+)\s*\}/
	);
	if (locationMatch) {
		return {
			latitude: parseFloat(locationMatch[1]),
			longitude: parseFloat(locationMatch[2]),
		};
	}
	return { latitude: null, longitude: null };
}

async function scrapeMoveli() {
	console.log(`\n🚀 Starting Moveli scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1, // Process sequentially
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: firefox, // Use Firefox
			launchOptions: {
				headless: true,
			},
		},

		async requestHandler({ page, request }) {
			const { category, isDetailPage, propertyData } = request.userData;

			if (isDetailPage) {
				// Processing detail page to get coordinates
				try {
					// Wait for the map script to load
					await page.waitForTimeout(1000);

					// Extract coordinates from the page HTML
					const htmlContent = await page.content();
					const coords = extractCoordinatesFromHTML(htmlContent);

					const is_rent = category === "for-rent";

					await updatePriceByPropertyURL(
						propertyData.link,
						propertyData.price,
						propertyData.title,
						propertyData.bedrooms,
						AGENT_ID,
						is_rent,
						coords.latitude,
						coords.longitude
					);

					totalSaved++;
					totalScraped++;

					const typeLabel = is_rent ? "RENT" : "SALE";
					if (coords.latitude && coords.longitude) {
						console.log(
							`✅ [${typeLabel}] ${propertyData.title} - £${propertyData.price} - ${coords.latitude}, ${coords.longitude}`
						);
					} else {
						console.log(
							`✅ [${typeLabel}] ${propertyData.title} - £${propertyData.price} - No coords`
						);
					}
				} catch (error) {
					console.error(`❌ Error saving property: ${error.message}`);
				}
			} else {
				// Processing listing page
				const typeLabel = category === "for-rent" ? "RENT" : "SALE";
				console.log(`📋 Scraping ${typeLabel} - ${request.url}`);

				// Wait for the properties container to appear
				await page.waitForSelector("#properties-container", { timeout: 30000 }).catch(() => {
					console.log(`⚠️ No #properties-container found`);
				});

				// Wait longer for dynamic content to load
				console.log(`   Waiting for properties to load...`);
				await page.waitForTimeout(5000); // Wait 5 seconds for React/Vue to render

				// Try to wait for property cards
				const cardsLoaded = await page
					.waitForSelector(".property_card", { timeout: 10000 })
					.catch(() => {
						console.log(`⚠️ No .property_card elements loaded`);
						return null;
					});

				if (!cardsLoaded) {
					console.log(`   Trying alternative selector .property-item...`);
					await page.waitForSelector(".property-item", { timeout: 10000 }).catch(() => {
						console.log(`⚠️ No .property-item found either`);
					});
				}

				// Debug: Check what we have
				const itemCount = await page
					.$$eval(".property-item", (items) => items.length)
					.catch(() => 0);
				console.log(`   Found ${itemCount} .property-item elements`);

				const cardCount = await page
					.$$eval(".property_card", (cards) => cards.length)
					.catch(() => 0);
				console.log(`   Found ${cardCount} .property_card elements`);

				if (cardCount === 0 && itemCount === 0) {
					console.log(`   No properties found, skipping...`);
					return;
				}

				// Extract all properties from the page
				const properties = await page.$$eval(".property_card", (cards) => {
					const results = [];

					const formatPrice = (raw) => {
						if (!raw) return null;
						const digits = raw.replace(/[^0-9]/g, "");
						if (!digits) return null;
						return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
					};

					cards.forEach((card) => {
						try {
							// Get the link
							const link = card.getAttribute("href");
							if (!link) return;

							// Get the title from h4
							const titleEl = card.querySelector(".property_label h4");
							const title = titleEl ? titleEl.textContent.trim() : null;

							// Get the price from .format_price and sanitize
							const priceEl = card.querySelector(".format_price");
							const priceText = priceEl ? priceEl.textContent.trim() : "";
							const price = formatPrice(priceText);

							// Get bedrooms - look for number before "beds"
							const allText = card.textContent || "";
							const bedroomsMatch = allText.match(/(\d+)\s*beds?/i);
							const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

							// Check status - only include AVAILABLE properties
							const statusEl = card.querySelector(".status_label");
							const status = statusEl ? statusEl.textContent.trim().toUpperCase() : "";

							// Only add if we have a link, price, title, and status is AVAILABLE
							if (link && price && title && status === "AVAILABLE") {
								results.push({
									link: link.startsWith("http") ? link : "https://www.moveli.co.uk" + link,
									title,
									price,
									bedrooms,
								});
							}
						} catch (err) {
							// Silent error
						}
					});

					return results;
				});

				console.log(`🔗 Found ${properties.length} ${typeLabel} properties`);

				// Debug: Show first property if available
				if (properties.length > 0) {
					console.log(`   Sample: ${properties[0].title} - £${properties[0].price}`);
				} else if (cardCount > 0) {
					// Debug: Check what's in the first card
					const sampleData = await page.evaluate(() => {
						const firstCard = document.querySelector(".property_card");
						if (firstCard) {
							return {
								href: firstCard.getAttribute("href"),
								text: firstCard.textContent.substring(0, 200),
								html: firstCard.innerHTML.substring(0, 300),
							};
						}
						return null;
					});
					console.log(`   Debug first card:`, sampleData);
				}

				// Add detail page requests to the queue
				const detailRequests = properties.map((property) => ({
					url: property.link,
					userData: {
						isDetailPage: true,
						propertyData: property,
						category,
					},
				}));

				await crawler.addRequests(detailRequests);
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Add listing page URLs for both sales and rentals
	const requests = [
		{
			url: "https://www.moveli.co.uk/test/properties?category=for-sale&searchKeywords=&status=all&maxPrice=any&minBeds=any&sortOrder=price-desc",
			userData: { category: "for-sale", isDetailPage: false },
		},
		{
			url: "https://www.moveli.co.uk/test/properties?category=for-rent&searchKeywords=&status=all&maxPrice=any&minBeds=any&sortOrder=price-desc",
			userData: { category: "for-rent", isDetailPage: false },
		},
	];

	await crawler.addRequests(requests);
	await crawler.run();

	console.log(`\n✅ Completed Moveli - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

// Main execution
(async () => {
	try {
		await scrapeMoveli();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
