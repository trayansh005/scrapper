// Taylors scraper using Playwright with Crawlee
// Agent ID: 135
//
// Usage:
// node backend/scraper-agent-135.js

const { PlaywrightCrawler, log } = require("crawlee");
const { chromium } = require("playwright");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 135;
let totalScraped = 0;
let totalSaved = 0;

// Extract coordinates from script tags
function extractCoordinatesFromHTML(html) {
	// Look for: ga4_property_latitude: 51.5728027, ga4_property_longitude: -0.1638948
	const latMatch = html.match(/ga4_property_latitude:\s*([0-9.-]+)/);
	const lngMatch = html.match(/ga4_property_longitude:\s*([0-9.-]+)/);
	if (latMatch && lngMatch) {
		return {
			latitude: parseFloat(latMatch[1]),
			longitude: parseFloat(lngMatch[1]),
		};
	}
	return { latitude: null, longitude: null };
}

async function scrapeTaylors() {
	console.log(`\n🚀 Starting Taylors scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1, // Process sequentially
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: chromium, // Use Chromium
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { isDetailPage, propertyData } = request.userData;

			if (isDetailPage) {
				// Processing detail page to get coordinates
				try {
					// Wait for the map script to load
					// Wait for the page to load
					await page.waitForTimeout(1000);

					// Extract coordinates from the page HTML
					const htmlContent = await page.content();
					const coords = extractCoordinatesFromHTML(htmlContent);

					await updatePriceByPropertyURL(
						propertyData.link,
						propertyData.price,
						propertyData.title,
						propertyData.bedrooms,
						AGENT_ID,
						false, // Taylors is sales only
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
				console.log(`📋 Scraping - ${request.url}`);

				// Wait for the properties container to appear
				await page
					.waitForSelector(".card--list .card, .hf-property-results .card", { timeout: 30000 })
					.catch(() => {
						console.log(`⚠️ No property cards found`);
					});

				// Wait longer for dynamic content to load
				console.log(`   Waiting for properties to load...`);
				await page.waitForTimeout(2000); // Wait 2 seconds for content to render

				// Extract all properties from the page
				const properties = await page
					.$$eval(".card--list .card, .hf-property-results .card", (cards) => {
						const results = [];

						cards.forEach((card) => {
							try {
								// Get the link - try card__link first, then fallback to any 'a' tag
								let linkEl = card.querySelector("a.card__link");
								if (!linkEl) {
									linkEl = card.querySelector("a");
								}
								const link = linkEl ? linkEl.getAttribute("href") : null;
								if (!link) return;

								// Convert relative URLs to absolute
								const fullLink = link.startsWith("http")
									? link
									: "https://www.taylorsestateagents.co.uk" + link;

								// Get the title - try .card__text-title first, then fallback
								let title = null;
								let titleEl = card.querySelector(".card__text-title");
								if (!titleEl) {
									titleEl = card.querySelector(".card__text-content");
								}
								if (titleEl) {
									title = titleEl.textContent.trim();
								}

								// Get the price from .card__heading
								let price = null;
								const priceEl = card.querySelector(".card__heading");
								if (priceEl) {
									const priceText = priceEl.textContent.trim();
									const priceMatch = priceText.match(/£([\d,]+)/);
									if (priceMatch) {
										price = priceMatch[1].replace(/,/g, "");
									}
								}

								// Get bedrooms - first .card-content__spec-list-number
								let bedrooms = null;
								const bedroomEls = card.querySelectorAll(".card-content__spec-list-number");
								if (bedroomEls && bedroomEls.length > 0) {
									const bedroomText = bedroomEls[0].textContent.trim();
									const bedroomMatch = bedroomText.match(/(\d+)/);
									if (bedroomMatch) {
										bedrooms = bedroomMatch[1];
									}
								}

								// Only add if we have a link, price, and title
								if (fullLink && price && title) {
									results.push({
										link: fullLink,
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
					})
					.catch(() => []);

				console.log(`� Found ${properties.length} properties`);

				// Debug: Show first property if available
				if (properties.length > 0) {
					console.log(`   Sample: ${properties[0].title} - £${properties[0].price}`);
				}

				// Add detail page requests to the queue
				const detailRequests = properties.map((property) => ({
					url: property.link,
					userData: {
						isDetailPage: true,
						propertyData: property,
					},
				}));

				await crawler.addRequests(detailRequests);
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Add listing page URLs - Taylors shows 128 pages of properties
	const requests = [];
	for (let pageNum = 1; pageNum <= 128; pageNum++) {
		requests.push({
			url: `https://www.taylorsestateagents.co.uk/properties/sales/status-available/most-recent-first/page-${pageNum}#/`,
			userData: { isDetailPage: false },
		});
	}

	await crawler.addRequests(requests);
	await crawler.run();

	console.log(
		`\n✅ Completed Taylors - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

// Local implementation of updateRemoveStatus
async function updateRemoveStatus(agent_id) {
	try {
		const remove_status = 1;
		await promisePool.query(
			`UPDATE property_for_sale SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
			[remove_status, agent_id]
		);
		console.log(`🧹 Removed old properties for agent ${agent_id}`);
	} catch (error) {
		console.error("Error updating remove status:", error.message);
	}
}

// Main execution
(async () => {
	try {
		await scrapeTaylors();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
