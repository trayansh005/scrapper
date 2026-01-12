// Romans scraper using Playwright with Crawlee
// Agent ID: 16
//
// Usage:
// node backend/scraper-agent-16.js

const { PlaywrightCrawler, log } = require("crawlee");
const { firefox } = require("playwright");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 16;
let totalScraped = 0;
let totalSaved = 0;

// Extract coordinates from script tag
function extractCoordinatesFromHTML(html) {
	// Look for latitude and longitude in script tags
	// Pattern from Romans: "latitude":51.3339,"longitude":-0.781668

	// Try multiple patterns to catch the coordinates
	let latMatch = html.match(/"latitude"\s*:\s*([0-9.-]+)/);
	let lngMatch = html.match(/"longitude"\s*:\s*([0-9.-]+)/);

	// Try without quotes around the key
	if (!latMatch) {
		latMatch = html.match(/latitude\s*:\s*([0-9.-]+)/);
	}
	if (!lngMatch) {
		lngMatch = html.match(/longitude\s*:\s*([0-9.-]+)/);
	}

	console.log(
		`   📍 Final results - Lat:`,
		latMatch ? latMatch[1] : "null",
		"Lng:",
		lngMatch ? lngMatch[1] : "null"
	);

	if (latMatch && lngMatch) {
		console.log(`   🎯 Extracted coords: ${latMatch[1]}, ${lngMatch[1]}`);
		return {
			latitude: parseFloat(latMatch[1]),
			longitude: parseFloat(lngMatch[1]),
		};
	}

	console.log(`   ❌ No coordinates extracted`);
	return { latitude: null, longitude: null };
}

async function scrapeRomans() {
	console.log(`\n🚀 Starting Romans scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3, // Process 3 pages in parallel for faster scraping
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: firefox,
			launchOptions: {
				headless: true,
			},
		},

		async requestHandler({ page, request, enqueueLinks }) {
			const { isDetailPage, propertyData, category, pageNum } = request.userData;

			if (isDetailPage) {
				// Processing detail page to get coordinates
				try {
					// Wait for scripts to load
					await page.waitForTimeout(1000);

					// Try to extract coordinates directly from page JavaScript
					const coords = await page.evaluate(() => {
						// Look for coordinates in window object or script content
						const scripts = document.querySelectorAll("script");
						let foundScripts = 0;
						let scriptsWithLatitude = 0;

						for (const script of scripts) {
							foundScripts++;
							const content = script.textContent || script.innerHTML;
							if (content.includes("latitude")) {
								scriptsWithLatitude++;
								// Try different patterns - escaped quotes
								let latMatch = content.match(/\\"latitude\\":\s*([0-9.-]+)/);
								let lngMatch = content.match(/\\"longitude\\":\s*([0-9.-]+)/);

								// Try without escaped quotes
								if (!latMatch) {
									latMatch = content.match(/"latitude":\s*([0-9.-]+)/);
								}
								if (!lngMatch) {
									lngMatch = content.match(/"longitude":\s*([0-9.-]+)/);
								}

								if (latMatch && lngMatch) {
									return {
										latitude: parseFloat(latMatch[1]),
										longitude: parseFloat(lngMatch[1]),
										debug: `Found in script ${scriptsWithLatitude}/${foundScripts}`,
									};
								}
							}
						}
						return {
							latitude: null,
							longitude: null,
							debug: `Checked ${foundScripts} scripts, ${scriptsWithLatitude} had 'latitude'`,
						};
					});

					console.log(`   🔍 Debug:`, coords.debug);

					const is_rent = category === "to-rent";

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
				const typeLabel = category === "to-rent" ? "RENT" : "for-sale";
				console.log(`📋 Page ${pageNum} - ${typeLabel.toUpperCase()} - ${request.url}`);

				// Wait for properties to load
				await page.waitForTimeout(1500);
				await page.waitForSelector(".property-card-wrapper", { timeout: 30000 }).catch(() => {
					console.log(`⚠️ No properties found`);
				});

				// Extract all properties from the page
				const properties = await page.$$eval(".property-card-wrapper", (cards) => {
					const results = [];

					const formatPrice = (raw) => {
						if (!raw) return null;
						const digits = raw.replace(/[^0-9]/g, "");
						if (!digits) return null;
						return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
					};

					cards.forEach((card) => {
						try {
							// Get the link from the first <a> tag
							const linkEl = card.querySelector('a[href*="/properties"]');
							const link = linkEl ? linkEl.getAttribute("href") : null;

							// Get the title from h2
							const titleEl = card.querySelector(".property-title h2");
							const title = titleEl ? titleEl.textContent.trim() : null;

							// Get the price from h3.property-price and sanitize
							const priceEl = card.querySelector(".property-price");
							const priceText = priceEl ? priceEl.textContent.trim() : "";
							const price = formatPrice(priceText);

							// Get bedrooms from the icon-bed list item
							const bedEl = card.querySelector(".icon-bed");
							let bedrooms = null;
							if (bedEl && bedEl.parentElement) {
								const bedText = bedEl.parentElement.textContent.trim();
								const bedMatch = bedText.match(/(\d+)/);
								bedrooms = bedMatch ? bedMatch[1] : null;
							}

							// Check status - exclude "Let Agreed"
							const statusEl = card.querySelector(".property-status");
							const status = statusEl ? statusEl.textContent.trim() : "";

							// Skip if status is "Let Agreed"
							if (status === "Let Agreed") {
								return;
							}

							if (link && price && title) {
								results.push({
									link: link.startsWith("http") ? link : "https://www.romans.co.uk" + link,
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

				console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

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

	// Add initial listing page URLs - Sales first, then rent
	const requests = [];

	// Add all sales pages (876 properties / 8 per page = 110 pages)
	for (let page = 1; page <= 110; page++) {
		requests.push({
			url: `https://www.romans.co.uk/properties/for-sale/page-${page}/`,
			userData: { category: "for-sale", isDetailPage: false, pageNum: page },
		});
	}

	// Add all rental pages (537 properties / 8 per page = 68 pages)
	for (let page = 1; page <= 68; page++) {
		requests.push({
			url: `https://www.romans.co.uk/properties/to-rent/page-${page}/`,
			userData: { category: "to-rent", isDetailPage: false, pageNum: page },
		});
	}

	await crawler.addRequests(requests);
	await crawler.run();

	console.log(`\n✅ Completed Romans - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

// Main execution
(async () => {
	try {
		await scrapeRomans();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
