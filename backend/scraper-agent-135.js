// Taylors scraper using Playwright with Crawlee
// Agent ID: 135
//
// Usage:
// node backend/scraper-agent-135.js

const { PlaywrightCrawler, log } = require("crawlee");
const { chromium } = require("playwright");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 135;
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
		urlPath: "properties/sales/status-available/most-recent-first",
		totalRecords: 1075,
		recordsPerPage: 10,
		isRental: false,
		label: "SALES",
	},
	{
		urlPath: "properties/lettings/status-available/most-recent-first",
		totalRecords: 208,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

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
			const { isDetailPage, propertyData, isRental } = request.userData;

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
					console.log(
						`✅ ${propertyData.title} - ${formatPrice(propertyData.price)} - ${coordsStr}`
					);
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

								// Get the price from .card__heading and sanitize
								let price = null;
								const priceEl = card.querySelector(".card__heading");
								if (priceEl) {
									const priceText = priceEl.textContent.trim();
									price = priceText.replace(/[^0-9]/g, "");
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
						isRental,
					},
				}));

				await crawler.addRequests(detailRequests);
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Add listing page URLs - process both sales and lettings sequentially
	for (const propertyType of PROPERTY_TYPES) {
		const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
		console.log(
			`🏠 Processing ${propertyType.label} properties (${propertyType.totalRecords} total, ${totalPages} pages)\n`
		);

		// Add pages for this property type
		const listingRequests = [];
		for (let pageNum = 1; pageNum <= totalPages; pageNum++) {
			listingRequests.push({
				url: `https://www.taylorsestateagents.co.uk/${propertyType.urlPath}/page-${pageNum}#/`,
				userData: {
					isDetailPage: false,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}

		// Process this property type before moving to next
		await crawler.addRequests(listingRequests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Taylors - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
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
