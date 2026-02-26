// Alan de Maid scraper using Playwright with Crawlee
// Agent ID: 103
//
// Usage:
// node backend/scraper-agent-103.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 103;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Configuration for sales and rentals
const PROPERTY_TYPES = [
	// {
	// 	urlPath: "properties/sales/status-available/most-recent-first",
	// 	totalRecords: 388,
	// 	recordsPerPage: 10,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlPath: "properties/lettings/status-available/most-recent-first",
		totalRecords: 7,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeAlanDeMaid() {
	console.log(`\n🚀 Starting Alan de Maid scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
			},
		},

		async requestHandler({ page, request }) {
			const { isDetailPage, propertyData, pageNum, isRental, label } = request.userData;

			if (isDetailPage) {
				// Processing detail page to get coordinates
				try {
					await page.waitForTimeout(1000);

					let coords = { latitude: null, longitude: null };

					// Extract coordinates from script tags containing propertyObject
					try {
						const htmlContent = await page.content();

						// Look for ga4_property_latitude and ga4_property_longitude in script tags
						const latMatch = htmlContent.match(/ga4_property_latitude:\s*([0-9.-]+)/);
						const lngMatch = htmlContent.match(/ga4_property_longitude:\s*([0-9.-]+)/);

						if (latMatch && lngMatch) {
							coords.latitude = parseFloat(latMatch[1]);
							coords.longitude = parseFloat(lngMatch[1]);
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
				console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

				// Wait for properties to load
				await page.waitForTimeout(3000);
				await page.waitForSelector(".card", { timeout: 30000 }).catch(() => {
					console.log(`⚠️ No properties found on page ${pageNum}`);
				});

				// Extract all properties from the page
				const properties = await page.$$eval(".card", (cards) => {
					const results = [];

					cards.forEach((card) => {
						try {
							// Extract link from anchor tag
							const linkEl = card.querySelector("a");
							let link = linkEl ? linkEl.getAttribute("href") : null;
							if (link && !link.startsWith("http")) {
								link = "https://www.alandemaid.co.uk" + link;
							}

							// Extract title from .card__text-content
							const titleEl = card.querySelector(".card__text-content");
							const title = titleEl ? titleEl.textContent.trim() : null;

							// Extract bedrooms from .card-content__spec-list-number (first occurrence)
							const bedroomsEl = card.querySelector(".card-content__spec-list-number");
							let bedrooms = null;
							if (bedroomsEl) {
								const bedroomsText = bedroomsEl.textContent.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								if (bedroomsMatch) {
									bedrooms = bedroomsMatch[0];
								}
							}

							// Extract price from .card__heading
							const priceEl = card.querySelector(".card__heading");
							let price = null;
							if (priceEl) {
								const priceText = priceEl.textContent.trim();
								price = priceText.replace(/[^0-9]/g, "");
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
							// Skip this card if error
						}
					});

					return results;
				});

				console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

				// Add detail page requests to the queue with delay
				for (let i = 0; i < properties.length; i++) {
					const property = properties[i];
					await crawler.addRequests([
						{
							url: property.link,
							userData: {
								isDetailPage: true,
								propertyData: property,
								isRental,
							},
						},
					]);

					// Add delay between detail page requests to avoid rate limiting
					if (i < properties.length - 1) {
						await new Promise((resolve) => setTimeout(resolve, 500));
					}
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Add initial listing page URLs for both sales and lettings
	const requests = [];

	for (const propertyType of PROPERTY_TYPES) {
		const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
		console.log(
			`🏠 Queueing ${propertyType.label} properties (${propertyType.totalRecords} total, ${totalPages} pages)`
		);

		for (let page = 1; page <= totalPages; page++) {
			const url = propertyType.isRental
				? `https://www.alandemaid.co.uk/${propertyType.urlPath}#/`
				: `https://www.alandemaid.co.uk/${propertyType.urlPath}/page-${page}#/`;

			requests.push({
				url: url,
				userData: {
					isDetailPage: false,
					pageNum: page,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});

			// For lettings, only scrape first page
			if (propertyType.isRental) {
				break;
			}
		}
	}

	await crawler.addRequests(requests);
	await crawler.run();

	console.log(
		`\n✅ Completed Alan de Maid - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

// Main execution
(async () => {
	try {
		await scrapeAlanDeMaid();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
