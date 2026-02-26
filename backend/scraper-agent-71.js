// Hawes & Co scraper using Playwright with Crawlee
// Agent ID: 71
//
// Usage:
// node backend/scraper-agent-71.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 71;
let totalScraped = 0;
let totalSaved = 0;
const logger = createAgentLogger(AGENT_ID);

// Configuration for sales and rentals
const PROPERTY_TYPES = [
	{
		urlPath: "properties-for-sale",
		totalRecords: 185,
		recordsPerPage: 12,
		isRental: false,
		label: "SALE",
	},
	{
		urlPath: "properties-to-rent",
		totalRecords: 36,
		recordsPerPage: 12,
		isRental: true,
		label: "RENTAL",
	},
];

async function scrapeHawesAndCo() {
	logger.step(`Starting Hawes & Co scraper (Agent ${AGENT_ID})...`);

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
				try {
					await blockNonEssentialResources(page);
					await page.waitForLoadState("networkidle");

					const htmlContent = await page.content();

					// Extract coordinates using common helper
					const coords = extractCoordinatesFromHTML(htmlContent);

					// Detect sold property
					const sold = isSoldProperty(htmlContent);

					logger.page(0, "", `Detail URL: ${propertyData.link}`);
					logger.step(
						`Extracted coords: ${coords?.latitude || "No Lat"}, ${coords?.longitude || "No Lng"}`,
					);
					logger.step(`Is Sold: ${sold}`);

					await updatePriceByPropertyURLOptimized({
						link: propertyData.link,
						price: propertyData.price,
						title: propertyData.title,
						bedrooms: propertyData.bedrooms,
						agentId: AGENT_ID,
						isRental,
						latitude: coords?.latitude || null,
						longitude: coords?.longitude || null,
						isSold: sold,
					});

					totalSaved++;
					totalScraped++;

					logger.property(
						propertyData.title,
						`£${propertyData.price}`,
						coords?.latitude && coords?.longitude
							? `${coords.latitude}, ${coords.longitude}`
							: "No coords",
					);
				} catch (error) {
					logger.error(`Error saving property: ${error.message}`);
				}

				return;
			} else {
				// Processing listing page
				logger.page(pageNum, label, request.url);

				// Wait for properties to load
				await page.waitForTimeout(2000);
				await page.waitForSelector(".property", { timeout: 30000 }).catch(() => {
					console.log(`⚠️ No properties found on page ${pageNum}`);
				});

				// Extract all properties from the page
				const { properties, debug } = await page.$$eval(".property", (listings) => {
					const results = [];
					const debugData = { total: listings.length, processed: 0 };

					listings.forEach((listing) => {
						try {
							debugData.processed++;

							// Extract link from data-link attribute
							const innerWrapper = listing.querySelector(".inner_wrapper");
							let link = innerWrapper ? innerWrapper.getAttribute("data-link") : null;
							if (link && !link.startsWith("http")) {
								link = "https://www.hawesandco.co.uk" + link;
							}

							// Extract price from .sale_price
							const priceEl = listing.querySelector(".sale_price");
							let price = null;
							if (priceEl) {
								const priceText = priceEl.textContent.trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								if (priceMatch) {
									const priceText = priceEl.textContent.trim();
									price = formatPriceUk(priceText);
								}
							}

							// Extract title from .blurb
							let title = null;
							const blurbEl = listing.querySelector(".blurb");
							if (blurbEl) {
								title = blurbEl.textContent.trim();
							}
							if (!title) {
								const headerLinkEl = listing.querySelector(".info_section__header__left a");
								if (headerLinkEl) {
									title = headerLinkEl.textContent.trim();
								}
							}

							// Extract bedrooms from .info_section__room.beds
							let bedrooms = null;
							const bedroomEl = listing.querySelector(".info_section__room.beds");
							if (bedroomEl) {
								const bedroomText = bedroomEl.textContent.trim();
								const bedroomMatch = bedroomText.match(/(\d+)/);
								if (bedroomMatch) {
									bedrooms = bedroomMatch[1];
								}
							}

							// Store debug info for first property
							if (results.length === 0) {
								debugData.firstProperty = {
									hasLink: !!link,
									hasTitle: !!title,
									hasPrice: !!price,
									price: price,
									title: title ? title.substring(0, 60) : null,
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

				logger.step(`Extraction debug: ${JSON.stringify(debug)}`);
				logger.page(pageNum, label, `Found ${properties.length} properties`);

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
						await new Promise((resolve) => setTimeout(resolve, 1000));
					}
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Add initial listing page URLs for both sales and rentals
	const requests = [];

	for (const propertyType of PROPERTY_TYPES) {
		const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
		logger.step(
			`Queueing ${propertyType.label} properties (${propertyType.totalRecords} total, ${totalPages} pages)`,
		);

		for (let page = 1; page <= totalPages; page++) {
			requests.push({
				url: `https://www.hawesandco.co.uk/${propertyType.urlPath}/all-properties/!/page/${page}`,
				userData: {
					isDetailPage: false,
					pageNum: page,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}
	}

	await crawler.run(requests);

	console.log(
		`\n✅ Completed Hawes & Co - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`,
	);
}

// Main execution
(async () => {
	try {
		await scrapeHawesAndCo();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
