// Fine & Country scraper using Playwright with Crawlee
// Agent ID: 70
//
// Usage:
// node backend/scraper-agent-70.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 70;
let totalScraped = 0;
let totalSaved = 0;
const scrapeStartTime = new Date();
const logger = createAgentLogger(AGENT_ID);

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlPath: "sales/property-for-sale",
		totalPages: 355,
		recordsPerPage: 10,
		isRental: false,
		label: "SALES",
	},
	{
		urlPath: "lettings/property-to-rent",
		totalPages: 21,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeFineAndCountry() {
	logger.step(`Starting Fine & Country scraper (Agent ${AGENT_ID})...`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1, // Process one page at a time
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, totalPages, isRental, label, isDetailPage, propertyData } = request.userData;
			if (isDetailPage) {
				try {
					logger.step(`[Detail] Scraping coordinates for: ${propertyData.title}`);
					await page.waitForLoadState("networkidle");

					const coordinates = await page.evaluate(() => {
						const bodyText = document.body.innerHTML;
						const match = bodyText.match(/"latitude":\s*([0-9.-]+).*?"longitude":\s*([0-9.-]+)/);
						if (match) {
							return {
								latitude: parseFloat(match[1]),
								longitude: parseFloat(match[2]),
							};
						}
						return null;
					});

					if (coordinates) {
						logger.step(`[Detail] Found coordinates: ${coordinates.latitude}, ${coordinates.longitude}`);
						await updatePriceByPropertyURL(
							propertyData.link,
							propertyData.price,
							propertyData.title,
							propertyData.bedrooms,
							AGENT_ID,
							isRental,
							coordinates.latitude,
							coordinates.longitude,
						);
						totalSaved++;
						totalScraped++;

						logger.property(
							pageNum,
							label,
							propertyData.title,
							propertyData.price,
							propertyData.link,
							isRental,
							totalPages,
							"CREATED",
							coordinates.latitude,
							coordinates.longitude,
						);
					} else {
						logger.step(`[Detail] Coordinates NOT found for: ${propertyData.link}`);
					}
				} catch (err) {
					logger.error("Detail page error", err, pageNum, label);
				}
				return;
			}

			// Processing listing page
			logger.page(pageNum, label, `Processing: ${request.url}`, totalPages);

			// Wait for properties to load
			await page.waitForTimeout(2000);
			await page.waitForSelector(".card-property", { timeout: 30000 }).catch(() => {
				logger.page(pageNum, label, `No properties found on page ${pageNum}`, totalPages);
			});

			// Extract all properties from the page
			const properties = await page.$$eval(".card-property", (cards) => {
				const results = [];

				cards.forEach((card) => {
					try {
						// Extract link from .property-title-link
						const linkEl = card.querySelector(".property-title-link");
						const link = linkEl ? linkEl.getAttribute("href") : null;

						// Extract title from .property-title-link span
						const titleEl = card.querySelector(".property-title-link span");
						const title = titleEl ? titleEl.textContent.trim() : null;

						// Extract price from .property-price
						const priceEl = card.querySelector(".property-price");
						let price = null;
						if (priceEl) {
							const priceText = priceEl.textContent.trim();
							const priceMatch = priceText.match(/£([\d,]+)/);
							if (priceMatch) {
								price = priceMatch[1]; // DO NOT remove commas
							}
						}

						// Extract bedrooms from .card__list-rooms li p
						const bedroomsEl = card.querySelector(".card__list-rooms li p");
						const bedrooms = bedroomsEl ? bedroomsEl.textContent.trim() : null;

						if (link && title && price) {
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

			logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

			// Process properties in batches of 5
			for (const property of properties) {
				// ⏭ Skip sold properties
				if (isSoldProperty(property.title)) {
					logger.property(
						pageNum,
						label,
						property.title,
						property.price,
						property.link,
						isRental,
						totalPages,
						"SKIPPED",
					);
					continue;
				}
				// Format UK price with commas
				const formattedPrice = formatPriceUk(property.price);
				if (!formattedPrice) continue;

				try {
					// Optimized DB update - checks if property exists and updates price in one step
					const result = await updatePriceByPropertyURLOptimized(
						property.link,
						formattedPrice,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
					);

					// If property exists → price updated only
					if (result.updated) {
						totalSaved++;
						logger.property(
							pageNum,
							label,
							property.title,
							formattedPrice,
							property.link,
							isRental,
							totalPages,
							"UPDATED",
						);
					} else if (result.isExisting) {
						logger.property(
							pageNum,
							label,
							property.title,
							formattedPrice,
							property.link,
							isRental,
							totalPages,
							"UNCHANGED",
						);
					}

					// If property is NEW → queue detail page
					if (!result.isExisting && !result.error) {
						await crawler.addRequests([
							{
								url: property.link,
								userData: {
									isDetailPage: true,
									propertyData: {
										...property,
										price: formattedPrice,
									},
									isRental,
									pageNum,
									totalPages,
									label,
								},
							},
						]);
					}
				} catch (err) {
					logger.error("Optimization error", err, pageNum, label);
				}
			}
		},

		failedRequestHandler({ request }) {
			const { pageNum, label } = request.userData;
			logger.error(`Request failed: ${request.url}`, null, pageNum, label);
		},
	});

	// Add initial listing page URLs for both sales and lettings
	const requests = [];
	for (const propertyType of PROPERTY_TYPES) {
		for (let page = 1; page <= propertyType.totalPages; page++) {
			requests.push({
				url: `https://www.fineandcountry.co.uk/${propertyType.urlPath}/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${page}`,
				userData: {
					pageNum: page,
					totalPages: propertyType.totalPages,
					isRental: propertyType.isRental,
					label: propertyType.label,
					isDetailPage: false,
				},
			});
		}
	}

	await crawler.run(requests);

	logger.step(
		`Completed Fine & Country - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`,
	);
}

// Main execution
(async () => {
	try {
		await scrapeFineAndCountry();
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
