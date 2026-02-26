// Fine & Country scraper using Playwright with Crawlee
// Agent ID: 70
//
// Usage:
// node backend/scraper-agent-70.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 70;
let totalScraped = 0;
let totalSaved = 0;
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
			const { pageNum, isRental, label, isDetailPage, propertyData } = request.userData;
			if (isDetailPage) {
				try {
					logger.step("DETAIL PAGE start");
					logger.page(null, "DETAIL", `Opening URL: ${page.url()}`);

					// Wait for JS to fully load
					await page.waitForLoadState("networkidle");
					logger.step("Page fully loaded");

					const coordinates = await page.evaluate(() => {
						const bodyText = document.body.innerHTML;

						// in-browser logs omitted

						const match = bodyText.match(/"latitude":\s*([0-9.-]+).*?"longitude":\s*([0-9.-]+)/);

						if (match) {
							// match found in browser
							return {
								latitude: parseFloat(match[1]),
								longitude: parseFloat(match[2]),
							};
						}

						return null;
					});

					logger.property(propertyData.title, `coords`, coordinates || null);

					if (coordinates) {
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

						logger.property(
							propertyData.title,
							`Saved with coordinates`,
							`${coordinates.latitude}, ${coordinates.longitude}`,
						);
						totalSaved++;
					} else {
						logger.step(`Coordinates NOT found for: ${propertyData.link}`);
					}
				} catch (err) {
					logger.error("Detail page error:", err?.message || err);
				}

				logger.step("DETAIL PAGE end");
				return;
			}

			// Processing listing page
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Wait for properties to load
			await page.waitForTimeout(2000);
			await page.waitForSelector(".card-property", { timeout: 30000 }).catch(() => {
				console.log(`⚠️ No properties found on page ${pageNum}`);
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

			logger.page(pageNum, label, `Found ${properties.length} properties`);

			// Process properties in batches of 5
			for (const property of properties) {
				// ⏭ Skip sold properties
				if (isSoldProperty(property.title)) {
					logger.step(`Skipping sold: ${property.title}`);
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
								},
							},
						]);
					}
				} catch (err) {
					logger.error("Optimization error:", err?.message || err);
				}
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
		},
	});

	// Process property types one by one
	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label} properties (${propertyType.totalPages} pages)`);

		// Add all pages to the queue
		const requests = [];
		for (let page = 1; page <= propertyType.totalPages; page++) {
			requests.push({
				url: `https://www.fineandcountry.co.uk/${propertyType.urlPath}/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${page}`,
				userData: {
					pageNum: page,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	logger.step(
		`Completed Fine & Country - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`,
	);
}

// Local implementation of updateRemoveStatus
async function updateRemoveStatus(agent_id) {
	try {
		const remove_status = 1;
		await promisePool.query(
			`UPDATE property_for_rent SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
			[remove_status, agent_id],
		);
		logger.step(`Removed old properties for agent ${agent_id}`);
	} catch (error) {
		logger.error("Error updating remove status:", error?.message || error);
	}
}

// Main execution
(async () => {
	try {
		await scrapeFineAndCountry();
		await updateRemoveStatus(AGENT_ID);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
