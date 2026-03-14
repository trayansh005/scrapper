// BHHS London Properties scraper using Playwright with Crawlee
// Agent ID: 63
//
// Usage:
// node backend/scraper-agent-63.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 63;
let totalScraped = 0;
let totalSaved = 0;
const scrapeStartTime = new Date(); // Record start time for remove status
const logger = createAgentLogger(AGENT_ID);

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlPath: "properties-for-sale",
		totalRecords: 116,
		recordsPerPage: 20,
		isRental: false,
		label: "SALES",
	},
	{
		urlPath: "properties-for-rent",
		totalRecords: 74,
		recordsPerPage: 20,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeBHHSLondon() {
	logger.step(`Starting BHHS London Properties scraper (Agent ${AGENT_ID})...`);

	const crawler = new PlaywrightCrawler({
		navigationTimeoutSecs: 60,
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
			},
		},

		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],

		async requestHandler({ page, request }) {
			const { isDetailPage, propertyData, pageNum, totalPages, isRental, label } = request.userData;

			if (isDetailPage) {
				try {
					logger.step(`[Detail] Scraping coordinates for: ${propertyData.title}`);
					await page.waitForLoadState("networkidle");
					// ✅ STEP 4 — Extract coordinates using helper
					const html = await page.content();
					const coords = extractCoordinatesFromHTML(html);

					if (coords?.latitude && coords?.longitude) {
						logger.step(`[Detail] Found coordinates: ${coords.latitude}, ${coords.longitude}`);
					}

					await updatePriceByPropertyURL(
						propertyData.link.trim(),
						propertyData.price,
						propertyData.title,
						propertyData.bedrooms,
						AGENT_ID,
						isRental,
						coords?.latitude || null,
						coords?.longitude || null,
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
						coords?.latitude || null,
						coords?.longitude || null,
					);
				} catch (error) {
					logger.error(`Error saving property: ${error.message}`, error, pageNum, label);
				}
			} else {
				// Processing listing page
				logger.page(pageNum, label, `Processing: ${request.url}`, totalPages);

				// Wait for properties to load
				await page.waitForTimeout(2000);
				await page.waitForSelector(".property-card", { timeout: 30000 }).catch(() => {
					logger.page(pageNum, label, `No properties found on page ${pageNum}`, totalPages);
				});

				// Extract all properties from the page
				const { properties, debug } = await page.$$eval(".property-card", (cards) => {
					const results = [];
					const debugData = { total: cards.length, processed: 0 };

					cards.forEach((card) => {
						try {
							debugData.processed++;

							// Extract link
							const linkEl = card.querySelector("a");
							const link = linkEl ? linkEl.getAttribute("href") : null;

							// Extract title from h3.md-heading
							const titleEl = card.querySelector("h3.md-heading");
							const title = titleEl ? titleEl.textContent.trim() : null;

							// Extract bedrooms from first p.text-sm.text-white
							let bedrooms = null;
							const bedroomsEl = card.querySelector("p.text-sm.text-white");
							if (bedroomsEl) {
								const bedroomsText = bedroomsEl.textContent.trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*Bedrooms/);
								if (bedroomsMatch) {
									bedrooms = bedroomsMatch[1];
								}
							}

							// Extract price
							let price = null;
							const priceEl = card.querySelector(".price");
							if (priceEl) {
								const priceText = priceEl.textContent.trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								if (priceMatch) {
									price = priceMatch[1]; // DO NOT remove commas
								}
							} else {
								// Try alternative price location
								const altPriceEl = card.querySelector("p.md-heading:last-child");
								if (altPriceEl) {
									const priceText = altPriceEl.textContent.trim();
									if (priceText.includes("POA")) {
										price = "POA";
									} else {
										const priceMatch = priceText.match(/£([\d,]+)/);
										if (priceMatch) {
											price = priceMatch[1].replace(/,/g, "");
										}
									}
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
				logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

				// Add detail page requests to the queue with delay
				for (const property of properties) {
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

					const price = formatPriceUk(property.price);
					if (!price) continue;

					try {
						const result = await updatePriceByPropertyURLOptimized(
							property.link,
							price,
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
								price,
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
								price,
								property.link,
								isRental,
								totalPages,
								"UNCHANGED",
							);
						}

						// If property is NEW → then go to detail page
						if (!result.isExisting && !result.error) {
							await crawler.addRequests([
								{
									url: property.link,
									userData: {
										isDetailPage: true,
										propertyData: {
											...property,
											price,
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
		const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
		logger.step(
			`🏠 Queueing ${propertyType.label} properties (${propertyType.totalRecords} total, ${totalPages} pages)`,
		);

		for (let page = 1; page <= totalPages; page++) {
			requests.push({
				url: `https://www.bhhslondonproperties.com/${propertyType.urlPath}?location=&page=${page}`,
				userData: {
					isDetailPage: false,
					pageNum: page,
					totalPages: totalPages,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}
	}

	await crawler.run(requests);

	logger.step(
		`Completed BHHS London Properties - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`,
	);
}

// Main execution
(async () => {
	try {
		await scrapeBHHSLondon();
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
