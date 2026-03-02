// Hawes & Co scraper using Playwright with Crawlee
// Agent ID: 71
//
// Usage:
// node backend/scraper-agent-71.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 71;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

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

					// Detect sold property
					const sold = isSoldProperty(htmlContent);
					if (sold) {
						logger.step(`Skipping sold property: ${propertyData.link}`);
						return;
					}

					const { link, price, title, bedrooms } = propertyData;

					// Check if property already exists and update price if so
					const result = await updatePriceByPropertyURLOptimized(
						link,
						price,
						title,
						bedrooms,
						AGENT_ID,
						isRental,
					);

					if (result.updated) {
						counts.totalSaved++;
						counts.totalScraped++;
						if (isRental) counts.savedRentals++;
						else counts.savedSales++;
					} else if (result.isExisting) {
						counts.totalScraped++;
					}

					let propertyAction = "UNCHANGED";
					if (result.updated) propertyAction = "UPDATED";

					if (!result.isExisting && !result.error) {
						propertyAction = "CREATED";
						// Insert new property — extract coordinates from the detail page HTML
						await processPropertyWithCoordinates(
							link,
							price,
							title,
							bedrooms,
							AGENT_ID,
							isRental,
							htmlContent, // pass HTML so coords are extracted inside
						);
						counts.totalSaved++;
						counts.totalScraped++;
						if (isRental) counts.savedRentals++;
						else counts.savedSales++;
					}

					logger.property(
						pageNum || 0,
						label || "",
						title.substring(0, 40),
						formatPriceDisplay(price, isRental),
						link,
						isRental,
						null,
						propertyAction,
					);
				} catch (error) {
					logger.error(`Error saving property: ${error.message}`);
				}

				return;
			}

			// ----------------------------------------------------------------
			// Listing page handling
			// ----------------------------------------------------------------
			logger.page(pageNum, label, request.url);

			// Wait for properties to load
			await page.waitForTimeout(2000);
			await page.waitForSelector(".property", { timeout: 30000 }).catch(() => {
				logger.warn(`No properties found on page ${pageNum}`);
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
								price = parseInt(priceMatch[1].replace(/,/g, ""), 10);
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
								bedrooms = parseInt(bedroomMatch[1], 10);
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
							results.push({ link, title, price, bedrooms });
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
							pageNum,
							label,
						},
					},
				]);

				// Add delay between detail page requests to avoid rate limiting
				if (i < properties.length - 1) {
					await new Promise((resolve) => setTimeout(resolve, 1000));
				}
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
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

	logger.step(
		`Completed Hawes & Co - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
}

// Main execution
(async () => {
	try {
		const scrapeStartTime = new Date();
		await scrapeHawesAndCo();
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
