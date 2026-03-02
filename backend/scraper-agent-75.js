// Kinleigh Folkard & Hayward (KFH) scraper using Playwright with Crawlee
// Agent ID: 75
// Modeled after agent 39 pattern (positional args, create vs update)
//
// Usage:
// node backend/scraper-agent-75.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	extractCoordinatesFromHTML,
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce logging noise
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 75;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// Configuration for sales and rentals — full KFH URLs
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.kfh.co.uk/property/for-sale/in-london/exclude-sale-agreed/",
		totalRecords: 1689,
		totalPages: 94,
		recordsPerPage: 18,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.kfh.co.uk/property/to-rent/in-london/exclude-let-agreed/",
		totalRecords: 691,
		totalPages: 39,
		recordsPerPage: 18,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeKFH() {
	logger.step(`Starting KFH scraper (Agent ${AGENT_ID})...`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			logger.page(pageNum, label, request.url);

			// Wait for page to load
			await page.waitForTimeout(6000);

			// Accept cookies if banner is present
			const cookieButton = await page.$("#onetrust-accept-btn-handler");
			if (cookieButton) {
				await cookieButton.click();
				await page.waitForTimeout(2000);
			}

			// Try to wait for a known listing container
			await page
				.waitForSelector(
					".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0, .property-card, .result-card",
					{ timeout: 40000 },
				)
				.catch(() => {
					logger.step(`No listing container found on page ${pageNum}`);
				});

			// Extract properties from DOM
			const properties = await page.evaluate(() => {
				try {
					const cardSelector =
						".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0";
					const cards = Array.from(document.querySelectorAll(cardSelector));

					return cards
						.map((card) => {
							try {
								// Link: prefer property detail links
								let link = null;
								const anchors = card.querySelectorAll("a[href]");
								for (const a of anchors) {
									const href = a.getAttribute("href");
									if (
										href &&
										(href.includes("/property-for-sale/") ||
											href.includes("/property-to-rent/"))
									) {
										link = a.href;
										break;
									}
									if (href && href.includes("/property")) {
										link = a.href;
										break;
									}
								}

								// Title
								let title =
									card.querySelector("h3")?.textContent?.trim() ||
									card
										.querySelector("a[class*='PropertyCard__StyledAddressLink']")
										?.textContent?.trim() ||
									null;

								// Price (raw text — will be parsed server-side)
								let price =
									card.querySelector(".highlight-text")?.textContent?.trim() ||
									card
										.querySelector(
											".PropertyPriceAndStatus__StyledPrice-sc-1dv7ovq-0 h2",
										)
										?.textContent?.trim() ||
									card
										.querySelector(
											"p[class*='PropertyPriceAndStatus__StyledPrice']",
										)
										?.textContent?.trim() ||
									card
										.querySelector(".PropertyCard__StyledPrice")
										?.textContent?.trim() ||
									"";

								// Bedrooms
								let bedrooms = null;
								const bedSpan = card.querySelector(".p-bed");
								if (bedSpan) {
									const text =
										bedSpan.parentElement?.textContent?.trim() || "";
									const match = text.match(/(\d+)\s+bedrooms?/i);
									if (match) bedrooms = match[1];
								}
								if (!bedrooms) {
									const bedIcon = card.querySelector(".icon-bed");
									if (bedIcon) {
										bedrooms =
											bedIcon.nextElementSibling?.textContent?.trim();
									}
								}
								if (!bedrooms) {
									const bedSpans = card.querySelectorAll(
										"span[class*='PropertyMeta__StyledMetaItem'], .property-meta-item",
									);
									for (const span of bedSpans) {
										const text = span.textContent?.trim() || "";
										if (text.toLowerCase().includes("bedroom")) {
											const match = text.match(/(\d+)/);
											bedrooms = match ? match[1] : null;
											break;
										}
									}
								}
								if (!bedrooms && link) {
									const urlBedMatch = link.match(/(\d+)-bedroom/i);
									if (urlBedMatch) bedrooms = urlBedMatch[1];
								}

								if (link && title) {
									return { link, title, price, bedrooms };
								}
								return null;
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					return [];
				}
			});

			logger.page(pageNum, label, `Found ${properties.length} properties`);

			// Process properties in batches
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						// Parse price to number
						const price = parsePrice(property.price.toString());
						if (!price) return;

						// Open detail page to get coords + sold status
						const detailPage = await page.context().newPage();
						let htmlContent = "";
						let coords = { latitude: null, longitude: null };
						let sold = false;

						try {
							await blockNonEssentialResources(detailPage);
							await detailPage.goto(property.link, {
								waitUntil: "networkidle",
								timeout: 30000,
							});

							htmlContent = await detailPage.content();

							const extracted = extractCoordinatesFromHTML(htmlContent);
							if (extracted) coords = extracted;

							sold = isSoldProperty(htmlContent);

							logger.step(
								`Coords: ${coords?.latitude || "No Lat"}, ${coords?.longitude || "No Lng"} | Sold: ${sold}`,
							);
						} catch (err) {
							logger.error(`Detail page error: ${err.message || err}`);
						} finally {
							await detailPage.close();
						}

						if (sold) {
							logger.step(`Skipping sold property: ${property.link}`);
							return;
						}

						// --- Agent 39 pattern: check existing → create or update ---
						const result = await updatePriceByPropertyURLOptimized(
							property.link,
							price,
							property.title,
							property.bedrooms,
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
							// Insert new property with coordinates extracted from detail page HTML
							await processPropertyWithCoordinates(
								property.link,
								price,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								htmlContent,
								coords?.latitude || null,
								coords?.longitude || null,
							);
							counts.totalSaved++;
							counts.totalScraped++;
							if (isRental) counts.savedRentals++;
							else counts.savedSales++;
						}

						logger.property(
							pageNum,
							label,
							property.title.substring(0, 40),
							formatPriceDisplay(price, isRental),
							property.link,
							isRental,
							null,
							propertyAction,
						);
					}),
				);

				// Small delay between batches
				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
		},
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
	});

	// Enqueue all listing pages
	for (const propertyType of PROPERTY_TYPES) {
		logger.step(
			`Processing ${propertyType.label} (${propertyType.totalPages} pages, ${propertyType.recordsPerPage} per page)`,
		);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}page-${pg}/`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		if (requests.length > 0) {
			await crawler.run(requests);
		}
	}

	logger.step(
		`Completed KFH - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
}

(async () => {
	try {
		const scrapeStartTime = new Date();
		await scrapeKFH();
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
