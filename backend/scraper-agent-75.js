// Kinleigh Folkard & Hayward (KFH) scraper using Playwright with Crawlee
// Agent ID: 75
// Modeled after agent 84 with full URL pagination (one by one)
//
// Usage:
// node backend/scraper-agent-75.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
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

const processedUrls = new Set();

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

/**
 * Scrapes detail page for coordinates and saved property
 */
async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// Wait for the property content to load
		await detailPage.waitForTimeout(2000);

		const html = await detailPage.content();

		// Process property into database with coordinates from HTML
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			html,
		);

		counts.totalScraped++;
		counts.totalSaved++;
		if (isRental) counts.savedRentals++;
		else counts.savedSales++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNumber, isRental, label, totalPages } = request.userData;
	logger.page(pageNumber, label, `Processing ${request.url}`, totalPages || null);

	try {
		// Accept cookies once per session/page if needed
		const cookieButton = await page.$("#onetrust-accept-btn-handler");
		if (cookieButton) {
			await cookieButton.click().catch(() => {});
			await page.waitForTimeout(1000);
		}

		// Wait for listing container
		await page
			.waitForSelector(".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0", {
				timeout: 20000,
			})
			.catch(() => {});

		// Extract properties
		const properties = await page.evaluate(() => {
			const cardSelector = ".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0";
			const cards = Array.from(document.querySelectorAll(cardSelector));

			return cards
				.map((card) => {
					try {
						let link = null;
						const anchors = card.querySelectorAll("a[href]");
						for (const a of anchors) {
							const href = a.getAttribute("href");
							if (
								href &&
								(href.includes("/property-for-sale/") || href.includes("/property-to-rent/"))
							) {
								link = a.href;
								break;
							}
						}

						const title =
							card.querySelector("h3")?.textContent?.trim() ||
							card
								.querySelector("a[class*='PropertyCard__StyledAddressLink']")
								?.textContent?.trim();

						const priceText =
							card.querySelector(".highlight-text")?.textContent?.trim() ||
							card
								.querySelector(".PropertyPriceAndStatus__StyledPrice-sc-1dv7ovq-0 h2")
								?.textContent?.trim() ||
							card
								.querySelector("p[class*='PropertyPriceAndStatus__StyledPrice']")
								?.textContent?.trim() ||
							card.querySelector(".PropertyCard__StyledPrice")?.textContent?.trim();

						// Bedroom extraction
						let bedrooms = null;
						const bedSpan = card.querySelector(".p-bed");
						if (bedSpan) {
							const text = bedSpan.parentElement?.textContent?.trim() || "";
							const match = text.match(/(\d+)\s+bedrooms?/i);
							if (match) bedrooms = match[1];
						}
						if (!bedrooms) {
							const bedIcon = card.querySelector(".icon-bed");
							if (bedIcon) bedrooms = bedIcon.nextElementSibling?.textContent?.trim();
						}
						if (!bedrooms) {
							const items = card.querySelectorAll("span[class*='PropertyMeta__StyledMetaItem']");
							for (const it of items) {
								if (it.textContent.toLowerCase().includes("bedroom")) {
									const m = it.textContent.match(/(\d+)/);
									if (m) {
										bedrooms = m[1];
										break;
									}
								}
							}
						}

						// Sold status from card
						const statusText =
							card.querySelector(".status-label, .sold-label")?.textContent?.trim() || "";

						return { link, title, priceText, bedrooms, statusText };
					} catch (e) {
						return null;
					}
				})
				.filter((p) => p && p.link);
		});

		for (const property of properties) {
			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			if (isSoldProperty(property.statusText || "")) {
				logger.property(
					pageNumber,
					label,
					property.title || "Property",
					property.priceText || "N/A",
					property.link,
					isRental,
					totalPages || null,
					"UNCHANGED",
				);
				continue;
			}

			const price = parsePrice(property.priceText);
			if (!price) {
				logger.property(
					pageNumber,
					label,
					property.title || "Property",
					"N/A",
					property.link,
					isRental,
					totalPages || null,
					"ERROR",
				);
				continue;
			}

			const updateResult = await updatePriceByPropertyURLOptimized(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";

			if (updateResult.updated) {
				counts.totalSaved++;
				propertyAction = "UPDATED";
			} else if (updateResult.isExisting) {
				counts.totalScraped++;
			}

			if (!updateResult.isExisting && !updateResult.error) {
				propertyAction = "CREATED";
				logger.property(
					pageNumber,
					label,
					property.title || "Property",
					`£${price}`,
					property.link,
					isRental,
					totalPages || null,
					propertyAction,
				);
				await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
				// Delay between detail requests
				await new Promise((r) => setTimeout(r, 2000));
			} else {
				// Log UNCHANGED for existing non-updated properties
				logger.property(
					pageNumber,
					label,
					property.title || "Property",
					`£${price}`,
					property.link,
					isRental,
					totalPages || null,
					propertyAction,
				);
			}
		}
		// Delay between listing pages
		await new Promise((r) => setTimeout(r, 3000));
	} catch (error) {
		logger.error("Error in handleListingPage", error, pageNumber, label);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler() {
	return new PlaywrightCrawler({
		maxConcurrency: 1, // Be polite
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		launchContext: {
			launchOptions: {
				headless: true,
				args: [
					"--no-sandbox",
					"--disable-setuid-sandbox",
					"--disable-dev-shm-usage",
					"--disable-accelerated-2d-canvas",
					"--disable-gpu",
				],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeKFH() {
	logger.step(`Starting KFH scraper (Agent ${AGENT_ID})`);
	const startPageArg = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	const startPage = Number.isFinite(startPageArg) && startPageArg > 0 ? startPageArg : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const crawler = createCrawler();

	const PROPERTY_TYPES = [
		{
			urlBase: "https://www.kfh.co.uk/property/for-sale/in-london/exclude-sale-agreed/",
			totalPages: 94,
			isRental: false,
			label: "SALES",
		},
		{
			urlBase: "https://www.kfh.co.uk/property/to-rent/in-london/exclude-let-agreed/",
			totalPages: 39,
			isRental: true,
			label: "RENTALS",
		},
	];

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		for (let p = Math.max(1, startPage); p <= type.totalPages; p++) {
			allRequests.push({
				url: `${type.urlBase}page-${p}/`,
				userData: {
					pageNumber: p,
					isRental: type.isRental,
					label: type.label,
					totalPages: type.totalPages,
				},
			});
		}
	}

	await crawler.run(allRequests);

	logger.step(
		`Finished KFH - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn(`Partial run detected (startPage=${startPage}). Skipping updateRemoveStatus.`);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeKFH();
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
