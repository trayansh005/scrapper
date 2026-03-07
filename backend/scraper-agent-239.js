// HKL Home scraper using Playwright with Crawlee
// Agent ID: 239
// Usage:
// node backend/scraper-agent-239.js

const { PlaywrightCrawler } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized, processPropertyWithCoordinates, } = require("./lib/db-helpers.js");
const { parsePrice } = require("./lib/property-helpers.js");
const logger = require("./lib/logger-helpers.js");
const { blockNonEssentialResources, sleep } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 239;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	const scrapeStartTime = new Date();
	await sleep(500);

	const detailPage = await browserContext.newPage();

	try {
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		const htmlContent = await detailPage.content();

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
		);

		stats.totalSaved++;
		stats.totalScraped++;
		if (isRental) stats.savedRentals++;
		else stats.savedSales++;
	} catch (error) {
		logger.error(` Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// Dynamic pagination - no fixed page count, continues until no properties found
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.hklhome.co.uk/search/",
		suffix:
			".html?showstc=off&showsold=off&instruction_type=Sale&ajax_polygon=&minprice=&maxprice=&property_type=",
		isRental: false,
		label: "FOR SALE",
		typeIndex: 0,
	},
	{
		urlBase: "https://www.hklhome.co.uk/search/",
		suffix:
			".html?showstc=off&showsold=off&instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=",
		isRental: true,
		label: "FOR LETTING",
		typeIndex: 1,
	},
];

const pagePropertyCount = {}; // Track properties found per page per type

async function scrapeHKLHome() {
	console.log(`\n🚀 Starting HKL Home scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		navigationTimeoutSecs: 30,
		requestHandlerTimeoutSecs: 300,

		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],

		launchContext: {
			launchOptions: {
				browserWSEndpoint,
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label, typeIndex } = request.userData;

			logger.page(AGENT_ID, pageNum, null, label)

			await page.waitForTimeout(1200);

			// Wait for property cards
			await page
				.waitForSelector(".property", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll(".property"));
					return cards
						.map((card) => {
							try {
								// Extract property URL
								const linkEl = card.querySelector('a[href*="/property-details/"]');
								if (!linkEl) return null;

								const href = linkEl.getAttribute("href");
								if (!href) return null;
								const link = href.startsWith("/") ? `https://www.hklhome.co.uk${href}` : href;

								// Extract price and status from h4
								let price = "";
								let status = "";
								const priceEl = card.querySelector("h4");
								if (priceEl) {
									const priceText = priceEl.textContent.trim();
									// Check for "For Sale" or "To Let" status
									status = priceText;
									const m = priceText.match(/£([0-9,]+)/);
									if (m) {
										price = parseInt(m[1].replace(/,/g, "")).toLocaleString();
									}
								}

								// Skip if status contains sold/let indicators
								if (/sold/i.test(status) || /stc/i.test(status) || /let agreed/i.test(status)) {
									return null;
								}

								// Extract address/title from h3
								let title = "";
								const titleEl = card.querySelector("h3 a");
								if (titleEl) {
									title = titleEl.textContent.trim();
								}

								// Extract bedrooms
								let bedrooms = null;
								const bedroomIcon = card.querySelector(
									'img[src*="bed-purple"], img[alt="bedrooms"]',
								);
								if (bedroomIcon) {
									// Get the next sibling text node or parent's text
									const parent = bedroomIcon.parentElement;
									if (parent) {
										const text = parent.textContent.trim();
										const match = text.match(/(\d+)/);
										if (match) {
											bedrooms = match[1];
										}
									}
								}

								return { link, title, price, bedrooms };
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);
			pagePropertyCount[`${typeIndex}-${pageNum}`] = properties.length;

			// If properties found, enqueue next page
			if (properties.length > 0) {
				const propertyType = PROPERTY_TYPES[typeIndex];
				const url = `${propertyType.urlBase}${pageNum + 1}${propertyType.suffix}`;
				await crawler.addRequests([
					{
						url,
						userData: {
							pageNum: pageNum + 1,
							isRental,
							label,
							typeIndex,
						},
					},
				]);
			}

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						if (processedUrls.has(property.link)) {
							log.info(` Skipping duplicate: ${property.title}`);
							return;
						}
						processedUrls.add(property.link);

						try {
							const priceNum = parsePrice(property.price);

							if (priceNum === null) {
								log.warn(` No price found: ${property.title}`);
								return;
							}

							const result = await updatePriceByPropertyURLOptimized(
								property.link.trim(),
								priceNum,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
							);

							if (result.updated) {
								stats.totalSaved++;
							}

							if (!result.isExisting && !result.error) {
								await scrapePropertyDetail(
									page.context(),
									{
										...property,
										price: priceNum,
									},
									isRental,
								);
							}

							const priceDisplay = formatPriceUk(priceNum);
							logger.property({
								agentId: AGENT_ID,
								title: property.title,
								price: priceDisplay,
								isRental,
							})
						} catch (dbErr) {
							logger.error(` DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					}),
				);

				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Starting ${propertyType.label}`);
		const url = `${propertyType.urlBase}1${propertyType.suffix}`;

		const requests = [
			{
				url,
				userData: {
					pageNum: 1,
					isRental: propertyType.isRental,
					label: propertyType.label,
					typeIndex: propertyType.typeIndex,
				},
			},
		];
		await crawler.addRequests(requests);
	}

	await crawler.run();

	logger.step(
		`\n✅ Completed HKL Home scraper - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logger.step(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
	try {
		await scrapeHKLHome();
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		logger.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
