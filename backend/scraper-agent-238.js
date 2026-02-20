// Rodgers Estates scraper using Playwright with Crawlee
// Agent ID: 238
// Usage:
// node backend/scraper-agent-238.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 238;

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
		console.error(` Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.rodgersestates.com/search/",
		suffix:
			".html?showstc=on&showsold=on&instruction_type=Sale&address_keyword=&minprice=&maxprice=&property_type=",
		isRental: false,
		label: "FOR SALE",
		typeIndex: 0,
	},
	{
		url: "https://www.rodgersestates.com/search/?showstc=on&showsold=on&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=",
		isRental: true,
		label: "FOR RENT",
		typeIndex: 1,
	},
];

const pagePropertyCount = {}; // Track properties found per page per type

async function scrapeRodgersEstates() {
	console.log(`\n🚀 Starting Rodgers Estates scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				browserWSEndpoint,
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label, typeIndex } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(1200);

			// Wait for property cards
			await page
				.waitForSelector(".row.property.property-bg", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll(".row.property.property-bg"));
					return cards
						.map((card) => {
							try {
								// Check for Sold/STC or Let/Let Agreed status
								const cornerFlash = card.querySelector(".corner-flash");
								if (cornerFlash) {
									const statusText = cornerFlash.textContent || "";
									// Exclude properties marked as Sold, Sold STC, Let, or Let Agreed
									if (
										/SOLD/i.test(statusText) ||
										/STC/i.test(statusText) ||
										/LET/i.test(statusText) ||
										/LET AGREED/i.test(statusText)
									) {
										return null;
									}
								}

								// Extract property URL
								const linkEl = card.querySelector('a[href*="/property-details/"]');
								if (!linkEl) return null;

								const href = linkEl.getAttribute("href");
								if (!href) return null;
								const link = href.startsWith("/") ? `https://www.rodgersestates.com${href}` : href;

								// Extract price
								let price = "";
								const priceEl = card.querySelector(".thumbnails-price .highlight");
								if (priceEl) {
									const priceText = priceEl.textContent.trim();
									const m = priceText.match(/[£]?([0-9,]+)/);
									if (m) {
										price = parseInt(m[1].replace(/,/g, "")).toLocaleString();
									}
								}

								// Extract address/title
								let title = "";
								const titleEl = card.querySelector(".thumbnails-address a");
								if (titleEl) {
									title = titleEl.textContent.trim();
								}

								// Extract bedrooms
								let bedrooms = null;
								const bedroomsEl = card.querySelector(".property-bedrooms");
								if (bedroomsEl) {
									bedrooms = bedroomsEl.textContent.trim();
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
				// Only paginate if propertyType has urlBase and suffix (rental uses static URL)
				if (propertyType.urlBase && propertyType.suffix) {
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
							console.log(`✅ ${property.title} - ${priceDisplay}`);
						} catch (dbErr) {
							console.error(` DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					}),
				);

				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue first page for each property type - subsequent pages will be auto-enqueued
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Starting ${propertyType.label}`);
		const url = propertyType.url || `${propertyType.urlBase}1${propertyType.suffix}`;
		await crawler.addRequests([
			{
				url,
				userData: {
					pageNum: 1,
					isRental: propertyType.isRental,
					label: propertyType.label,
					typeIndex: propertyType.typeIndex,
				},
			},
		]);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Rodgers Estates scraper - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

(async () => {
	try {
		await scrapeRodgersEstates();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
