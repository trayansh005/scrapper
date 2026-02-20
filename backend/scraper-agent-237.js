// Selectiv scraper using Playwright with Crawlee
// Agent ID: 237
// Usage:
// node backend/scraper-agent-237.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 237;

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
			timeout: 45000,
		});

		const htmlContent = await detailPage.content();

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.address,
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

// Dynamic pagination - no fixed page count, continues until no properties found
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.selectiv.co.uk/properties/for-sale/hide-completed/page/",
		isRental: false,
		label: "FOR SALE",
		typeIndex: 0,
	},
	{
		urlBase: "https://www.selectiv.co.uk/properties/to-rent/hide-completed/page/",
		isRental: true,
		label: "TO RENT",
		typeIndex: 1,
	},
];

const pagePropertyCount = {}; // Track properties found per page per type

async function scrapeSelectiv() {
	console.log(`\n🚀 Starting Selectiv scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(1000);

			// Wait for property cards/links
			await page
				.waitForSelector('a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]', {
					timeout: 20000,
				})
				.catch(() => console.log(` No property links found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const results = [];
					const seenLinks = new Set();

					const links = Array.from(
						document.querySelectorAll(
							'a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]',
						),
					);

					links.forEach((link) => {
						const h3 = link.querySelector("h3");
						const h2 = link.querySelector("h2");

						if (h3 && h2) {
							const href = link.getAttribute("href");
							if (!href || seenLinks.has(href)) return;
							seenLinks.add(href);

							const card = link.closest("div") || link.parentElement;
							if (card && /Under Offer|Sold STC|SSTC/i.test(card.textContent || "")) return;

							const fullLink = href.startsWith("http")
								? href
								: "https://www.selectiv.co.uk" + (href.startsWith("/") ? "" : "/") + href;

							const priceText = h3.textContent.trim();
							const address = h2.textContent.trim();

							let bedrooms = null;
							try {
								// Target the specific row we know contains the numbers
								const iconRow = link.querySelector('div[class*="flex"][class*="items-center"]');
								if (iconRow) {
									const numberSpans = Array.from(iconRow.querySelectorAll("span")).filter((s) =>
										/^\d+$/.test(s.textContent.trim()),
									);
									if (numberSpans.length > 0) {
										bedrooms = numberSpans[0].textContent.trim();
									}
								}

								if (!bedrooms) {
									const bedSpan = link.querySelector('span[class*="mr-20"], .font-bold.text-17');
									if (bedSpan && /^\d+$/.test(bedSpan.textContent.trim())) {
										bedrooms = bedSpan.textContent.trim();
									}
								}
							} catch (e) {
								// ignore
							}

							results.push({ link: fullLink, priceText, address, bedrooms });
						}
					});
					return results;
				} catch (err) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);
			pagePropertyCount[`${typeIndex}-${pageNum}`] = properties.length;

			// If properties found, enqueue next page
			if (properties.length > 0) {
				const propertyType = PROPERTY_TYPES[typeIndex];
				await crawler.addRequests([
					{
						url: `${propertyType.urlBase}${pageNum + 1}`,
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
							log.info(` Skipping duplicate: ${property.address}`);
							return;
						}
						processedUrls.add(property.link);

						try {
							const priceNum = parsePrice(property.priceText);

							if (priceNum === null) {
								log.warn(` No price found: ${property.address}`);
								return;
							}

							const result = await updatePriceByPropertyURLOptimized(
								property.link.trim(),
								priceNum,
								property.address,
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
							console.log(`✅ ${property.address} - ${priceDisplay}`);
						} catch (dbErr) {
							console.error(` DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					}),
				);
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue first page for each property type - subsequent pages will be auto-enqueued
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Starting ${propertyType.label}`);
		await crawler.addRequests([
			{
				url: `${propertyType.urlBase}1`,
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
		`\n✅ Completed Selectiv scraper - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

(async () => {
	try {
		await scrapeSelectiv();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
