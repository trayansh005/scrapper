// Richard James scraper using Playwright with Crawlee
// Agent ID: 232
// Usage:
// node backend/scraper-agent-232.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 232;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// Two searches:
// - For Sale: 408 properties, 18 per page => 23 pages
// - To Let: 28 properties, 18 per page => 2 pages
const PROPERTY_TYPES = [
	{
		urlBase: "https://richardjames.uk/search-results/page", // append /{page}/?keyword&status%5B0%5D=for-sale
		totalRecords: 408,
		recordsPerPage: 18,
		totalPages: 23,
		isRental: false,
		label: "FOR SALE",
		suffix: "/?keyword&status%5B0%5D=for-sale",
	},
	{
		urlBase: "https://richardjames.uk/search-results/page", // append /{page}/?keyword&status[0]=to-let&...
		totalRecords: 28,
		recordsPerPage: 18,
		totalPages: 2,
		isRental: true,
		label: "TO LET",
		suffix:
			"/?keyword&status%5B0%5D=to-let&min-price=0&max-price=2500000&bathrooms&bedrooms&property_id",
	},
];

async function scrapeRichardJames() {
	console.log(`\n🚀 Starting RichardJames scraper (Agent ${AGENT_ID})...\n`);

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

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(2000);

			// Wait for listing cards
			await page
				.waitForSelector(".item-listing-wrap, .item-listing-wrap-v6, .item-listing-wrap-v6.card", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No listing container found on page ${pageNum}`));

			// Extract properties
			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(
							".item-listing-wrap, .item-listing-wrap-v6, .item-listing-wrap-v6.card",
						),
					);

					return items
						.map((el) => {
							try {
								// Prefer the title anchor for the detail URL (h2.item-title a). Fall back to thumbnail anchor.
								const linkEl =
									el.querySelector("h2.item-title a") ||
									el.querySelector(".item-title a") ||
									el.querySelector(".listing-image-wrap a") ||
									el.querySelector(".rh_list_card__thumbnail a");
								const link = linkEl ? linkEl.href : null;
								const title =
									el.querySelector("h2.item-title a, .item-title a")?.textContent?.trim() || "";
								const rawPrice =
									el.querySelector(".item-price, .item-price .price")?.textContent?.trim() || "";
								// Extract numeric-only price (remove currency symbols, text and commas)
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}
								const status =
									el.querySelector(".label-status, .status")?.textContent?.trim() || "";
								const beds =
									el.querySelector(".hz-figure, .figure, .hz-figure")?.textContent?.trim() || null;

								// Skip if link missing
								if (!link) return null;

								return { link, title, price, bedrooms: beds, status };
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

			// Batch processing
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							const htmlContent = await detailPage.content();

							try {
								const priceNum = property.price
									? parseFloat(property.price.replace(/[^0-9.]/g, ""))
									: null;

								if (priceNum === null) {
									log.warn(`No price found: ${property.title}`);
									return;
								}

								// Extract coordinates from HTML
								const coords = await extractCoordinatesFromHTML(htmlContent);

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
									await processPropertyWithCoordinates(
										property.link,
										priceNum,
										property.title,
										property.bedrooms || null,
										AGENT_ID,
										isRental,
										htmlContent,
										coords.latitude,
										coords.longitude,
									);
								} else if (result.isExisting && (coords.latitude || coords.longitude)) {
									const priceDisplay = formatPriceUk(priceNum);
									console.log(
										`✅ ${property.title} - ${priceDisplay}${
											coords.latitude ? ` - (${coords.latitude}, ${coords.longitude})` : ""
										}`,
									);
								} else {
									const priceDisplay = formatPriceUk(priceNum);
									console.log(`✅ ${property.title} - ${priceDisplay}`);
								}

								stats.totalScraped++;
								if (isRental) stats.savedRentals++;
								else stats.savedSales++;
							} catch (dbErr) {
								console.error(`❌ DB error for ${property.link}: ${dbErr?.message || dbErr}`);
							}
						} catch (err) {
							// ignore
						} finally {
							await detailPage.close();
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

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		// Both For Sale and To Let use /page/{pg}/ + suffix pattern
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}/${pg}/${propertyType.suffix}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed RichardJames - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

(async () => {
	try {
		await scrapeRichardJames();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
