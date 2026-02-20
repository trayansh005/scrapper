// Humberts scraper using Playwright with Crawlee
// Agent ID: 230
// Usage:
// node backend/scraper-agent-230.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 230;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// Two searches: sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.humberts.com/search/page/",
	// 	// 182 properties / 12 per page = 16 pages (rounded up)
	// 	totalRecords: 182,
	// 	recordsPerPage: 12,
	// 	totalPages: 16,
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	suffix:
	// 		"/?country=GB&department=residential-sales&tenure&address_keyword&radius=25&commercial_for_sale_to_rent&property_type&minimum_bedrooms&minimum_price&maximum_price&lat&lng",
	// },
	{
		urlBase: "https://www.humberts.com/search/",
		// 3 pages for lettings
		totalRecords: 3 * 12, // approx
		recordsPerPage: 12,
		totalPages: 3,
		isRental: true,
		label: "TO LET",
		suffix:
			"?country=GB&department=residential-lettings&tenure=&address_keyword=&radius=25&commercial_for_sale_to_rent=&property_type=&minimum_bedrooms=&minimum_price=&maximum_price=&lat=&lng=",
	},
];

async function scrapeHumberts() {
	console.log(`\n🚀 Starting Humberts scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(1500);

			// Wait for property list items
			await page
				.waitForSelector("li.type-property", { timeout: 15000 })
				.catch(() => console.log(`⚠️ No property items found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll("li.type-property"));

					return items
						.map((el) => {
							try {
								// Get the detail link from h3 anchor
								const titleAnchor = el.querySelector("h3 a");
								const link = titleAnchor ? titleAnchor.href : null;

								const title = titleAnchor ? titleAnchor.textContent.trim() : "";

								// Extract price from .price div
								const rawPrice = el.querySelector(".price")?.textContent?.trim() || "";
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}

								// Extract bedrooms, bathrooms, receptions from .room divs
								const rooms = Array.from(el.querySelectorAll(".room-count")).map((s) =>
									s.textContent.trim(),
								);
								const bedrooms = rooms[0] || null;
								const bathrooms = rooms[1] || null;
								const receptions = rooms[2] || null;

								if (!link) return null;

								return { link, title, price, bedrooms, bathrooms, receptions };
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

							// Get HTML content for coordinate extraction
							const htmlContent = await detailPage.content();

							try {
								const priceNum = property.price
									? parseFloat(property.price.replace(/[^0-9.]/g, ""))
									: null;

								if (priceNum === null) {
									log.warn(`No price found: ${property.title}`);
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
									await processPropertyWithCoordinates(
										property.link,
										priceNum,
										property.title,
										property.bedrooms || null,
										AGENT_ID,
										isRental,
										htmlContent,
									);
								}

								const priceDisplay = formatPriceUk(priceNum);
								console.log(`✅ ${property.title} - ${priceDisplay}`);
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
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = propertyType.urlBase.includes("/page/")
				? `${propertyType.urlBase}${pg}/${propertyType.suffix}`
				: `${propertyType.urlBase}${propertyType.suffix}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed Humberts - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

(async () => {
	try {
		await scrapeHumberts();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
