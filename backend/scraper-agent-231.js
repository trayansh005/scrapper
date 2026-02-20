// Map Estate Agents scraper using Playwright with Crawlee
// Agent ID: 231
// Usage:
// node backend/scraper-agent-231.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 231;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// Two searches: sales and lettings
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.mapestateagents.com/property-sales/properties-for-sale?start=",
		totalRecords: 20 * 12, // approx placeholder (20 pages x 12 per page)
		recordsPerPage: 12,
		totalPages: 20,
		isRental: false,
		label: "FOR SALE",
	},
	{
		urlBase: "https://www.mapestateagents.com/property-lettings/properties-to-let?start=",
		totalRecords: 1 * 12,
		recordsPerPage: 12,
		totalPages: 1,
		isRental: true,
		label: "TO LET",
	},
];

async function scrapeMapEstateAgents() {
	console.log(`\n🚀 Starting Map Estate Agents scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for listing cards
			await page
				.waitForSelector(
					".span4.eapow-row0.eapow-overview-row, .span4.eapow-row1.eapow-overview-row",
					{ timeout: 15000 },
				)
				.catch(() => console.log(`⚠️ No listing container found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(
							".span4.eapow-row0.eapow-overview-row, .span4.eapow-row1.eapow-overview-row",
						),
					);

					return items
						.map((el) => {
							try {
								// Check for sold / sold stc banners
								const soldBannerImg = el.querySelector(
									'img[src*="banner_sold"], img[src*="banner_soldstc"], img[alt*="Sold"]',
								);
								if (soldBannerImg) return null;

								const thumbAnchor =
									el.querySelector(".eapow-property-thumb-holder a") || el.querySelector("a");
								const relativeLink = thumbAnchor ? thumbAnchor.getAttribute("href") : null;
								const link = relativeLink
									? new URL(relativeLink, "https://www.mapestateagents.com").href
									: null;

								const title =
									el.querySelector(".eapow-overview-title h3")?.textContent?.trim() || "";
								const rawPrice =
									el.querySelector(".eapow-overview-price.propPrice")?.textContent?.trim() || "";
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = m[0].replace(/,/g, "");
								}

								// icons show numbers in .IconNum spans; extract first three if present
								const iconNums = Array.from(el.querySelectorAll(".IconNum")).map((s) =>
									s.textContent.trim(),
								);
								const bedrooms = iconNums[0] || null;
								const bathrooms = iconNums[1] || null;
								const receptions = iconNums[2] || null;

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

								// Check if property already exists
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
									// For new properties, insert with coordinates
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
									// For existing properties, update with coordinates if found
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
		for (let pg = 0; pg < propertyType.totalPages; pg++) {
			const start = pg * propertyType.recordsPerPage;
			const url = `${propertyType.urlBase}${start}`;
			requests.push({
				url,
				userData: { pageNum: pg + 1, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n✅ Completed MapEstateAgents - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

(async () => {
	try {
		await scrapeMapEstateAgents();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
