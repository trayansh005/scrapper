// Homesea scraper using Playwright with Crawlee
// Agent ID: 217
// Usage:
// node backend/scraper-agent-217.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 217;
const logger = createAgentLogger(AGENT_ID);
let totalScraped = 0;
let totalSaved = 0;


// Homesea: 167 properties, 9 per page => 19 pages
const PROPERTY_TYPES = [
	{
		urlBase: "https://homesea.co.uk/property-search/page", // page number will be appended like /{page}/?department=residential-sales&...
		totalRecords: 167,
		totalPages: 19,
		recordsPerPage: 9,
		isRental: false,
		label: "SALES",
	},
];

async function scrapeHomesea() {
	console.log(`\n🚀 Starting Homesea scraper (Agent ${AGENT_ID})...\n`);

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
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(2000);

			// Wait for property list items
			await page.waitForSelector("li.type-property, li.post-", { timeout: 15000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll("li.type-property"));

					return items
						.map((li) => {
							try {
								const anchor = li.querySelector("a[href]");
								const link = anchor ? anchor.href : null;

								const title = li.querySelector("h2 a")?.textContent?.trim() || "";

								const price = li.querySelector(".price")?.textContent?.trim() || "";

								// bedrooms: look for 'Bedrooms:' text in rooms list
								let bedrooms = null;
								const rooms = li.querySelectorAll("ul.rooms li");
								for (const r of rooms) {
									const txt = r.textContent || "";
									if (txt.toLowerCase().includes("bedrooms")) {
										const m = txt.match(/(\d+)/);
										if (m) bedrooms = m[1];
										break;
									}
								}

								if (link) return { link, title, price, bedrooms };
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

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			for (const property of properties) {
				if (!property.link) continue;
				// open detail page to extract JSON-LD
				let coords = { latitude: null, longitude: null };

				try {
					const detailPage = await page.context().newPage();
					await blockNonEssentialResources(detailPage);

					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});

					const html = await detailPage.content();
					coords = extractCoordinatesFromHTML(html);

					await detailPage.close();
				} catch (err) {
					// ignore detail errors
				} finally {
					await detailPage.close();
				}

				try {
					const priceClean = property.price
						? property.price.replace(/[^0-9.]/g, "")
						: null;

					const priceNum = priceClean ? parseFloat(priceClean) : null;

					if (!priceNum) {
						logger.info(`No valid price for ${property.title}`);
						continue;
					}

					const sold = isSoldProperty(property.price, property.title);
					const formattedPrice = formatPriceUk(priceNum);

					await updatePriceByPropertyURLOptimized(
						property.link.trim(),
						formattedPrice,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
						coords.latitude,
						coords.longitude,
						sold
					);

					totalSaved++;
					totalScraped++;

					// ✅ NEW CLEAN DEBUG CONSOLE
					console.log(
						`✅ [${isRental ? "RENTALS" : "SALES"}]`,
						"\n Title:      ", property.title,
						"\n PriceText:  ", formattedPrice,
						"\n Bedrooms:   ", property.bedrooms,
						"\n Latitude:   ", coords.latitude,
						"\n Longitude:  ", coords.longitude,
						"\n Link:       ", property.link,
						"\n------------------------------------------------"
					);

				} catch (dbErr) {
					logger.error(`DB error for ${property.link}: ${dbErr?.message || dbErr}`);
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(
			`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages, ${propertyType.recordsPerPage} per page)`
		);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}/${pg}/?department=residential-sales&address_keyword&radius&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&availability=2`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Homesea - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeHomesea();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
