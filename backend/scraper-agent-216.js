// CJ Hole scraper using Playwright with Crawlee
// Agent ID: 216
// Usage:
// node backend/scraper-agent-216.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { updatePriceByPropertyURLOptimized, } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty, } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 216;
const logger = createAgentLogger(AGENT_ID);

let totalScraped = 0;
let totalSaved = 0;

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.cjhole.co.uk/search-results/for-sale/in-united-kingdom",
		totalPages: Math.ceil(592 / 9),
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.cjhole.co.uk/search-results/for-letting/in-united-kingdom",
		totalPages: Math.ceil(123 / 9),
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeCJHole() {
	console.log(`\n🚀 Starting CJ Hole scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 2,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 120,

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

			logger.page(pageNum, label, request.url);

			await page.waitForSelector(".property--card__results", {
				timeout: 20000,
			}).catch(() => null);

			const properties = await page.evaluate(() => {
				const items = Array.from(
					document.querySelectorAll(".property--card__results")
				);

				return items
					.map((el) => {
						const linkEl = el.querySelector(
							"a.property--card__image-wrapper"
						);
						const href = linkEl?.href || null;

						const title =
							el.querySelector(".property-title a")?.textContent?.trim() ||
							"";

						const price =
							el.querySelector(".property-price")?.textContent?.trim() ||
							"";

						let bedrooms = null;
						const typeText =
							el.querySelector(".property-type")?.textContent || "";
						const bedMatch = typeText.match(/(\d+)\s*bedroom/i);
						if (bedMatch) bedrooms = bedMatch[1];

						return {
							link: href,
							title,
							price,
							bedrooms,
						};
					})
					.filter((p) => p.link);
			});

			totalScraped += properties.length;

			for (const property of properties) {
				try {
					// Skip sold properties
					if (isSoldProperty(property.price)) continue;

					// Clean numeric price
					const priceClean = property.price
						? property.price.replace(/[^0-9]/g, "")
						: null;

					if (!priceClean) continue;

					const formattedPrice = Number(priceClean).toLocaleString("en-GB");

					// Open detail page ONLY for coordinates
					const detailPage = await page.context().newPage();
					await blockNonEssentialResources(detailPage);

					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});

					const html = await detailPage.content();
					await detailPage.close();

					let coords = extractCoordinatesFromHTML(html);

					// Fallback for CJ Hole custom script format
					if (!coords || !coords.latitude || !coords.longitude) {
						const geoMatch = html.match(
							/"GeoCoordinates","latitude":([0-9.-]+),"longitude":([0-9.-]+)/
						);

						if (geoMatch) {
							coords = {
								latitude: parseFloat(geoMatch[1]),
								longitude: parseFloat(geoMatch[2]),
							};
						}
					}

					await updatePriceByPropertyURLOptimized(
						property.link.trim(),
						formattedPrice,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
						coords?.latitude || null,
						coords?.longitude || null
					);

					totalSaved++;

					console.log(
						`✅ [${isRental ? "RENTALS" : "SALES"}]`,
						"\n Title:      ", property.title,
						"\n PriceText:  ", formattedPrice,
						"\n Bedrooms:   ", property.bedrooms,
						"\n Latitude:   ", coords?.latitude,
						"\n Longitude:  ", coords?.longitude,
						"\n Link:       ", property.link,
						"\n------------------------------------------------"
					);
				} catch (err) {
					logger.error(property.link, err.message);
				}
			}
		},

		failedRequestHandler({ request }) {
			logger.failed(request.url);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(
			`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`
		);

		const requests = [];

		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			requests.push({
				url: `${propertyType.urlBase}/page-${pg}/?orderby=price_desc&radius=0.1`,
				userData: {
					pageNum: pg,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed CJ Hole - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeCJHole();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();