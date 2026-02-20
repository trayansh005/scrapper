// Taylforths scraper using Playwright with Crawlee
// Agent ID: 225
// Website: taylforths.co.uk
// Usage:
// node backend/scraper-agent-225.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 225;
const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.taylforths.co.uk/find-a-property/page/",
		params:
			"/?address_keyword&radius=20&minimum_bedrooms&maximum_rent&maximum_price&department=residential-sales",
		totalPages: 5,
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.discoverpm.co.uk/find-a-property/page/",
		params: "",
		totalPages: 5,
		isRental: true,
		label: "RENTALS",
	},
];

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			log.error(`Failed listing page: ${request.url}`);
		},
	});
}

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;

	console.log(` ${label} - Page ${pageNum} - ${request.url}`);

	try {
		await page.waitForTimeout(2000);
		await page.waitForSelector("li.type-property", { timeout: 20000 }).catch(() => {
			console.log(` No listing container found on page ${pageNum}`);
		});

		// Extract properties
		const properties = await page.evaluate(() => {
			try {
				const items = Array.from(document.querySelectorAll("li.type-property"));
				return items
					.map((el) => {
						const linkEl = el.querySelector("h3 a");
						const link = linkEl ? linkEl.href : null;
						const title = linkEl ? linkEl.innerText.trim() : "";
						const rawPrice = el.querySelector("div.price")?.innerText.trim() || "";
						const bedrooms =
							el.querySelector(".room-bedrooms .room-count")?.innerText.trim() || null;
						const statusText = `${title} ${rawPrice} ${el.innerText || ""}`.trim();
						return { link, title, rawPrice, bedrooms, statusText };
					})
					.filter((p) => p.link);
			} catch (err) {
				return [];
			}
		});

		console.log(` Found ${properties.length} properties on page ${pageNum}`);
		stats.totalScraped += properties.length;

		const batchSize = 2;
		for (let i = 0; i < properties.length; i += batchSize) {
			const batch = properties.slice(i, i + batchSize);

			await Promise.all(
				batch.map(async (property) => {
					if (!property.link) return;
					if (isSoldProperty(property.statusText)) return;

					const priceNum = property.rawPrice
						? parseFloat(property.rawPrice.replace(/[^0-9.]/g, ""))
						: null;
					if (priceNum === null) return;

					const updateResult = await updatePriceByPropertyURLOptimized(
						property.link.trim(),
						priceNum,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
					);

					let persisted = !!updateResult.updated;

					if (!updateResult.isExisting) {
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 40000,
							});
							const htmlContent = await detailPage.content();
							await processPropertyWithCoordinates(
								property.link.trim(),
								formatPriceUk(priceNum),
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								htmlContent,
							);
							persisted = true;
						} catch (err) {
						} finally {
							await detailPage.close();
						}
					}

					if (persisted) {
						stats.totalSaved++;
						if (isRental) stats.savedRentals++;
						else stats.savedSales++;
					}

					console.log(` ${property.title} - ${formatPriceUk(priceNum)}`);
				}),
			);
			await page.waitForTimeout(500);
		}
	} catch (error) {
		console.error(` Error in ${label} page ${pageNum}: ${error.message}`);
	}
}

async function scrapeTaylforths() {
	console.log(`\n Starting Taylforths scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`\n Processing ${propertyType.label} (${propertyType.totalPages} pages)`);
		const crawler = createCrawler(browserWSEndpoint);
		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			requests.push({
				url: `${propertyType.baseUrl}${pg}${propertyType.params}`,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}
		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(`\n Scraping complete!`);
	console.log(`Total scraped: ${stats.totalScraped}`);
	console.log(`Total saved: ${stats.totalSaved}`);
}

(async () => {
	try {
		await scrapeTaylforths();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
