// Palmer Partners scraper using Playwright with Crawlee
// Agent ID: 226
// Website: palmerpartners.com
// Usage:
// node backend/scraper-agent-226.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 226;
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
		urlBase: "https://www.palmerpartners.com/buy/property-for-sale/",
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.palmerpartners.com/let/property-to-let/",
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

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNum = 1 } = request.userData;

	console.log(` ${label} - Page ${pageNum} - ${request.url}`);

	try {
		await page.waitForTimeout(2000);
		await page
			.waitForSelector('.property, .property-card, a[href*="/property/"]', { timeout: 20000 })
			.catch(() => {
				console.log(` No listing container found on page ${pageNum}`);
			});

		// Extract properties
		const properties = await page.evaluate((isRental) => {
			try {
				const items = Array.from(
					document.querySelectorAll(".property-card, .search-items li, .property, .row.property"),
				);
				return items
					.map((el) => {
						const linkTag = el.querySelector('a[href*="/property/"]');
						const priceText =
							el.querySelector(".price, .property-price, .price-display, .list-price")?.innerText ||
							"";
						const title =
							el.querySelector(".address, .property-address, .address-display, .list-address")
								?.innerText || "";
						const status =
							el.querySelector(".property-status, .status, .label")?.innerText?.trim() || "";
						return { link: linkTag?.href, priceText, title, status };
					})
					.filter((p) => p.link);
			} catch (err) {
				return [];
			}
		}, isRental);

		console.log(` Found ${properties.length} properties on page ${pageNum}`);
		stats.totalScraped += properties.length;

		const batchSize = 2;
		for (let i = 0; i < properties.length; i += batchSize) {
			const batch = properties.slice(i, i + batchSize);

			await Promise.all(
				batch.map(async (property) => {
					if (!property.link) return;

					const statusLower = property.status.toLowerCase();
					if (isRental) {
						if (statusLower.includes("let agreed") || statusLower.includes("let stc")) return;
					} else {
						if (
							statusLower.includes("sold") ||
							statusLower.includes("under offer") ||
							statusLower.includes("sold stc")
						)
							return;
					}

					const priceNum = property.priceText
						? parseFloat(property.priceText.replace(/[^0-9.]/g, ""))
						: null;
					if (priceNum === null) return;

					const updateResult = await updatePriceByPropertyURLOptimized(
						property.link.trim(),
						priceNum,
						property.title,
						null,
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
							const propData = await detailPage.evaluate(() => {
								const data = { lat: null, lng: null, bedrooms: null };
								const bedsInput = document.querySelector('input[name="beds"]');
								if (bedsInput) data.bedrooms = bedsInput.value;
								const allHiddenInputs = Array.from(
									document.querySelectorAll('input[type="hidden"]'),
								);
								for (const input of allHiddenInputs) {
									const val = input.value?.trim() || "";
									if (val.startsWith("[") && val.includes('"lat"') && val.includes('"lng"')) {
										try {
											const coords = JSON.parse(val);
											if (coords?.[0]) {
												data.lat = coords[0].lat;
												data.lng = coords[0].lng;
												if (!data.bedrooms && coords[0].beds) data.bedrooms = coords[0].beds;
											}
										} catch (e) {}
									}
								}
								return data;
							});

							await processPropertyWithCoordinates(
								property.link.trim(),
								formatPriceUk(priceNum),
								property.title,
								propData.bedrooms,
								AGENT_ID,
								isRental,
								htmlContent,
								propData.lat,
								propData.lng,
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

		// Handle pagination
		if (pageNum === 1) {
			const maxPage = await page.evaluate(() => {
				const paginationLinks = Array.from(document.querySelectorAll(".pagination a"));
				let highest = 1;
				paginationLinks.forEach((a) => {
					const val = parseInt(a.textContent.trim());
					if (!isNaN(val) && val > highest) highest = val;
				});
				return highest;
			});

			if (maxPage > 1) {
				for (let p = 2; p <= maxPage; p++) {
					const pageUrl = `${request.url}${request.url.includes("?") ? "&" : "?"}page=${p}`;
					await crawler.addRequests([
						{
							url: pageUrl,
							userData: { isRental, label, pageNum: p },
						},
					]);
				}
			}
		}
	} catch (error) {
		console.error(` Error in ${label} page ${pageNum}: ${error.message}`);
	}
}

async function scrapePalmerPartners() {
	console.log(`\n Starting Palmer Partners scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`\n Processing ${propertyType.label}`);
		const crawler = createCrawler(browserWSEndpoint);
		await crawler.addRequests([
			{
				url: propertyType.urlBase,
				userData: { isRental: propertyType.isRental, label: propertyType.label, pageNum: 1 },
			},
		]);
		await crawler.run();
	}

	console.log(`\n Scraping complete!`);
	console.log(`Total scraped: ${stats.totalScraped}`);
	console.log(`Total saved: ${stats.totalSaved}`);
}

(async () => {
	try {
		await scrapePalmerPartners();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
