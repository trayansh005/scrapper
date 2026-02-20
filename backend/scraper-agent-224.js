// Mistoria scraper using Playwright with Crawlee
// Agent ID: 224
// Website: mistoriaestateagents.co.uk
// Usage:
// node backend/scraper-agent-224.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 224;
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
		baseUrl: "https://mistoriaestateagents.co.uk/property-search/page/",
		params:
			"/?address_keyword&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&department=residential-sales&availability&maximum_bedrooms",
		totalPages: 10,
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://mistoriaestateagents.co.uk/property-search/page/",
		params:
			"/?address_keyword=&department=residential-lettings&availability=&minimum_bedrooms=&maximum_bedrooms=",
		totalPages: 15,
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
		const properties = await page.evaluate((isRental) => {
			try {
				const cards = Array.from(document.querySelectorAll("li.type-property"));
				return cards
					.map((card) => {
						const statusText = card.innerText || "";
						// Simple check for sold/let within evaluation
						if (
							statusText.toLowerCase().includes("sold") ||
							statusText.toLowerCase().includes("let stc") ||
							statusText.toLowerCase().includes("let agreed")
						) {
							// We'll filter properly outside using the library helper if needed,
							// but evaluating here saves roundtrips.
						}

						const linkEl = card.querySelector("h3 a");
						const link = linkEl ? linkEl.href : null;
						const title = linkEl ? linkEl.innerText.trim() : "";

						const priceEl = card.querySelector("div.price");
						let priceRaw = priceEl ? priceEl.innerText.trim() : "";

						// Remove tenancy info if present
						const tenancyInfo = card.querySelector("span.lettings-fees");
						if (tenancyInfo) {
							priceRaw = priceRaw.replace(tenancyInfo.innerText, "").trim();
						}

						const bedEl = card.querySelector(".room-bedrooms");
						const bedroomsMatch = bedEl ? bedEl.innerText.match(/\d+/) : null;
						const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

						return { link, title, priceRaw, bedrooms };
					})
					.filter((p) => p.link);
			} catch (e) {
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

					// Filter sold properties
					if (isSoldProperty(property.title) || isSoldProperty(property.priceRaw)) {
						return;
					}

					const priceNum = property.priceRaw
						? parseFloat(property.priceRaw.replace(/[^0-9.]/g, ""))
						: null;
					if (priceNum === null) return;

					let coords = { latitude: null, longitude: null };

					const detailPage = await page.context().newPage();
					try {
						await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 40000 });
						await detailPage.waitForTimeout(1000);

						const detailCoords = await detailPage.evaluate(() => {
							let lat = null,
								lng = null;
							const scripts = Array.from(
								document.querySelectorAll('script[type="application/ld+json"]'),
							);
							for (const script of scripts) {
								try {
									const json = JSON.parse(script.innerText);
									const items = json["@graph"] || (Array.isArray(json) ? json : [json]);
									for (const item of items) {
										if (item.geo && item.geo.latitude != null) {
											lat = item.geo.latitude;
											lng = item.geo.longitude;
											break;
										}
									}
								} catch (e) {}
								if (lat) break;
							}
							if (!lat) {
								const allScripts = Array.from(document.querySelectorAll("script"));
								for (const script of allScripts) {
									const content = script.innerText;
									const gmapsMatch = content.match(
										/new\s+google\.maps\.LatLng\(\s*([\d.-]+)\s*,\s*([\d.-]+)\s*\)/i,
									);
									if (gmapsMatch) {
										lat = gmapsMatch[1];
										lng = gmapsMatch[2];
										break;
									}
								}
							}
							return { lat, lng };
						});

						if (detailCoords.lat) {
							coords.latitude = detailCoords.lat;
							coords.longitude = detailCoords.lng;
						}
					} catch (err) {
					} finally {
						await detailPage.close();
					}

					try {
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
							await updatePriceByPropertyURL(
								property.link.trim(),
								formatPriceUk(priceNum),
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								coords.latitude,
								coords.longitude,
							);
							persisted = true;
						}

						if (persisted) {
							stats.totalSaved++;
							if (isRental) stats.savedRentals++;
							else stats.savedSales++;
						}

						console.log(` ${property.title} - ${formatPriceUk(priceNum)}`);
					} catch (dbErr) {
						console.error(` DB error: ${dbErr.message}`);
					}
				}),
			);
			await page.waitForTimeout(500);
		}
	} catch (error) {
		console.error(` Error in ${label} page ${pageNum}: ${error.message}`);
	}
}

async function scrapeMistoria() {
	console.log(`\n Starting Mistoria scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

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
	console.log(` Breakdown - SALES: ${stats.savedSales}, RENTALS: ${stats.savedRentals}\n`);
}

(async () => {
	try {
		await scrapeMistoria();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
