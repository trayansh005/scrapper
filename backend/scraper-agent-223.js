// Galbraithgroup scraper using Playwright with Crawlee
// Agent ID: 223
// Website: galbraithgroup.com
// Usage:
// node backend/scraper-agent-223.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 223;
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
		urlBase:
			"https://www.galbraithgroup.com/sales-and-lettings/search/?sq.BuyOrLet=true&sq.MaxDistance=30&sq.sq_stc=true&sq.Sort=newest",
		totalPages: 10,
		recordsPerPage: 10,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase:
			"https://www.galbraithgroup.com/sales-and-lettings/search/?sq.BuyOrLet=false&sq.MaxDistance=30&sq.sq_stc=true&sq.Sort=newest",
		totalPages: 5,
		recordsPerPage: 10,
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
		await page.waitForTimeout(3000);

		// Wait for property cards to load
		await page.waitForSelector("div[class*='carousel']", { timeout: 30000 }).catch(() => {
			console.log(` No listing container found on page ${pageNum}`);
		});

		// Extract properties from the DOM
		const properties = await page.evaluate(() => {
			try {
				const containers = Array.from(document.querySelectorAll("div[class*='generic']")).filter(
					(el) => {
						return el.querySelector("img[alt*='Bedroom Count']") !== null;
					},
				);

				return containers
					.map((container) => {
						try {
							const titleEl = container.querySelector("h2 a, h3 a");
							const title = titleEl ? titleEl.textContent.trim() : "";
							const link = titleEl ? titleEl.getAttribute("href") : null;
							const fullLink = link
								? link.startsWith("http")
									? link
									: "https://www.galbraithgroup.com" + link
								: null;

							const priceTexts = Array.from(container.querySelectorAll("p")).map((p) =>
								p.textContent.trim(),
							);
							const priceEl = priceTexts.find((t) => t.includes("") || t.includes("Offers Over"));
							const price = priceEl || "";

							const bedroomImg = container.querySelector("img[alt*='Bedroom Count']");
							const bedroomEl =
								bedroomImg?.nextElementSibling || bedroomImg?.parentElement?.querySelector("p");
							const bedrooms = bedroomEl ? bedroomEl.textContent.trim() : null;

							if (!fullLink || !title) return null;

							return { link: fullLink, title, price, bedrooms };
						} catch (e) {
							return null;
						}
					})
					.filter((p) => p);
			} catch (e) {
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

					let coords = { latitude: null, longitude: null };

					const detailPage = await page.context().newPage();
					try {
						await detailPage.goto(property.link, {
							waitUntil: "domcontentloaded",
							timeout: 40000,
						});
						await detailPage.waitForTimeout(1000);

						const detailCoords = await detailPage.evaluate(() => {
							try {
								const scripts = Array.from(document.querySelectorAll("script"));
								for (const script of scripts) {
									const content = script.textContent;
									if (content.includes("GeoCoordinates")) {
										const geoMatch = content.match(/{\s*"@type"\s*:\s*"GeoCoordinates"[^}]*}/);
										if (geoMatch) {
											const geo = JSON.parse(geoMatch[0]);
											if (geo.latitude && geo.longitude) {
												return { lat: parseFloat(geo.latitude), lng: parseFloat(geo.longitude) };
											}
										}
									}
								}
								return null;
							} catch (e) {
								return null;
							}
						});

						if (detailCoords) {
							coords.latitude = detailCoords.lat;
							coords.longitude = detailCoords.lng;
						}
					} catch (err) {
						// ignore detail page errors
					} finally {
						await detailPage.close();
					}

					try {
						const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;
						const priceNum = priceClean ? parseFloat(priceClean) : null;

						if (priceNum === null) {
							console.log(` No price found: ${property.title}`);
							return;
						}

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

						console.log(
							` ${property.title} - ${formatPriceUk(priceNum)} - ${
								coords.latitude && coords.longitude
									? `${coords.latitude}, ${coords.longitude}`
									: "No coords"
							}`,
						);
					} catch (dbErr) {
						console.error(` DB error for ${property.link}: ${dbErr.message}`);
					}
				}),
			);

			await page.waitForTimeout(500);
		}
	} catch (error) {
		console.error(` Error in ${label} page ${pageNum}: ${error.message}`);
	}
}

async function scrapeGalbraithgroup() {
	console.log(`\n Starting Galbraithgroup scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`\n Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const crawler = createCrawler(browserWSEndpoint);
		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const pageSize = propertyType.recordsPerPage;
			const url =
				pg === 1
					? propertyType.urlBase
					: `${propertyType.urlBase}&sq.Page=${pg}&sq.PageSize=${pageSize}`;

			requests.push({
				url,
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
		await scrapeGalbraithgroup();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
