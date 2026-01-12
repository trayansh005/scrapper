// CJ Hole scraper using Playwright with Crawlee
// Agent ID: 216
// Usage:
// node backend/scraper-agent-216.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 216;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

// Configuration for CJ Hole
// 9 properties per page
const PROPERTY_TYPES = [
	// {
	// 	// Sales
	// 	urlBase: "https://www.cjhole.co.uk/search-results/for-sale/in-united-kingdom",
	// 	totalPages: Math.ceil(592 / 9),
	// 	recordsPerPage: 9,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		// Rentals
		urlBase: "https://www.cjhole.co.uk/search-results/for-letting/in-united-kingdom",
		totalPages: Math.ceil(123 / 9),
		recordsPerPage: 9,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeCJHole() {
	console.log(`\n🚀 Starting CJ Hole scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(800);

			await page
				.waitForSelector(".property--card__results", { timeout: 20000 })
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll(".property--card__results"));
					return items
						.map((el) => {
							const linkEl = el.querySelector("a.property--card__image-wrapper");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href ? href : null;

							const titleEl = el.querySelector(".property-title a");
							const title = titleEl ? titleEl.textContent.trim() : "";

							const priceEl = el.querySelector(".property-price");
							const price = priceEl ? priceEl.textContent.trim() : "";

							let bedrooms = null;
							const typeEl = el.querySelector(".property-type");
							if (typeEl) {
								const typeText = typeEl.textContent.trim();
								const bedMatch = typeText.match(/(\d+)\s*bedroom/i);
								if (bedMatch) bedrooms = bedMatch[1];
							}

							return { link, price, title, bedrooms, lat: null, lng: null };
						})
						.filter((p) => p.link);
				} catch (e) {
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

						let coords = { latitude: property.lat || null, longitude: property.lng || null };

						if (!coords.latitude || !coords.longitude) {
							const detailPage = await page.context().newPage();
							try {
								await detailPage.goto(property.link, {
									waitUntil: "domcontentloaded",
									timeout: 30000,
								});
								await detailPage.waitForTimeout(400);

								const detailCoords = await detailPage.evaluate(() => {
									try {
										const scripts = Array.from(document.querySelectorAll("script"))
											.map((s) => s.textContent)
											.join("\n");
										// Look for "GeoCoordinates","latitude":...,"longitude":...
										const geoMatch = scripts.match(
											/"GeoCoordinates","latitude":([0-9.-]+),"longitude":([0-9.-]+)/
										);
										if (geoMatch) {
											return { lat: parseFloat(geoMatch[1]), lng: parseFloat(geoMatch[2]) };
										}
										return null;
									} catch (e) {
										return null;
									}
								});

								if (detailCoords) {
									let lat = detailCoords.lat;
									let lng = detailCoords.lng;
									// Heuristic for inverted coordinates (UK region)
									if (
										Math.abs(lat) <= 10 &&
										lng >= 49 &&
										lng <= 61 &&
										!(lat >= 49 && lat <= 61 && Math.abs(lng) <= 10)
									) {
										const t = lat;
										lat = lng;
										lng = t;
									}
									coords.latitude = lat;
									coords.longitude = lng;
								}
							} catch (err) {
								// ignore detail page errors
							} finally {
								await detailPage.close();
							}
						}

						try {
							const rawPrice = (property.price || "").toString();
							const numMatch = rawPrice.match(/[0-9][0-9,\.\s]*/);
							const priceClean = numMatch ? numMatch[0].replace(/[^0-9]/g, "") : "";

							await updatePriceByPropertyURL(
								property.link,
								priceClean,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								coords.latitude,
								coords.longitude
							);

							totalSaved++;
							totalScraped++;

							const coordsStr =
								coords.latitude && coords.longitude
									? `${coords.latitude}, ${coords.longitude}`
									: "No coords";
							console.log(`✅ ${property.title} - ${formatPrice(priceClean)} - ${coordsStr}`);
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
						}
					})
				);

				await new Promise((resolve) => setTimeout(resolve, 300));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}/page-${pg}/?orderby=price_desc&radius=0.1`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
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
