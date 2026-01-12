// Entwistle Green scraper using Playwright with Crawlee
// Agent ID: 219
// Website: entwistlegreen.co.uk
// Usage:
// node backend/scraper-agent-219.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 219;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.entwistlegreen.co.uk/properties/sales/status-available/most-recent-first",
	// 	totalPages: 141, // 1403 properties / 10 per page = 141 pages
	// 	recordsPerPage: 10,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlBase:
			"https://www.entwistlegreen.co.uk/properties/lettings/status-available/most-recent-first",
		totalPages: 18, // 173 properties / 10 per page = 18 pages
		recordsPerPage: 10,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeEntwistleGreen() {
	console.log(`\n🚀 Starting Entwistle Green scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 600,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Wait for page content to populate
			await page.waitForTimeout(1500);
			await page.waitForSelector(".results-page", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			// Extract properties from the DOM
			const properties = await page.evaluate(() => {
				try {
					const container = document.querySelector(".results-page");
					if (!container) return [];
					const cards = Array.from(container.querySelectorAll(".card"));
					return cards
						.map((card) => {
							const linkEl = card.querySelector("a.card__link");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.entwistlegreen.co.uk" + href
								: null;
							const priceEl = card.querySelector("a.card__link span");
							const price = priceEl ? priceEl.textContent.trim() : "";
							const titleEl = card.querySelector("p.card__text-content");
							const title = titleEl ? titleEl.textContent.trim() : "";
							const bedrooms =
								card
									.querySelector(
										".card-content__spec-list li:nth-child(1) .card-content__spec-list-number"
									)
									?.textContent.trim() || null;
							const bathrooms =
								card
									.querySelector(
										".card-content__spec-list li:nth-child(2) .card-content__spec-list-number"
									)
									?.textContent.trim() || null;
							const reception =
								card
									.querySelector(
										".card-content__spec-list li:nth-child(3) .card-content__spec-list-number"
									)
									?.textContent.trim() || null;
							return { link, price, title, bedrooms, bathrooms, reception };
						})
						.filter((p) => p.link);
				} catch (e) {
					console.log("Error extracting properties:", e);
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process properties one by one with delays to avoid rate limiting
			for (const property of properties) {
				// Ensure absolute URL
				if (!property.link) return;

				let coords = { latitude: null, longitude: null };

				// Visit detail page to extract coordinates from comments
				if (!coords.latitude || !coords.longitude) {
					const detailPage = await page.context().newPage();
					try {
						await detailPage.goto(property.link, {
							waitUntil: "domcontentloaded",
							timeout: 30000,
						});
						await detailPage.waitForTimeout(500);

						const detailCoords = await detailPage.evaluate(() => {
							try {
								const html = document.documentElement.outerHTML;
								const latMatch = html.match(/<!--property-latitude:"([^"]+)"-->/);
								const lngMatch = html.match(/<!--property-longitude:"([^"]+)"-->/);
								if (latMatch && lngMatch) {
									return { lat: parseFloat(latMatch[1]), lng: parseFloat(lngMatch[1]) };
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
				}

				try {
					// Format price: extract only digits
					const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;

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

					console.log(
						`✅ ${property.title} - ${formatPrice(priceClean)} - ${
							coords.latitude && coords.longitude
								? `${coords.latitude}, ${coords.longitude}`
								: "No coords"
						}`
					);
				} catch (dbErr) {
					console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
				}

				// Delay between properties to avoid rate limiting
				await new Promise((resolve) => setTimeout(resolve, 2000));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue all listing pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			// Construct page URL
			const url = pg === 1 ? `${propertyType.urlBase}#/` : `${propertyType.urlBase}/page-${pg}#/`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Entwistle Green - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeEntwistleGreen();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
