// Rook Matthews Sayer scraper using Playwright with Crawlee
// Agent ID: 220
// Website: rookmatthewssayer.co.uk
// Usage:
// node backend/scraper-agent-220.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const {
	extractCoordinatesFromHTML,
	isSoldProperty,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 220;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const recentPageSignatures = new Map();
const processedUrls = new Set();

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.rookmatthewssayer.co.uk/for-sale",
		totalPages: 123, // 1098 properties / 9 per page = 122 pages
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.rookmatthewssayer.co.uk/for-rent",
		totalPages: 17, // 151 properties / 9 per page = 17 pages
		isRental: true,
		label: "RENTALS",
	},
];

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;

	console.log(` ${label} - Page ${pageNum} - ${request.url}`);

	try {
		// Wait for page content to populate
		await page.waitForTimeout(1500);
		await page.waitForSelector(".properties-grid-col", { timeout: 20000 }).catch(() => {
			console.log(` No listing container found on page ${pageNum}`);
		});

		// Extract properties from the DOM
		const properties = await page.evaluate(() => {
			try {
				const cards = Array.from(
					document.querySelectorAll(".col-lg-4.col-md-12.col-sm-12.properties-grid-col"),
				);
				return cards
					.map((card) => {
						// Check for status labels (Sold, Sold STC, Let, Let STC)
						const statusLabel = card.querySelector(
							".listing-custom-label-sold, .listing-custom-label-soldstc, .listing-custom-label-let, .listing-custom-label-letstc",
						);
						if (statusLabel) {
							return null; // Skip sold/let properties
						}

						const linkEl = card.querySelector("a.rwsp-grid-link");
						const href = linkEl ? linkEl.getAttribute("href") : null;
						const link = href
							? href.startsWith("http")
								? href
								: "https://www.rookmatthewssayer.co.uk" + href
							: null;
						const titleEl = card.querySelector("h2.property-title");
						const title = titleEl ? titleEl.textContent.trim() : "";
						const priceEl = card.querySelector("span.item-price");
						const price = priceEl ? priceEl.textContent.trim() : "";

						// Extract bedrooms, living rooms, and bathrooms from detail-icons
						const detailIcons = Array.from(card.querySelectorAll(".detail-icons ul li"));
						let bedrooms = null;
						let reception = null;
						let bathrooms = null;

						if (detailIcons.length >= 1) {
							const text = detailIcons[0].textContent.trim();
							bedrooms = text.split(/\s+/).pop();
						}
						if (detailIcons.length >= 2) {
							const text = detailIcons[1].textContent.trim();
							reception = text.split(/\s+/).pop();
						}
						if (detailIcons.length >= 3) {
							const text = detailIcons[2].textContent.trim();
							bathrooms = text.split(/\s+/).pop();
						}

						return {
							link,
							title,
							price,
							bedrooms,
							reception,
							bathrooms,
							statusText: card.innerText || "",
						};
					})
					.filter((p) => p); // Remove null entries
			} catch (e) {
				console.log("Error extracting properties:", e);
				return [];
			}
		});
		console.log(` Found ${properties.length} properties on page ${pageNum}`);
		stats.totalScraped += properties.length;

		// Process properties in small batches
		const batchSize = 2;
		for (let i = 0; i < properties.length; i += batchSize) {
			const batch = properties.slice(i, i + batchSize);

			await Promise.all(
				batch.map(async (property) => {
					// Ensure absolute URL
					if (!property.link) return;

					if (isSoldProperty(property.statusText || "")) {
						return;
					}
					let coords = { latitude: null, longitude: null };

					// Visit detail page to extract coordinates from comments
					const detailPage = await page.context().newPage();
					try {
						await detailPage.goto(property.link, {
							waitUntil: "domcontentloaded",
							timeout: 30000,
						});
						await detailPage.waitForTimeout(500);

						const html = await detailPage.content();
						const detailCoords = extractCoordinatesFromHTML(html);

						if (detailCoords) {
							coords.latitude = detailCoords.latitude;
							coords.longitude = detailCoords.longitude;
						}

					} catch (err) {
						// ignore detail page errors
					} finally {
						await detailPage.close();
					}

					try {
						// Format price: extract only digits
						const priceClean = property.price
							? property.price.replace(/[^0-9.]/g, "")
							: null;

						const priceNum = priceClean ? parseFloat(priceClean) : null;

						if (!priceNum) {
							console.log(` No price found: ${property.title}`);
							return;
						}

						const formattedPrice = formatPriceUk(priceNum);
						const dbPrice = Number(priceNum).toLocaleString("en-GB");

						const updateResult = await updatePriceByPropertyURLOptimized(
							property.link.trim(),
							dbPrice,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
						);

						let persisted = !!updateResult.updated;

						if (!updateResult.isExisting) {
							await updatePriceByPropertyURL(
								property.link.trim(),
								dbPrice,
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
							` ${property.title} - ${dbPrice} - ${coords.latitude && coords.longitude
								? `${coords.latitude}, ${coords.longitude}`
								: "No coords"
							}`,
						);
					} catch (dbErr) {
						console.error(` DB error for ${property.link}: ${dbErr.message}`);
					}
				}),
			);

			// Small delay between batches
			await page.waitForTimeout(500);
		}
	} catch (error) {
		console.error(` Error in ${label} page ${pageNum}: ${error.message}`);
	}
}

async function scrapeRookMatthewsSayer() {
	console.log(`\n Starting Rook Matthews Sayer scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`\n Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const crawler = createCrawler(browserWSEndpoint);
		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url =
				pg === 1
					? `${propertyType.urlBase}/?sortby=d_date`
					: `${propertyType.urlBase}/page/${pg}/?sortby=d_date`;
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
		await scrapeRookMatthewsSayer();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
