// ESPC scraper using Playwright with Crawlee
// Agent ID: 222
// Website: espc.com
// Usage:
// node backend/scraper-agent-222.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty, extractBedroomsFromHTML } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 222;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const recentPageSignatures = new Map();
const processedUrls = new Set();

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

const PROPERTY_TYPES = [
	{
		baseUrl: "https://espc.com/properties",
		isRental: false,
		label: "SALES",
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
	const { isRental, label, pageNumber } = request.userData;

	console.log(` [${label}] Page ${pageNumber || 1} - ${request.url}`);

	try {
		await page.waitForTimeout(700);
		await page.waitForSelector('a[href*="/property/"] h3', { timeout: 15000 }).catch(() => {
			console.log(` Listing container not found on page ${pageNumber || 1}`);
		});

		const result = await page.evaluate(() => {
			try {
				const items = Array.from(document.querySelectorAll('a[href*="/property/"]')).filter((a) =>
					a.querySelector("h3"),
				);

				const scraped = items.map((item) => {
					const titleEl = item.querySelector("h3.propertyTitle");
					const title = titleEl ? titleEl.innerText.trim() : "N/A";

					const priceEl = item.querySelector(".price");
					const price = priceEl ? priceEl.innerText.trim() : null;

					let bedrooms = null;
					const bedEl = Array.from(item.querySelectorAll(".facilities .opt")).find((opt) =>
						opt.querySelector(".icon.bed"),
					);
					if (bedEl) {
						const bedText = bedEl.innerText.trim();
						const match = bedText.match(/(\d+)/);
						if (match) bedrooms = parseInt(match[1]);
					}

					const statusText = item.innerText || "";
					return {
						link: item.href,
						title,
						priceRaw: price,
						bedrooms,
						statusText,
					};
				});

				const paginationNext = document.querySelector("a.next, a.nextPage, .paginationList a.next");

				return {
					properties: scraped,
					hasNextPage: !!paginationNext,
				};
			} catch (e) {
				return { properties: [], hasNextPage: false };
			}
		});

		const properties = result.properties;
		console.log(` Found ${properties.length} properties on page ${pageNumber || 1}`);

		const pageSignature = properties.map((p) => p.link).slice(0, 5).join("|");
		const signatureKey = isRental ? "RENTALS" : "SALES";
		const previousSignature = recentPageSignatures.get(signatureKey);
		if (pageSignature && previousSignature === pageSignature) {
			console.log(
				` Warning: ${signatureKey} page ${pageNumber || 1} has same leading links as previous page.`,
			);
		}
		recentPageSignatures.set(signatureKey, pageSignature);

		const batchSize = 2;
		for (let i = 0; i < properties.length; i += batchSize) {
			const batch = properties.slice(i, i + batchSize);
			for (const property of batch) {
				if (!property.link) continue;
				if (isSoldProperty(property.statusText || "")) continue;
				if (processedUrls.has(property.link)) continue;
				processedUrls.add(property.link);

				// Extract numeric price first
				const priceNum = property.priceRaw
					? parseFloat(property.priceRaw.replace(/[^0-9.]/g, ""))
					: null;

				if (priceNum === null) {
					console.log(` Skipping update (no price): ${property.link}`);
					continue;   // use continue, not return
				}

				// Create formatted version
				const formattedPrice = parseInt(priceNum, 10).toLocaleString("en-GB");

				const bedrooms = property.bedrooms || extractBedroomsFromHTML(property.title || "");
				let latitude = null;
				let longitude = null;

				const result = await updatePriceByPropertyURLOptimized(
					property.link.trim(),
					formattedPrice,
					property.title,
					bedrooms,
					AGENT_ID,
					isRental,
				);

				if (result.updated) stats.totalSaved++;

				console.log(
					`✅ [${isRental ? "RENTALS" : "SALES"}]`,
					"\n Title:      ", property.title,
					"\n PriceText:  ", formattedPrice,
					"\n Bedrooms:   ", bedrooms,
					"\n Latitude:   ", latitude,
					"\n Longitude:  ", longitude,
					"\n Link:       ", property.link,
					"\n------------------------------------------------"
				);


				if (!result.isExisting && !result.error) {

					const detailPage = await page.context().newPage();
					let html = null;

					try {
						await detailPage.goto(property.link, {
							waitUntil: "domcontentloaded",
							timeout: 40000,
						});

						html = await detailPage.content();

						if (html) {
							const coords = extractCoordinatesFromHTML(html);
							latitude = coords?.latitude || null;
							longitude = coords?.longitude || null;
						}

					} catch (err) {
						console.log("❌ Failed to load detail page:", property.link);
					} finally {
						await detailPage.close();
					}

					await processPropertyWithCoordinates(
						property.link.trim(),
						formattedPrice,
						property.title,
						bedrooms,
						AGENT_ID,
						isRental,
						html,
						latitude,
						longitude,
					);
				}

				const categoryLabel = isRental ? "RENTALS" : "SALES";
				console.log(
					` [${categoryLabel}] ${property.title.substring(0, 40)} - ${formatPriceUk(
						priceNum,
					)} - ${property.link}`,
				);
			}

			await page.waitForTimeout(500);
		}

		if (result.hasNextPage) {
			const nextPage = (pageNumber || 1) + 1;
			const nextUrl = `https://espc.com/properties?p=${nextPage}`;
			await crawler.addRequests([
				{
					url: nextUrl,
					userData: { isRental, label, pageNumber: nextPage },
				},
			]);
		}
	} catch (error) {
		console.error(` Error in handleListingPage: ${error.message}`);
	}
}



async function scrapeAll() {
	console.log(` Starting ESPC Scraper (Agent ${AGENT_ID})...`);
	const browserWSEndpoint = getBrowserlessEndpoint();

	for (const propertyType of PROPERTY_TYPES) {
		const crawler = createCrawler(browserWSEndpoint);
		await crawler.addRequests([
			{
				url: propertyType.baseUrl,
				userData: { isRental: propertyType.isRental, label: propertyType.label, pageNumber: 1 },
			},
		]);
		await crawler.run();
	}

	console.log(`\n Scraping complete! Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

(async () => {
	try {
		await scrapeAll();
		await updateRemoveStatus(AGENT_ID);
		process.exit(0);
	} catch (err) {
		console.error(" Fatal Error:", err);
		process.exit(1);
	}
})();
