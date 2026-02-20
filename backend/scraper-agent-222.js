// ESPC scraper using Playwright with Crawlee
// Agent ID: 222
// Website: espc.com
// Usage:
// node backend/scraper-agent-222.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 222;

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

	console.log(` Loading: ${request.url}`);

	try {
		await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });
		await page.waitForTimeout(3000);

		const result = await page.evaluate(() => {
			// Find all property link containers that have a title
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

				return {
					url: item.href,
					title,
					price,
					bedrooms,
				};
			});

			// Improved pagination detection for ESPC
			const paginationNext = document.querySelector("a.next, a.nextPage, .paginationList a.next");

			return {
				properties: scraped,
				hasNextPage: !!paginationNext,
			};
		});

		const properties = result.properties;
		console.log(` Found ${properties.length} unique properties on page ${pageNumber || 1}`);
		stats.totalScraped += properties.length;

		for (const property of properties) {
			try {
				const priceNum = property.price ? parseFloat(property.price.replace(/[^0-9.]/g, "")) : null;
				if (priceNum === null) {
					console.log(` No price found: ${property.title}`);
					continue;
				}

				const updateResult = await updatePriceByPropertyURLOptimized(
					property.url.trim(),
					priceNum,
					property.title,
					property.bedrooms,
					AGENT_ID,
					isRental,
				);

				if (!updateResult.isExisting || updateResult.updated) {
					const coords = await extractCoordsFromDetailsPage(page.context(), property.url);

					await processPropertyWithCoordinates(
						property.url.trim(),
						priceNum,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
						null,
						coords ? coords.latitude : null,
						coords ? coords.longitude : null,
					);

					stats.totalSaved++;
					console.log(
						` ${property.title} saved at ${priceNum} (${coords ? "with coords" : "no coords"})`,
					);
				} else {
					console.log(`ℹ No changes for: ${property.title}`);
				}
			} catch (err) {
				console.error(` Error processing property: ${err.message}`);
			}
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

async function extractCoordsFromDetailsPage(browserContext, url) {
	try {
		const tempPage = await browserContext.newPage();
		await tempPage.goto(url, { waitUntil: "domcontentloaded", timeout: 45000 });
		await tempPage.waitForTimeout(1000);

		const coords = await tempPage.evaluate(() => {
			try {
				const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
				for (const script of scripts) {
					const data = JSON.parse(script.innerText);
					const traverse = (obj) => {
						if (obj && obj["@type"] === "GeoCoordinates") {
							return { lat: parseFloat(obj.latitude), lng: parseFloat(obj.longitude) };
						}
						if (obj && obj.geo && obj.geo["@type"] === "GeoCoordinates") {
							return { lat: parseFloat(obj.geo.latitude), lng: parseFloat(obj.geo.longitude) };
						}
						for (let k in obj) {
							if (obj[k] && typeof obj[k] === "object") {
								const res = traverse(obj[k]);
								if (res) return res;
							}
						}
					};
					const res = traverse(data);
					if (res) return res;
				}
				return null;
			} catch (e) {
				return null;
			}
		});

		await tempPage.close();
		if (coords && coords.lat && coords.lng) {
			return { latitude: coords.lat, longitude: coords.lng };
		}
		return null;
	} catch (err) {
		return null;
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
