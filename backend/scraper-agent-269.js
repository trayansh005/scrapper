// 99Home Scraper
// Agent ID: 269
// Agent Name: 99Home
// Updated: 23 April 2026

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	extractCoordinatesFromHTML,
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 269;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.99home.co.uk/property_search/ForSale/?min_price=0&max_price=10000000",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.99home.co.uk/property_search/ForRent/?min_price=0&max_price=10000",
		isRental: true,
		label: "RENTALS",
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
}

function getBrowserlessEndpoint() {
	return process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	await sleep(1500);
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 60000 });

		await detailPage.waitForTimeout(3000);

		const detailInfo = await detailPage.evaluate(() => {
			let latitude = null, longitude = null;

			const html = document.documentElement.innerHTML;

			// Coordinates from script tag: var latlog = { lat: 53.97596739999999, lng: -1.5426389};
			const latlogMatch = html.match(/var\s+latlog\s*=\s*{\s*lat:\s*([\d.-]+),\s*lng:\s*([\d.-]+)\s*}/);
			if (latlogMatch) {
				latitude = parseFloat(latlogMatch[1]);
				longitude = parseFloat(latlogMatch[2]);
			}

			// Fallback if not found in latlog
			if (!latitude || !longitude) {
				const latMatch = html.match(/lat\s*[:=]\s*([\d.-]+)/i);
				const lngMatch = html.match(/lng\s*[:=]\s*([\d.-]+)/i);
				if (latMatch && lngMatch) {
					latitude = parseFloat(latMatch[1]);
					longitude = parseFloat(lngMatch[1]);
				}
			}

			return { latitude, longitude };
		});

		return detailInfo;

	} catch (error) {
		logger.error(`Detail page error ${property.link}: ${error.message}`);
		return { latitude: null, longitude: null };
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, isFirstPage } = request.userData;
	logger.step(`Processing ${label} page: ${request.url}`);

	try {
		await page.waitForLoadState("networkidle", { timeout: 45000 }).catch(() => {});
		await page.waitForTimeout(3000);
		await page.waitForSelector('article.property-item', { timeout: 25000 }).catch(() => {});
	} catch (e) {
		logger.error(`Load timeout or no properties on ${label}`);
	}

	// Handle pagination on the first page
	if (isFirstPage) {
		const totalPages = await page.evaluate(() => {
			const paginationLinks = document.querySelectorAll('.pagination li a[data-page]');
			let maxPage = 1;
			paginationLinks.forEach(link => {
				const p = parseInt(link.getAttribute('data-page'), 10);
				if (!isNaN(p) && p > maxPage) maxPage = p;
			});
			return maxPage;
		});

		if (totalPages > 1) {
			logger.step(`Found ${totalPages} pages for ${label}. Queuing remaining pages...`);
			for (let i = 2; i <= totalPages; i++) {
				const nextUrl = request.url.includes('?') 
					? `${request.url}&page=${i}` 
					: `${request.url}?page=${i}`;
				await crawler.addRequests([{
					url: nextUrl,
					userData: { isRental, label, isFirstPage: false }
				}]);
			}
		}
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const items = document.querySelectorAll('article.property-item');

		items.forEach((item) => {
			const linkEl = item.querySelector('h4 a');
			if (!linkEl) return;

			const link = new URL(linkEl.getAttribute('href'), 'https://www.99home.co.uk').href;
			const title = linkEl.innerText.trim() || "Property";

			const priceRaw = item.querySelector('h5.price')?.innerText.trim() || "";
			const flag = item.querySelector('.ribbon span')?.innerText.trim() || "";

			// Bedrooms
			let bedrooms = null;
			const metaItems = item.querySelectorAll('.property-meta span');
			metaItems.forEach(span => {
				const text = span.innerText;
				if (text.toLowerCase().includes('bedroom')) {
					const match = text.match(/(\d+)/);
					if (match) bedrooms = parseInt(match[1], 10);
				}
			});

			results.push({ link, title, priceRaw, bedrooms, flag });
		});

		return results;
	});

	logger.step(`Found ${properties.length} ${label} properties`);

	for (const property of properties) {
		if (!property.link || processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		// Filtering logic: Exclude "Sold STC" and "Let Agreed"
		const status = property.flag.toLowerCase();
		if (status.includes("sold") || status.includes("stc") || status.includes("let") || status.includes("agreed")) {
			logger.property(1, label, property.title.substring(0, 40), "N/A", property.link, isRental, 1, "SKIPPED (Sold/Let)");
			continue;
		}

		const price = parsePrice(property.priceRaw);

		if (!price) {
			logger.property(1, label, property.title.substring(0, 40), "N/A", property.link, isRental, 1, "SKIPPED (No Price)");
			continue;
		}

		let bedrooms = property.bedrooms;

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental
		);

		let action = "UNCHANGED";
		let lat = null, lng = null;

		if (result.updated) {
			counts.totalSaved++;
			action = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const details = await scrapePropertyDetail(page.context(), property);
			lat = details.latitude;
			lng = details.longitude;

			await processPropertyWithCoordinates(
				property.link, price, property.title, bedrooms, AGENT_ID, isRental, null, lat, lng
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++; else counts.savedSales++;
			action = "CREATED";
		}

		logger.property(
			1, label, property.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			property.link, isRental, 1, action, lat, lng
		);

		if (action !== "UNCHANGED") await sleep(700);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 120,
		requestHandlerTimeoutSecs: 1200,
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: handleListingPage,
	});
}

async function scrape99Home() {
	logger.step("Starting 99Home scraper...");

	const scrapeStartTime = new Date();
	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = PROPERTY_TYPES.map(type => ({
		url: type.baseUrl,
		userData: { isRental: type.isRental, label: type.label, isFirstPage: true }
	}));

	await crawler.run(allRequests);

	logger.step(`Completed - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, RENTALS: ${counts.savedRentals}`);

	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
	try {
		await scrape99Home();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
