// Alan de Maid scraper using Playwright with Crawlee
// Agent ID: 103
//
// Usage:
// node backend/scraper-agent-103.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 103;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const recentPageSignatures = new Map();
const processedUrls = new Set();

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function formatPriceDisplay(price, isRental) {
	if (!price) return isRental ? "£0 pcm" : "£0";
	return `£${price}${isRental ? " pcm" : ""}`;
}

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	await sleep(700);
	const detailPage = await browserContext.newPage();
	try {
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 90000,
		});

		await detailPage.waitForTimeout(1500);

		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);

		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
			},
		};
	} catch (err) {
		logger.warn(`Error scraping detail page ${property.link}: ${err.message}`);
		return null;
	} finally {
		await detailPage.close();
	}
}

// utility helpers added later (sleep, formatPriceDisplay, etc)

// Configuration for sales and rentals
const PROPERTY_TYPES = [
	{
		urlPath: "properties/sales/status-available/most-recent-first",
		totalRecords: 388,
		recordsPerPage: 10,
		isRental: false,
		label: "SALES",
	},
	{
		urlPath: "properties/lettings/status-available/most-recent-first",
		totalRecords: 7,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, `Starting page - ${request.url}`, totalPages);

	try {
		await page.waitForSelector(".card", { timeout: 30000 });
	} catch (e) {
		logger.warn(`Listing container not found on page`, pageNum, label);
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const cards = Array.from(document.querySelectorAll(".card"));
		for (const card of cards) {
			try {
				let linkEl = card.querySelector("a");
				let link = linkEl ? linkEl.getAttribute("href") : null;
				if (link && !link.startsWith("http")) {
					link = "https://www.alandemaid.co.uk" + link;
				}

				const titleEl = card.querySelector(".card__text-content");
				const title = titleEl ? titleEl.textContent.trim() : "";

				let bedrooms = null;
				const bedroomsEl = card.querySelector(".card-content__spec-list-number");
				if (bedroomsEl) {
					const bedsText = bedroomsEl.textContent.trim();
					const m = bedsText.match(/\d+/);
					if (m) bedrooms = m[0];
				}

				let price = null;
				const priceEl = card.querySelector(".card__heading");
				if (priceEl) {
					const priceText = priceEl.textContent.trim();
					price = priceText.replace(/[^0-9]/g, "");
				}

				if (link && price && title) {
					results.push({ link, title, price, bedrooms });
				}
			} catch (err) {
				// ignore
			}
		}
		return results;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	const pageSignature = properties
		.map((p) => p.link)
		.slice(0, 5)
		.join("|");
	const signatureKey = isRental ? "LETTINGS" : "SALES";
	const previousSignature = recentPageSignatures.get(signatureKey);
	if (pageSignature && previousSignature === pageSignature) {
		logger.warn(`Page has the same leading links as previous page.`, pageNum, label);
	}
	recentPageSignatures.set(signatureKey, pageSignature);

	for (const property of properties) {
		if (!property.link) continue;

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const numericPrice = Number(property.price.toString().replace(/,/g, ""));
		const price = numericPrice.toLocaleString("en-GB");
		let bedrooms = null;
		if (property.bedrooms) {
			const m = property.bedrooms.match(/\d+/);
			if (m) bedrooms = parseInt(m[0]);
		}

		if (!price) {
			logger.warn(`Skipping update (no price found): ${property.link}`, pageNum, label);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let action = "UNCHANGED";

		if (result.updated) {
			stats.totalSaved++;
			action = "UPDATED";
		}

		let lat = null;
		let lng = null;

		if (!result.isExisting && !result.error) {
			action = "CREATED";
			const detail = await scrapePropertyDetail(page.context(), property);
			lat = detail?.coords?.latitude || null;
			lng = detail?.coords?.longitude || null;

			await updatePriceByPropertyURL(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				lat,
				lng,
			);
			stats.totalSaved++;
			stats.totalScraped++;
			if (isRental) stats.savedRentals++;
			else stats.savedSales++;
			
			await sleep(500);
		}

		logger.property(
			pageNum,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			property.link,
			isRental,
			totalPages,
			action,
			lat,
			lng
		);
	}
}

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
			console.error(` Failed listing page: ${request.url}`);
		},
	});
}

async function scrapeAlanDeMaid() {
	logger.step(`Starting Alan de Maid scraper...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
		const effectiveStartPage = 1;

		for (let pg = effectiveStartPage; pg <= totalPages; pg++) {
			const url = type.isRental
				? `https://www.alandemaid.co.uk/${type.urlPath}#/`
				: `https://www.alandemaid.co.uk/${type.urlPath}/page-${pg}#/`;

			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: `${type.label}_PAGE_${pg}`,
					totalPages,
				},
			});

			if (type.isRental) break;
		}
	}

	if (allRequests.length === 0) {
		logger.step("No pages to scrape.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	logger.step(`Completed Alan de Maid - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// Main execution
(async () => {
	const scrapeStartTime = new Date();
	try {
		await scrapeAlanDeMaid();
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("✅ All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
