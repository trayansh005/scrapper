// Taylors scraper using Playwright with Crawlee
// Agent ID: 135
// Website: taylorsestateagents.co.uk
// Usage:
// node backend/scraper-agent-135.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Reduce logging noise
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 135;
const logger = createAgentLogger(AGENT_ID);

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
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		await detailPage.waitForTimeout(1500);
		const html = await detailPage.content();

		// Use helper to extract coordinates from HTML
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			html,
		);

		counts.totalScraped++;
		counts.totalSaved++;
		if (isRental) counts.savedRentals++;
		else counts.savedSales++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		// Wait for listing container
		await page.waitForSelector(".card--list .card, .hf-property-results .card", { timeout: 30000 });
	} catch (e) {
		logger.warn(`Listing container not found on page ${pageNum} (${label})`);
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const cards = Array.from(
			document.querySelectorAll(".card--list .card, .hf-property-results .card"),
		);
		for (const card of cards) {
			const linkEl = card.querySelector("a.card__link") || card.querySelector("a");
			let link = linkEl ? linkEl.getAttribute("href") : null;
			if (!link) continue;

			const fullLink = link.startsWith("http")
				? link
				: "https://www.taylorsestateagents.co.uk" + (link.startsWith("/") ? "" : "/") + link;

			// Extract title
			let titleEl =
				card.querySelector(".card__text-title") ||
				card.querySelector(".card__text-content") ||
				card.querySelector("h3");
			const title = titleEl ? titleEl.textContent.trim() : "Property";

			// Extract price text
			const priceEl = card.querySelector(".card__heading") || card.querySelector(".price");
			const priceText = priceEl ? priceEl.textContent.trim() : "";

			// Extract bedrooms
			const bedroomEl =
				card.querySelector(".card-content__spec-list-number") || card.querySelector(".bedrooms");
			let bedrooms = null;
			if (bedroomEl) {
				const match = bedroomEl.textContent.match(/\d+/);
				bedrooms = match ? parseInt(match[0]) : null;
			}

			// Status/Sold
			const statusText = card.innerText || "";

			results.push({ link: fullLink, title, priceText, bedrooms, statusText });
		}
		return results;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		if (isSoldProperty(property.statusText)) {
			continue;
		}

		const price = parsePrice(property.priceText);
		if (!price) {
			logger.page(pageNum, label, `Skipping (no price): ${property.link}`, totalPages);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		let action = "UNCHANGED";

		if (result.updated) {
			counts.totalSaved++;
			action = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			action = "CREATED";
			await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
			// Throttle detail page visits
			await new Promise((r) => setTimeout(r, 1000));
		} else if (result.error) {
			action = "ERROR";
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
		);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 600,
		launchContext: {
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

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlPath: "properties/sales/status-available/most-recent-first",
		totalRecords: 1075,
		recordsPerPage: 10,
		isRental: false,
		label: "SALES",
	},
	{
		urlPath: "properties/lettings/status-available/most-recent-first",
		totalRecords: 208,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeTaylors() {
	logger.step(`Starting Taylors scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
		logger.step(`Queueing ${type.label} (${totalPages} pages)`);

		for (let pg = Math.max(1, startPage); pg <= totalPages; pg++) {
			allRequests.push({
				url: `https://www.taylorsestateagents.co.uk/${type.urlPath}/page-${pg}#/`,
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: type.label,
					totalPages,
				},
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	}

	logger.step(
		`Completed Taylors - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New lettings: ${counts.savedRentals}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

scrapeTaylors()
	.then(() => {
		logger.step("All done!");
		process.exit(0);
	})
	.catch((error) => {
		logger.error("Unhandled scraper error", error);
		process.exit(1);
	});
