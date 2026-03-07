// Ashtons scraper using Playwright with Crawlee
// Agent ID: 240
// Usage:
// node backend/scraper-agent-240.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized, processPropertyWithCoordinates, } = require("./lib/db-helpers.js");
const { parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources, sleep, } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");



const AGENT_ID = 240;
const logger = createAgentLogger(AGENT_ID);
const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

// ------------------------------------------------------------------
// DETAIL PAGE SCRAPE
// ------------------------------------------------------------------

async function scrapePropertyDetail(browserContext, property, isRental) {
	const scrapeStartTime = new Date();
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		logger.step(`[Detail] Scraping coordinates: ${property.title}`);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		const htmlContent = await detailPage.content();

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent
		);

		stats.totalScraped++;

		if (isRental) stats.savedRentals++;
		else stats.savedSales++;

		logger.step(`[Detail] Coordinates extracted`);
	} catch (err) {
		logger.error(`Detail scrape error: ${err?.message || err}`);
	} finally {
		await detailPage.close();
	}
}

// ------------------------------------------------------------------
// PROPERTY TYPES
// ------------------------------------------------------------------

const PROPERTY_TYPES = [
	{
		url: "https://www.ashtons.co.uk/buy?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
		isRental: false,
		label: "FOR SALE",
	},
	{
		url: "https://www.ashtons.co.uk/rent?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
		isRental: true,
		label: "FOR LETTING",
	},
];

// ------------------------------------------------------------------
// MAIN SCRAPER
// ------------------------------------------------------------------

async function scrapeAshtons() {
	logger.step(`Starting Ashtons scraper (Agent ${AGENT_ID})`);

	const browserWSEndpoint = getBrowserlessEndpoint();

	const crawler = new PlaywrightCrawler({

		maxConcurrency: 2,
		maxRequestRetries: 2,

		navigationTimeoutSecs: 30,
		requestHandlerTimeoutSecs: 600,

		launchContext: {
			launchOptions: {
				browserWSEndpoint,
			},
		},

		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],

		async requestHandler({ page, request }) {

			const { isRental, label } = request.userData;

			logger.page(pageNum, label, "Processing listing page...");

			await page.waitForTimeout(2000);

			await page
				.waitForSelector(".c-property-card", { timeout: 15000 })
				.catch(() => logger.step("No property cards found"));

			// ----------------------------------------------------
			// CLICK "SHOW MORE"
			// ----------------------------------------------------

			let clickCount = 0;
			const maxClicks = 50;

			while (clickCount < maxClicks) {
				const showMoreButton = await page.$(
					".c-property-search__list-action button.c-button--tertiary"
				);

				if (!showMoreButton) break;

				const disabled = await showMoreButton.evaluate(el => el.disabled);

				if (disabled) break;

				logger.step(`Clicking Show More (${clickCount + 1})`);

				await page.evaluate(() => {
					const btn = document.querySelector(
						".c-property-search__list-action button.c-button--tertiary"
					);
					if (btn) btn.click();
				});

				await page.waitForTimeout(2000);
				clickCount++;
			}

			logger.step(`Finished loading properties after ${clickCount} clicks`);

			// ----------------------------------------------------
			// EXTRACT PROPERTIES
			// ----------------------------------------------------

			const properties = await page.evaluate(() => {

				const cards = Array.from(document.querySelectorAll(".c-property-card"));

				return cards.map(card => {

					const linkEl = card.querySelector("a.c-property-card__anchor");
					if (!linkEl) return null;

					const href = linkEl.getAttribute("href");
					if (!href) return null;

					const link = href.startsWith("/")
						? `https://www.ashtons.co.uk${href}`
						: href;

					const priceEl = card.querySelector(".c-property-price__value");

					let price = "";
					if (priceEl) {
						const m = priceEl.textContent.match(/£([\d,]+)/);
						if (m) price = m[1];
					}

					const titleEl = card.querySelector(".c-property-card__title");

					const title = titleEl
						? titleEl.textContent.trim().replace(/\s+/g, " ")
						: "";

					const bedEl = card.querySelector(
						".c-property-feature--bedrooms .c-property-feature__value"
					);

					let bedrooms = null;

					if (bedEl) {
						const match = bedEl.textContent.match(/(\d+)/);
						if (match) bedrooms = match[1];
					}

					return { link, title, price, bedrooms };

				}).filter(Boolean);
			});

			logger.step(`Found ${properties.length} properties`);

			// ----------------------------------------------------
			// PROCESS PROPERTIES
			// ----------------------------------------------------

			const batchSize = 5;

			for (let i = 0; i < properties.length; i += batchSize) {

				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {

						if (!property.link) return;

						if (actionTaken === "CREATED") {
							await new Promise((resolve) => setTimeout(resolve, 500));
						}
						if (isSoldProperty(property.statusText || "")) {
							logger.warn(`Skipping sold property`, pageNum, label);
							return;
						}
						if (processedUrls.has(property.link)) {
							logger.warn(`Skipping duplicate`, pageNum, label);
							return;
						}
						processedUrls.add(property.link);

						try {

							let actionTaken = "UNCHANGED";

							const priceNum = parsePrice(property.price);

							if (priceNum === null) {
								logger.warn(`No price found`, pageNum, label);
								return;
							}

							const result = await updatePriceByPropertyURLOptimized(
								property.link.trim(),
								priceNum,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
							);

							if (result.updated) {
								stats.totalSaved++;
								actionTaken = "UPDATED";
							}

							if (!result.isExisting && !result.error) {

								await scrapePropertyDetail(
									page.context(),
									{
										...property,
										price: priceNum,
									},
									isRental
								);

								actionTaken = "CREATED";
							}

							const priceDisplay = isNaN(priceNum) ? "N/A" : formatPriceUk(priceNum);

							logger.property(
								pageNum,
								label,
								property.title,
								priceDisplay,
								property.link,
								isRental,
								null,
								actionTaken
							);

							// STEP 7
							if (actionTaken === "CREATED") {
								await new Promise((resolve) => setTimeout(resolve, 500));
							}

						} catch (err) {
							logger.error(`DB error`, err, pageNum, label);
						}

					})
				);
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Request failed: ${request.url}`);
		},
	});

	const initialRequests = PROPERTY_TYPES.map(type => ({
		url: type.url,
		userData: {
			isRental: type.isRental,
			label: type.label,
		},
	}));

	await crawler.run(initialRequests);

	// ----------------------------------------------------
	// FINAL SUMMARY
	// ----------------------------------------------------

	logger.step(`Agent ${AGENT_ID} Completed`);

	logger.step(`Total Scraped: ${stats.totalScraped}`);
	logger.step(`Total Saved: ${stats.totalSaved}`);
	logger.step(`Sales: ${stats.savedSales}`);
	logger.step(`Rentals: ${stats.savedRentals}`);

	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

// ------------------------------------------------------------------
// RUN
// ------------------------------------------------------------------

(async () => {
	try {
		await scrapeAshtons();
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.message || err}`);
		process.exit(1);
	}
})();