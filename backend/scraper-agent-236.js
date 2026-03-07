// Avocado Property Agents scraper using Playwright with Crawlee
// Agent ID: 236
// Usage:
// node backend/scraper-agent-236.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);
const logger = createAgentLogger(236);

const AGENT_ID = 236;

const stats = {
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

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

const PROPERTY_TYPES = [
	{
		urlBase: "https://avocadopropertyagents.co.uk/property-for-sale?page=",
		totalRecords: 172,
		recordsPerPage: 22,
		totalPages: Math.ceil(172 / 22),
		isRental: false,
		label: "FOR SALE",
		suffix: "",
	},
	{
		urlBase:
			"https://avocadopropertyagents.co.uk/property-to-rent/property/any-bed/all-location?exclude=1&page=",
		totalRecords: 5,
		recordsPerPage: 22,
		totalPages: Math.ceil(5 / 22) || 1,
		isRental: true,
		label: "TO LET",
		suffix: "",
	},
];

async function scrapeAvocado() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Avocado scraper...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless...`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				browserWSEndpoint,
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, totalPages, isRental, label } = request.userData;

			logger.page(pageNum, label, `Processing listing page...`, totalPages);

			await page.waitForTimeout(1200);

			// Wait for card elements
			await page
				.waitForSelector(".card", { timeout: 15000 })
				.catch(() => logger.warn(`No cards found`, pageNum, label));

			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll(".card"));
					return cards
						.filter((card) => !card.classList.contains("card--property-worth"))
						.map((card) => {
							try {
								// Find anchor inside card (image or title link)
								const a = card.querySelector('a[href*="/property/"]');
								if (!a) return null;
								const href = a.getAttribute("href");
								const link = href.startsWith("/")
									? `https://avocadopropertyagents.co.uk${href}`
									: href;

								// Price
								const priceEl = card.querySelector(".price-value, .card-price, .price");
								const rawPrice = priceEl ? priceEl.textContent.trim() : "";
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = parseInt(m[0].replace(/,/g, "")).toLocaleString();
								}

								// Title
								const title = a.textContent ? a.textContent.trim() : "";

								// Bedrooms: look for i.fa-bed then the following .number span
								let bedrooms = null;
								const bedIcon = card.querySelector("i.fa-bed, .icon-bedroom");
								if (bedIcon) {
									// Try immediate sibling(s)
									let el = bedIcon.nextElementSibling;
									while (el && !(el.classList && el.classList.contains("number")))
										el = el.nextElementSibling;
									if (el && el.textContent && el.textContent.trim()) {
										bedrooms = el.textContent.trim();
									} else {
										// Fallback: take the first .number inside the card detail (usually bedrooms)
										const nums = Array.from(
											card.querySelectorAll(".card-content__detail .number, .number"),
										);
										if (nums.length) bedrooms = nums[0].textContent.trim();
									}
								} else {
									// If there's no bed icon, still try to find a .number that appears near bedroom text
									const nums = Array.from(
										card.querySelectorAll(".card-content__detail .number, .number"),
									);
									if (nums.length) bedrooms = nums[0].textContent.trim();
								}

								const statusText = card.innerText || "";

								return { link, title, price, bedrooms, statusText };
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					return [];
				}
			});

			logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

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
							const priceNum = parsePrice(property.price);

							if (priceNum === null) {
								logger.warn(`No price found`, pageNum, label);
								return;
							}

							const result = await updatePriceByPropertyURLOptimized(
								property.link,
								priceNum,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
							);

							let actionTaken = "UNCHANGED";
							
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
									isRental,
									pageNum,
									label,
									totalPages,
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
								totalPages,
								actionTaken,
							);

							// Conditional sleep: only if property was CREATED
							if (actionTaken === "CREATED") {
								await new Promise((resolve) => setTimeout(resolve, 500));
							}
						} catch (dbErr) {
							logger.error(`DB error`, dbErr, pageNum, label);
						}
					}),
				);

				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Request failed`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}${pg}${propertyType.suffix}`;
			requests.push({
				url,
				userData: {
					pageNum: pg,
					totalPages: propertyType.totalPages,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}

		await crawler.run(requests);
	}

	logger.step(`Completed Avocado scraper - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(
	browserContext,
	property,
	isRental,
	pageNum,
	label,
	totalPages,
) {
	await sleep(500);

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
			htmlContent,
		);

		stats.totalSaved++;
		stats.totalScraped++;
		if (isRental) stats.savedRentals++;
		else stats.savedSales++;
	} catch (error) {
		logger.error(`Error scraping detail page`, error, pageNum, label);
	} finally {
		await detailPage.close();
	}
}

(async () => {
	try {
		await scrapeAvocado();
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
