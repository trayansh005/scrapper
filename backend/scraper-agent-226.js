// Palmer Partners scraper using Playwright with Crawlee
// Agent ID: 226
// Website: palmerpartners.com
// Usage:
// node backend/scraper-agent-226.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 226;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
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
// CRAWLER CONFIGURATION
// ============================================================================

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
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			const { pageNum, label } = request.userData || {};
			logger.error(`Failed listing page: ${request.url}`, null, pageNum, label);
		},
	});
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNum = 1, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForTimeout(2000);
		await page
			.waitForSelector('.property, .property-card, a[href*="/property/"]', { timeout: 20000 })
			.catch(() => {
				logger.warn(`No listing container found on page ${pageNum}`);
			});

		// Extract properties
		const properties = await page.evaluate((isRental) => {
			try {
				const items = Array.from(
					document.querySelectorAll(".property-card, .search-items li, .property, .row.property"),
				);
				return items
					.map((el) => {
						const linkTag = el.querySelector('a[href*="/property/"]');
						const priceText =
							el.querySelector(".price, .property-price, .price-display, .list-price")?.innerText ||
							"";
						const title =
							el.querySelector(".address, .property-address, .address-display, .list-address")
								?.innerText || "";
						const status =
							el.querySelector(".property-status, .status, .label")?.innerText?.trim() || "";
						return { link: linkTag?.href, priceText, title, status };
					})
					.filter((p) => p.link);
			} catch (err) {
				return [];
			}
		}, isRental);

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);
		counts.totalFound += properties.length;

		for (const property of properties) {
			if (!property.link || processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			counts.totalScraped++;

			// Skip sold/let properties
			const statusLower = property.status.toLowerCase();
			const isSold =
				(isRental && (statusLower.includes("let agreed") || statusLower.includes("let stc"))) ||
				(!isRental &&
					(statusLower.includes("sold") ||
						statusLower.includes("under offer") ||
						statusLower.includes("sold stc")));

			if (isSold) {
				counts.totalSkipped++;
				logger.property(
					property.title.substring(0, 40),
					formatPriceDisplay(null, isRental),
					property.link,
					isRental ? "RENTALS" : "SALES",
					"SKIPPED",
				);
				continue;
			}

			const priceNum = parsePrice(property.priceText);
			if (priceNum === null) {
				counts.totalSkipped++;
				continue;
			}

			// For new properties, scrape details first to get bedrooms and coordinates
			let propData = { bedrooms: null, lat: null, lng: null };
			const propExists = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				priceNum,
				property.title,
				null,
				AGENT_ID,
				isRental,
			);

			let action = "UNCHANGED";

			if (!propExists.isExisting && !propExists.error) {
				// Property is new - scrape details to get full data
				const detailPage = await page.context().newPage();
				try {
					await blockNonEssentialResources(detailPage);
					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 40000,
					});

					const propDataRaw = await detailPage.evaluate(() => {
						const data = { lat: null, lng: null, bedrooms: null };
						const bedsInput = document.querySelector('input[name="beds"]');
						if (bedsInput) data.bedrooms = bedsInput.value;
						const allHiddenInputs = Array.from(
							document.querySelectorAll('input[type="hidden"]'),
						);
						for (const input of allHiddenInputs) {
							const val = input.value?.trim() || "";
							if (val.startsWith("[") && val.includes('"lat"') && val.includes('"lng"')) {
								try {
									const coords = JSON.parse(val);
									if (coords?.[0]) {
										data.lat = coords[0].lat;
										data.lng = coords[0].lng;
										if (!data.bedrooms && coords[0].beds) data.bedrooms = coords[0].beds;
									}
								} catch (e) {}
							}
						}
						return data;
					});

					const htmlContent = await detailPage.content();
					propData = propDataRaw;

					// Create the new property with full details including coordinates
					await processPropertyWithCoordinates(
						property.link.trim(),
						priceNum,
						property.title,
						propData.bedrooms,
						AGENT_ID,
						isRental,
						htmlContent,
						propData.lat,
						propData.lng,
					);

					action = "CREATED";
					counts.totalSaved++;
					if (isRental) counts.savedRentals++;
					else counts.savedSales++;
				} catch (err) {
					logger.error(`Error scraping detail for new property ${property.link}`, err);
					action = "ERROR";
					counts.totalSkipped++;
				} finally {
					await detailPage.close();
				}
			} else if (propExists.updated) {
				action = "UPDATED";
				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (propExists.error) {
				action = "ERROR";
				counts.totalSkipped++;
			}

			logger.property(
				property.title.substring(0, 40),
				formatPriceDisplay(priceNum, isRental),
				property.link,
				isRental ? "RENTALS" : "SALES",
				action,
			);

			if (action !== "UNCHANGED") {
				await sleep(100);
			}
		}

		// Handle pagination
		if (pageNum === 1) {
			const maxPage = await page.evaluate(() => {
				const paginationLinks = Array.from(document.querySelectorAll(".pagination a"));
				let highest = 1;
				paginationLinks.forEach((a) => {
					const val = parseInt(a.textContent.trim());
					if (!isNaN(val) && val > highest) highest = val;
				});
				return highest;
			});

			if (maxPage > 1) {
				for (let p = 2; p <= maxPage; p++) {
					const pageUrl = `${request.url}${request.url.includes("?") ? "&" : "?"}page=${p}`;
					await crawler.addRequests([
						{
							url: pageUrl,
							userData: { isRental, label, pageNum: p, totalPages: maxPage },
						},
					]);
				}
			}
		}
	} catch (error) {
		logger.error(`Error processing page ${pageNum} for ${label}`, error);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		const scrapeStartTime = new Date();
		const startPage = getStartPage();
		const isPartialRun = startPage > 1;

		logger.step(`Starting Palmer Partners scraper (Agent ${AGENT_ID})`, `startPage=${startPage}`);

		const browserWSEndpoint = getBrowserlessEndpoint();
		logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

		const PROPERTY_TYPES = [
			{
				urlBase: "https://www.palmerpartners.com/buy/property-for-sale/",
				isRental: false,
				label: "SALES",
			},
			{
				urlBase: "https://www.palmerpartners.com/let/property-to-let/",
				isRental: true,
				label: "RENTALS",
			},
		];

		for (const propertyType of PROPERTY_TYPES) {
			const crawler = createCrawler(browserWSEndpoint);
			await crawler.addRequests([
				{
					url: propertyType.urlBase,
					userData: {
						isRental: propertyType.isRental,
						label: propertyType.label,
						pageNum: startPage,
						totalPages: startPage,
					},
				},
			]);
			await crawler.run();
		}

		logger.step(
			`Completed Palmer Partners Agent ${AGENT_ID}`,
			`found=${counts.totalFound}, scraped=${counts.totalScraped}, saved=${counts.totalSaved}, skipped=${counts.totalSkipped}, sales=${counts.savedSales}, rentals=${counts.savedRentals}`,
		);

		if (!isPartialRun) {
			logger.step("Updating remove status...");
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		} else {
			logger.warn("Partial run detected. Skipping updateRemoveStatus.");
		}

		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
