// Homesea scraper using Playwright with Crawlee
// Agent ID: 217
// Usage:
// node backend/scraper-agent-217.js

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
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 217;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// Homesea: 167 properties, 9 per page => 19 pages
const PROPERTY_TYPES = [
	{
		urlBase: "https://homesea.co.uk/property-search/page",
		totalRecords: 167,
		totalPages: 19,
		recordsPerPage: 9,
		isRental: false,
		label: "SALES",
	},
];

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;

	logger.page(pageNum, label, request.url);

	await page.waitForTimeout(2000);
	await page.waitForSelector("li.type-property", { timeout: 15000 }).catch(() => {
		logger.warn(`No listing container found on page ${pageNum}`);
	});

	const properties = await page.evaluate(() => {
		try {
			const items = Array.from(document.querySelectorAll("li.type-property"));
			return items
				.map((li) => {
					try {
						const anchor = li.querySelector("a[href]");
						const link = anchor ? anchor.href : null;

						const title = li.querySelector("h2 a")?.textContent?.trim() || "";
						const priceRaw = li.querySelector(".price")?.textContent?.trim() || "";

						// Bedrooms: look for 'Bedrooms:' text in rooms list
						let bedrooms = null;
						const rooms = li.querySelectorAll("ul.rooms li");
						for (const r of rooms) {
							const txt = r.textContent || "";
							if (txt.toLowerCase().includes("bedrooms")) {
								const m = txt.match(/(\d+)/);
								if (m) bedrooms = m[1];
								break;
							}
						}

						if (link) return { link, title, priceRaw, bedrooms };
						return null;
					} catch (e) {
						return null;
					}
				})
				.filter((p) => p !== null);
		} catch (err) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`);

	for (const property of properties) {
		if (!property.link) continue;
		if (isSoldProperty(property.priceRaw || "")) continue;

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.warn(`Skipping (no price): ${property.link}`);
			continue;
		}

		// --- Agent 39 base pattern: check existing → update or create ---
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
		} else if (result.isExisting) {
			counts.totalScraped++;
		}

		let propertyAction = "UNCHANGED";
		if (result.updated) propertyAction = "UPDATED";

		if (!result.isExisting && !result.error) {
			propertyAction = "CREATED";

			// Fetch detail page ONLY for new properties to extract coords
			const detailPage = await page.context().newPage();
			let html = null;
			let latitude = null;
			let longitude = null;

			try {
				await blockNonEssentialResources(detailPage);
				await detailPage.goto(property.link, {
					waitUntil: "domcontentloaded",
					timeout: 30000,
				});

				html = await detailPage.content();
				const coords = await extractCoordinatesFromHTML(html);
				latitude = coords?.latitude || null;
				longitude = coords?.longitude || null;

				logger.step(`Coords: ${latitude || "No Lat"}, ${longitude || "No Lng"}`);
			} catch (err) {
				logger.error(`Detail page failed: ${property.link}`);
			} finally {
				// Single close in finally — prevents the double-close bug
				await detailPage.close();
			}

			await processPropertyWithCoordinates(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
				html,
				latitude,
				longitude,
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
		}

		logger.property(
			pageNum,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			property.link,
			isRental,
			null,
			propertyAction,
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(500);
		}
	}
}

// ============================================================================
// CRAWLER SETUP
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
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeHomesea() {
	logger.step(`Starting Homesea scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	for (const propertyType of PROPERTY_TYPES) {
		logger.step(
			`Processing ${propertyType.label} (${propertyType.totalPages} pages, ${propertyType.recordsPerPage} per page)`,
		);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}/${pg}/?department=residential-sales&address_keyword&radius&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&availability=2`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	logger.step(
		`Completed Homesea - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
}

(async () => {
	try {
		const scrapeStartTime = new Date();
		await scrapeHomesea();
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
