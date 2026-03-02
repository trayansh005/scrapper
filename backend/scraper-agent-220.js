// Rook Matthews Sayer scraper using Playwright with Crawlee
// Agent ID: 220
// Website: rookmatthewssayer.co.uk
// Usage:
// node backend/scraper-agent-220.js

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

const AGENT_ID = 220;
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

// ============================================================================
// COORDINATE EXTRACTION
// Rook Matthews Sayer (Adfenix) embeds coords in HTML comments:
//   <!--property-longitude:"-1.62600372524389"-->
//   <!--property-latitude:"55.2037196725107"-->
// extractCoordinatesFromHTML already covers latCommentMatch / lngCommentMatch
// which match this exact pattern.
// ============================================================================

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.rookmatthewssayer.co.uk/for-sale",
		totalPages: 123,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.rookmatthewssayer.co.uk/for-rent",
		totalPages: 17,
		isRental: true,
		label: "RENTALS",
	},
];

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;

	logger.page(pageNum, label, request.url);

	try {
		await page.waitForTimeout(1500);
		await page.waitForSelector(".properties-grid-col", { timeout: 20000 }).catch(() => {
			logger.warn(`No listing container found on page ${pageNum}`);
		});

		// Extract properties from the DOM
		const properties = await page.evaluate(() => {
			try {
				const cards = Array.from(
					document.querySelectorAll(".col-lg-4.col-md-12.col-sm-12.properties-grid-col"),
				);
				return cards
					.map((card) => {
						// Skip sold/let cards
						const statusLabel = card.querySelector(
							".listing-custom-label-sold, .listing-custom-label-soldstc, .listing-custom-label-let, .listing-custom-label-letstc",
						);
						if (statusLabel) return null;

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
						const priceRaw = priceEl ? priceEl.textContent.trim() : "";

						// Bedrooms from first detail-icon li
						const detailIcons = Array.from(card.querySelectorAll(".detail-icons ul li"));
						let bedrooms = null;
						if (detailIcons.length >= 1) {
							const text = detailIcons[0].textContent.trim();
							const match = text.match(/(\d+)/);
							if (match) bedrooms = parseInt(match[1], 10);
						}

						return { link, title, priceRaw, bedrooms, statusText: card.innerText || "" };
					})
					.filter(Boolean);
			} catch (e) {
				return [];
			}
		});

		logger.page(pageNum, label, `Found ${properties.length} properties`);

		for (const property of properties) {
			if (!property.link) continue;
			if (isSoldProperty(property.statusText || "")) continue;

			// Parse price using shared helper
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
					await detailPage.waitForTimeout(500);

					html = await detailPage.content();

					// Extract coords from Adfenix HTML comment tags:
					//   <!--property-latitude:"55.203..."-->
					//   <!--property-longitude:"-1.626..."-->
					const coords = extractCoordinatesFromHTML(html);
					latitude = coords?.latitude || null;
					longitude = coords?.longitude || null;

					logger.step(
						`Coords: ${latitude || "No Lat"}, ${longitude || "No Lng"}`,
					);
				} catch (err) {
					logger.error(`Detail page failed: ${property.link}`);
				} finally {
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
				await sleep(500); // DB politeness delay for writes
			}
		}
	} catch (error) {
		logger.error(`Error in ${label} page ${pageNum}: ${error.message}`);
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

// ============================================================================
// MAIN
// ============================================================================

async function scrapeRookMatthewsSayer() {
	logger.step(`Starting Rook Matthews Sayer scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

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

	logger.step(
		`Completed Rook Matthews Sayer - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
	);
}

(async () => {
	try {
		const scrapeStartTime = new Date();
		await scrapeRookMatthewsSayer();
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
