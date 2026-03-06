// Mistoria scraper using Playwright with Crawlee
// Agent ID: 224
// Website: mistoriaestateagents.co.uk
// Usage:
// node backend/scraper-agent-224.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 224;
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
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();
	let coords = { latitude: null, longitude: null, bedrooms: property.bedrooms };

	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 40000 });
		await detailPage.waitForTimeout(1000);

		const detailData = await detailPage.evaluate(() => {
			let lat = null,
				lng = null;
			let bedrooms = null;

			// Try to extract bedrooms from page text
			const pageText = document.body.innerText || "";
			const bedroomsMatch = pageText.match(/(\d+)\s*bedroom/i);
			if (bedroomsMatch) {
				bedrooms = parseInt(bedroomsMatch[1], 10);
			}

			const scripts = Array.from(
				document.querySelectorAll('script[type="application/ld+json"]'),
			);
			for (const script of scripts) {
				try {
					const json = JSON.parse(script.innerText);
					const items = json["@graph"] || (Array.isArray(json) ? json : [json]);
					for (const item of items) {
						if (item.geo && item.geo.latitude != null) {
							lat = item.geo.latitude;
							lng = item.geo.longitude;
							break;
						}
					}
				} catch (e) {}
				if (lat) break;
			}
			if (!lat) {
				const allScripts = Array.from(document.querySelectorAll("script"));
				for (const script of allScripts) {
					const content = script.innerText;
					const gmapsMatch = content.match(
						/new\s+google\.maps\.LatLng\(\s*([\d.-]+)\s*,\s*([\d.-]+)\s*\)/i,
					);
					if (gmapsMatch) {
						lat = gmapsMatch[1];
						lng = gmapsMatch[2];
						break;
					}
				}
			}
			return { lat, lng, bedrooms };
		});

		if (detailData.lat) {
			coords.latitude = parseFloat(detailData.lat);
			coords.longitude = parseFloat(detailData.lng);
		}
		if (detailData.bedrooms) {
			coords.bedrooms = detailData.bedrooms;
		}
	} catch (err) {
		logger.error(`Error scraping detail page ${property.link}`, err);
	} finally {
		await detailPage.close();
	}

	return coords;
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

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForTimeout(2000);
		await page.waitForSelector("li.type-property", { timeout: 20000 }).catch(() => {
			logger.warn(`No listing container found on page ${pageNum}`);
		});

		// Extract properties
		const properties = await page.evaluate((isRental) => {
			try {
				const cards = Array.from(document.querySelectorAll("li.type-property"));
				return cards
					.map((card) => {
						const statusText = card.innerText || "";
						// Simple check for sold/let within evaluation
						if (
							statusText.toLowerCase().includes("sold") ||
							statusText.toLowerCase().includes("let stc") ||
							statusText.toLowerCase().includes("let agreed")
						) {
							// We'll filter properly outside using the library helper if needed,
							// but evaluating here saves roundtrips.
						}

						const linkEl = card.querySelector("h3 a");
						const link = linkEl ? linkEl.href : null;
						const title = linkEl ? linkEl.innerText.trim() : "";

						const priceEl = card.querySelector("div.price");
						let priceRaw = priceEl ? priceEl.innerText.trim() : "";

						// Remove tenancy info if present
						const tenancyInfo = card.querySelector("span.lettings-fees");
						if (tenancyInfo) {
							priceRaw = priceRaw.replace(tenancyInfo.innerText, "").trim();
						}

						const bedEl = card.querySelector(".room-bedrooms");
						const bedroomsMatch = bedEl ? bedEl.innerText.match(/\d+/) : null;
						const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

						return { link, title, priceRaw, bedrooms };
					})
					.filter((p) => p.link);
			} catch (e) {
				return [];
			}
		}, isRental);

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);
		counts.totalFound += properties.length;

		for (const property of properties) {
			if (!property.link || processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			counts.totalScraped++;

			// Skip sold properties
			if (isSoldProperty(property.title) || isSoldProperty(property.priceRaw)) {
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

			const priceNum = parsePrice(property.priceRaw);
			if (priceNum === null) {
				counts.totalSkipped++;
				continue;
			}

			const coords = await scrapePropertyDetail(page.context(), property, isRental);

			const result = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				priceNum,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let action = "UNCHANGED";
			if (result.updated) {
				action = "UPDATED";
				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (!result.isExisting && !result.error) {
				action = "CREATED";
				await processPropertyWithCoordinates(
					property.link,
					priceNum,
					property.title,
					coords.bedrooms,
					AGENT_ID,
					isRental,
					null,
					coords.latitude,
					coords.longitude,
				);
				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} else if (result.error) {
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

		logger.step(`Starting Mistoria scraper (Agent ${AGENT_ID})`, `startPage=${startPage}`);

		const browserWSEndpoint = getBrowserlessEndpoint();
		logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

		const crawler = createCrawler(browserWSEndpoint);

		const PROPERTY_TYPES = [
			{
				baseUrl: "https://mistoriaestateagents.co.uk/property-search/page/",
				params:
					"/?address_keyword&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&department=residential-sales&availability&maximum_bedrooms",
				totalPages: 10,
				isRental: false,
				label: "SALES",
			},
			{
				baseUrl: "https://mistoriaestateagents.co.uk/property-search/page/",
				params:
					"/?address_keyword=&department=residential-lettings&availability=&minimum_bedrooms=&maximum_bedrooms=",
				totalPages: 15,
				isRental: true,
				label: "RENTALS",
			},
		];

		const requests = [];
		for (const propertyType of PROPERTY_TYPES) {
			for (let pg = startPage; pg <= propertyType.totalPages; pg++) {
				requests.push({
					url: `${propertyType.baseUrl}${pg}${propertyType.params}`,
					userData: {
						pageNum: pg,
						isRental: propertyType.isRental,
						label: propertyType.label,
						totalPages: propertyType.totalPages,
					},
				});
			}
		}

		await crawler.addRequests(requests);
		await crawler.run();

		logger.step(
			`Completed Mistoria Agent ${AGENT_ID}`,
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
