// Carter Jonas scraper using Playwright with Crawlee
// Agent ID: 113
//
// Usage:
// node backend/scraper-agent-113.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 113;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePropertyCard($, element) {
	try {
		const $li = $(element);

		// Check for "Sold" status in any badge or text
		const soldBadgeText = $li.find(".bg-plum").text().trim();
		if (soldBadgeText.toLowerCase().includes("sold")) return null;

		// General sold keyword check on the card
		if (isSoldProperty($li.text())) return null;

		const h3 = $li.find("h3");
		const h4 = $li.find("h4");

		if (!h3.length || !h4.length) return null;

		const title = h3.text().trim();
		const priceText = h4.text().trim();

		const linkRel = h3.find("a").attr("href") || $li.find("a").attr("href");
		if (!linkRel) return null;
		const link = linkRel.startsWith("http") ? linkRel : `https://www.carterjonas.co.uk${linkRel}`;

		// Specs (Bedrooms)
		const specs = [];
		$li.find("ul li").each((j, specEl) => {
			specs.push($(specEl).text().trim());
		});
		const bedsMatch = specs.find((s) => /^\d+$/.test(s));
		const bedroomsValue = bedsMatch || null;
		const bedrooms = bedroomsValue ? parseInt(bedroomsValue) : null;

		if (link && title) {
			return { link, title, priceText, bedrooms };
		}
		return null;
	} catch (error) {
		return null;
	}
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const properties = [];

	$("li").each((index, element) => {
		const property = parsePropertyCard($, element);
		if (property) {
			properties.push(property);
		}
	});

	return properties;
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
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		const detailData = await detailPage.evaluate(() => {
			try {
				let lat = null;
				let lon = null;
				// Check meta tags first
				const latMeta = document.querySelector('meta[property="place:location:latitude"]');
				const lonMeta = document.querySelector('meta[property="place:location:longitude"]');
				if (latMeta && lonMeta) {
					lat = parseFloat(latMeta.content);
					lon = parseFloat(lonMeta.content);
				}

				if (!lat) {
					const html = document.documentElement.innerHTML;
					const latMatch = html.match(/"latitude":\s*(-?\d+\.\d+)/i);
					const lonMatch = html.match(/"longitude":\s*(-?\d+\.\d+)/i);
					if (latMatch && lonMatch) {
						lat = parseFloat(latMatch[1]);
						lon = parseFloat(lonMatch[1]);
					}
				}
				
				return { lat, lon };
			} catch (e) {}
			return null;
		});

		return {
			price: property.price,
			bedrooms: property.bedrooms,
			title: property.title,
			coords: {
				latitude: detailData?.lat || null,
				longitude: detailData?.lon || null,
			},
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	// Handle cookie dismissal if present
	const cookieButton = page.getByRole("button", { name: "Accept All Cookies" });
	if (await cookieButton.isVisible()) {
		await cookieButton.click();
		await page.waitForTimeout(1000);
	}

	await page.waitForTimeout(2000);
	await page.waitForSelector("li h3", { timeout: 30000 }).catch(() => {});

	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		try {
			// Skip properties with no price
			const price = parsePrice(property.priceText);
			if (!price || price === 0) continue;

			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			const result = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";
			if (result.updated) {
				stats.totalSaved++;
				propertyAction = "UPDATED";
			}

			let lat = null;
			let lng = null;
			let finalPrice = price;
			let finalTitle = property.title;
			let finalBedrooms = property.bedrooms;

			if (!result.isExisting && !result.error) {
				const detail = await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
				if (detail) {
					lat = detail.coords.latitude;
					lng = detail.coords.longitude;
					finalPrice = detail.price || price;
					finalTitle = detail.title || property.title;
					finalBedrooms = detail.bedrooms || property.bedrooms;

					await processPropertyWithCoordinates(
						property.link.trim(),
						finalPrice,
						finalTitle,
						finalBedrooms,
						AGENT_ID,
						isRental,
						null, // html
						lat,
						lng
					);

					stats.totalSaved++;
					propertyAction = "CREATED";
				}
			}

			logger.property(
				pageNum,
				label,
				finalTitle,
				formatPriceDisplay(finalPrice, isRental),
				property.link,
				isRental,
				totalPages,
				propertyAction,
				lat,
				lng,
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500);
			}
		} catch (err) {
			logger.error(`Error processing property ${property.link}: ${err.message}`, err, pageNum, label);
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
		requestHandlerTimeoutSecs: 600,
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
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeCarterJonas() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Carter Jonas scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const maxPages = 50;

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	
	// Queue SALES pages
	for (let pg = Math.max(1, startPage); pg <= maxPages; pg++) {
		allRequests.push({
			url: `https://www.carterjonas.co.uk/property-search?division=Homes&area=GreaterLondon&toBuy=true&sortOrder=HighestPriceFirst&page=${pg}`,
			userData: { pageNum: pg, isRental: false, label: "SALES", totalPages: maxPages },
		});
	}
	
	// Queue RENTALS pages
	if (startPage === 1) {
		for (let pg = 1; pg <= maxPages; pg++) {
			allRequests.push({
				url: `https://www.carterjonas.co.uk/property-search?division=Homes&area=GreaterLondon&toBuy=false&sortOrder=HighestPriceFirst&page=${pg}`,
				userData: { pageNum: pg, isRental: true, label: "RENTALS", totalPages: maxPages },
			});
		}
	}

	if (allRequests.length > 0) {
		logger.step(`Queueing ${allRequests.length} listing pages...`);
		await crawler.addRequests(allRequests);
		await crawler.run();
	}

	logger.step(`Finished Carter Jonas - Saved: ${stats.totalSaved}`);
	
	if (startPage === 1) {
		logger.step(`Updating remove status...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

(async () => {
	try {
		await scrapeCarterJonas();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.message || err}`);
		process.exit(1);
	}
})();
