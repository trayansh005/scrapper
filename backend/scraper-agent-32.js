// Remax scraper using Playwright with Crawlee
// Agent ID: 32
// Usage: node backend/scraper-agent-32.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 32;
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
	const page = await browserContext.newPage();
	try {
		await blockNonEssentialResources(page);
		await page.goto(property.link, { waitUntil: "domcontentloaded", timeout: 60000 });

		// Use page.evaluate for robust extraction directly from the browser context
		const coords = await page.evaluate(() => {
			let lat = null,
				lng = null;

			// 1. Try JSON-LD
			const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
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
						// Direct lat/lng check
						if (item.latitude != null && item.longitude != null) {
							lat = item.latitude;
							lng = item.longitude;
							break;
						}
					}
				} catch (e) {}
				if (lat && lng) break;
			}

			// 2. Fallback: Search script contents for coordinate patterns
			if (!lat || !lng) {
				const allScripts = Array.from(document.querySelectorAll("script"));
				for (const script of allScripts) {
					const content = script.innerText;

					// Match google.maps.LatLng pattern
					const gmapsMatch = content.match(
						/new\s+google\.maps\.LatLng\(\s*([\d.-]+)\s*,\s*([\d.-]+)\s*\)/i,
					);
					if (gmapsMatch) {
						lat = gmapsMatch[1];
						lng = gmapsMatch[2];
						break;
					}

					// Match lat: 53.3, lng: -2.1 pattern
					const coordMatch = content.match(
						/lat\s*[:=]\s*["']?([\d.-]+)["']?\s*,\s*lng\s*[:=]\s*["']?([\d.-]+)["']?/i,
					);
					if (coordMatch) {
						lat = coordMatch[1];
						lng = coordMatch[2];
						break;
					}
				}
			}
			return { lat, lng };
		});

		const { lat, lng } = coords;

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			null,
			lat,
			lng,
		);

		counts.totalSaved++;
	} catch (error) {
		logger.error(`Error detail ${property.link}`, error);
	} finally {
		await page.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ request, page, crawler }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector(".property-item", { timeout: 30000 }).catch(() => {
			logger.warn(`No properties found on page ${pageNum}`);
		});
	} catch (e) {
		return;
	}

	const content = await page.content();
	const $ = cheerio.load(content);
	const $items = $(".property-item");
	const itemCount = $items.length;

	counts.totalFound += itemCount;
	logger.page(pageNum, label, `Found ${itemCount} properties`, totalPages);

	for (let i = 0; i < $items.length; i++) {
		const $item = $($items[i]);

		const statusText = $item.find(".f-price, .p-name").text() || "";
		if (isSoldProperty(statusText)) {
			counts.totalSkipped++;
			continue;
		}

		const linkEl = $item.find("a").first();
		let link = linkEl.attr("href");
		if (!link) continue;
		if (!link.startsWith("http")) link = `https://remax.co.uk${link}`;

		if (processedUrls.has(link)) {
			logger.step(`Skipping duplicate: ${link.substring(0, 60)}`);
			continue;
		}
		processedUrls.add(link);

		const title = $item.find(".p-name").text().trim() || "Remax Property";
		const priceRaw = $item.find(".f-price").text().trim();
		const price = parsePrice(priceRaw);

		const attrText = $item.find(".property-attr").text().trim();
		const bedroomsMatch = attrText.match(/(\d+)\s*Bed/);
		const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

		const property = {
			link,
			price,
			title,
			bedrooms,
			statusText,
		};

		const result = await updatePriceByPropertyURLOptimized(
			link.trim(),
			price,
			title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let action = "UNCHANGED";
		if (result.updated) {
			action = "UPDATED";
		} else if (!result.isExisting && !result.error) {
			action = "CREATED";
			counts.savedSales++;
			counts.totalSaved++;
		}

		counts.totalScraped++;

		const categoryLabel = isRental ? "LETTINGS" : "SALES";
		logger.property(
			title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			link,
			categoryLabel,
			action,
		);

		if (action !== "UNCHANGED") {
			await sleep(100);
		}
	}

	// Pagination logic
	if (itemCount > 0) {
		const baseUrl = isRental
			? "https://remax.co.uk/properties-for-rent/"
			: "https://remax.co.uk/properties-for-sale/";
		const nextPage = pageNum + 1;
		const nextUrl = `${baseUrl}?page=${nextPage}`;
		const nextTotalPages = totalPages + 1;

		await crawler.addRequests([
			{
				url: nextUrl,
				userData: { pageNum: nextPage, isRental, label, totalPages: nextTotalPages },
			},
		]);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function run() {
	const scrapeStartTime = new Date();
	const startPage = getStartPage();
	const isPartialRun = startPage > 1;

	logger.step(`Starting Remax Agent ${AGENT_ID}`, `startPage=${startPage}`);

	const browserWSEndpoint = getBrowserlessEndpoint();

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 360,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed request: ${request.url}`);
		},
	});

	const initialRequests = [];

	// Sales
	initialRequests.push({
		url: `https://remax.co.uk/properties-for-sale/?page=${startPage}`,
		userData: { pageNum: startPage, isRental: false, label: "SALES", totalPages: startPage },
	});

	// Lettings (only if startPage is 1)
	if (startPage === 1) {
		initialRequests.push({
			url: `https://remax.co.uk/properties-for-rent/?page=1`,
			userData: { pageNum: 1, isRental: true, label: "RENTALS", totalPages: 1 },
		});
	}

	await crawler.run(initialRequests);

	if (!isPartialRun) {
		logger.step(`Updating remove status for Agent ${AGENT_ID}`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}

	logger.step(
		`Completed Agent ${AGENT_ID}`,
		`found=${counts.totalFound}, scraped=${counts.totalScraped}, saved=${counts.totalSaved}, skipped=${counts.totalSkipped}`,
	);
}

if (require.main === module) {
	run().catch((err) => {
		logger.error(`Fatal error`, err);
		process.exit(1);
	});
}

module.exports = { run };
