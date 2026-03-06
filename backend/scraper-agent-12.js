// Purplebricks scraper using Playwright with Browserless
// Agent ID: 12
// Usage:
// node backend/scraper-agent-12.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { formatPriceUk } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 12;
const logger = createAgentLogger(AGENT_ID);
const scrapeStartTime = new Date();

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

const processedUrls = new Set();

const PROPERTY_TYPES = [
	{
		urlBase:
			"https://www.purplebricks.co.uk/search/property-for-sale/greater-london/london?page=1&sortBy=2&betasearch=true&latitude=51.5072178&longitude=-0.1275862&location=london&searchRadius=2&searchType=ForSale&soldOrLet=false",
		totalPages: 72,
		isRental: false,
		label: "LONDON_SALES",
	},
	{
		urlBase:
			"https://www.purplebricks.co.uk/search/property-for-sale/west-midlands/birmingham?page=1&sortBy=2&betasearch=true&latitude=52.4822694&longitude=-1.8900078&location=birmingham&searchRadius=2&searchType=ForSale&soldOrLet=false",
		totalPages: 14,
		isRental: false,
		label: "BIRMINGHAM_SALES",
	},
	{
		urlBase:
			"https://www.purplebricks.co.uk/search/property-for-sale/greater-manchester/manchester?page=1&sortBy=2&betasearch=true&latitude=53.4807593&longitude=-2.2426305&location=manchester&searchRadius=2&searchType=ForSale&soldOrLet=false",
		totalPages: 21,
		isRental: false,
		label: "MANCHESTER_SALES",
	},
	{
		urlBase:
			"https://www.purplebricks.co.uk/search/property-to-rent/greater-london/london?page=1&sortBy=2&betasearch=true&latitude=51.5072178&longitude=-0.1275862&location=london&searchRadius=2&searchType=ForRent&soldOrLet=false",
		totalPages: 2,
		isRental: true,
		label: "LONDON_RENTS",
	},
];

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

function isValidCoord(latitude, longitude) {
	if (latitude === null || longitude === null) return false;
	if (Number.isNaN(latitude) || Number.isNaN(longitude)) return false;
	return Math.abs(latitude) <= 90 && Math.abs(longitude) <= 180;
}

function extractCoordsFromMapsHref(mapsHref) {
	if (!mapsHref) return { latitude: null, longitude: null };

	let match = mapsHref.match(/@([\-0-9.]+),([\-0-9.]+)/);
	if (match) {
		return { latitude: parseFloat(match[1]), longitude: parseFloat(match[2]) };
	}

	match = mapsHref.match(/[?&]ll=([\-0-9.]+),([\-0-9.]+)/);
	if (match) {
		return { latitude: parseFloat(match[1]), longitude: parseFloat(match[2]) };
	}

	match = mapsHref.match(/!3d([\-0-9.]+)!4d([\-0-9.]+)/);
	if (match) {
		return { latitude: parseFloat(match[1]), longitude: parseFloat(match[2]) };
	}

	return { latitude: null, longitude: null };
}

function extractCoordsFromHtml(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const mapsHref =
		$("a[href*='google.com/maps']").attr("href") ||
		$("a[href*='maps.google']").attr("href") ||
		$("a[href*='maps?q=']").attr("href");

	const mapCoords = extractCoordsFromMapsHref(mapsHref);
	if (isValidCoord(mapCoords.latitude, mapCoords.longitude)) {
		return mapCoords;
	}

	const patterns = [
		/"latitude"\s*:\s*([\-0-9.]+)[\s\S]*?"longitude"\s*:\s*([\-0-9.]+)/i,
		/"longitude"\s*:\s*([\-0-9.]+)[\s\S]*?"latitude"\s*:\s*([\-0-9.]+)/i,
		/"lat"\s*:\s*([\-0-9.]+)[\s\S]*?"lng"\s*:\s*([\-0-9.]+)/i,
		/"lng"\s*:\s*([\-0-9.]+)[\s\S]*?"lat"\s*:\s*([\-0-9.]+)/i,
	];

	for (const pattern of patterns) {
		const match = htmlContent.match(pattern);
		if (!match) continue;

		const first = parseFloat(match[1]);
		const second = parseFloat(match[2]);
		const isReversed = pattern === patterns[1] || pattern === patterns[3];
		const latitude = isReversed ? second : first;
		const longitude = isReversed ? first : second;

		if (isValidCoord(latitude, longitude)) {
			return { latitude, longitude };
		}
	}

	return { latitude: null, longitude: null };
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const results = [];

	const $list = $("[data-testid='results-list']");
	const $items = $list.length ? $list.find("li") : $("li");

	$items.each((_, el) => {
		const $item = $(el);
		const $link = $item.find("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']");
		if (!$link.length) return;

		const href = $link.attr("href");
		if (!href) return;

		const link = href.startsWith("http") ? href : `https://www.purplebricks.co.uk${href}`;
		const priceText =
			$item.find("[data-testid='search-result-price']").text().trim() ||
			$item.find(".sc-cda42038-7").text().trim();
		const price = formatPriceUk(priceText);

		const address =
			$item.find("[data-testid='search-result-address']").text().trim() ||
			$item.find(".sc-cda42038-10").text().trim();

		const bedroomsText =
			$item.find("[data-testid='search-result-bedrooms']").text().trim() ||
			$item.find("[data-testid='search-result-bedrooms-title'] strong").text().trim();
		const bedrooms = bedroomsText ? bedroomsText.replace(/[^0-9]/g, "") : null;

		const title = address || $link.attr("aria-label") || "";

		if (link) {
			results.push({ link, title, price, bedrooms });
		}
	});

	return results;
}

async function dismissCookieDialogs(page) {
	await page
		.evaluate(() => {
			const buttons = Array.from(document.querySelectorAll("button"));
			const accept = buttons.find(
				(b) => b.textContent && b.textContent.toLowerCase().includes("allow all"),
			);
			if (accept) accept.click();
			const cookiebotBtn = document.getElementById(
				"CybotCookiebotDialogBodyLevelButtonLevelOptinAllowAll",
			);
			if (cookiebotBtn) cookiebotBtn.click();
		})
		.catch(() => {});
}

function extractPropertyId(link) {
	const match = link.match(/-(\d+)(?:\/|#|$)/);
	return match ? match[1] : null;
}

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
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		await dismissCookieDialogs(detailPage);
		await detailPage.waitForTimeout(1000);

		const htmlContent = await detailPage.content();
		let coords = { latitude: null, longitude: null };

		const propertyId = extractPropertyId(property.link);
		if (propertyId) {
			const apiCoords = await detailPage
				.evaluate(async (id) => {
					try {
						const response = await fetch(
							`https://www.purplebricks.co.uk/Api/Propertylisting/${id}`,
						);
						if (!response.ok) return null;
						const data = await response.json();
						return {
							latitude:
								data.latitude || (data.streetViewData ? data.streetViewData.latitude : null),
							longitude:
								data.longitude || (data.streetViewData ? data.streetViewData.longitude : null),
						};
					} catch (e) {
						return null;
					}
				}, propertyId)
				.catch(() => null);

			if (apiCoords && isValidCoord(apiCoords.latitude, apiCoords.longitude)) {
				coords = apiCoords;
			}
		}

		if (!isValidCoord(coords.latitude, coords.longitude)) {
			coords = extractCoordsFromHtml(htmlContent);
		}

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			coords.latitude,
			coords.longitude,
		);

		stats.totalScraped++;
		stats.totalSaved++;
		logger.property(
			pageNum,
			label,
			property.title,
			property.price,
			property.link,
			isRental,
			totalPages,
			"CREATED",
			coords.latitude,
			coords.longitude,
		);
	} catch (error) {
		logger.error(`Error scraping detail page`, error, pageNum, label);
	} finally {
		await detailPage.close();
	}
}

async function handleListingPage({ page, request }) {
	const { pageNum, totalPages, isRental, label } = request.userData;
	logger.page(pageNum, label, `Processing listing page ${request.url}`, totalPages);

	await page.waitForLoadState("domcontentloaded");
	await page.waitForSelector("[data-testid='results-list'] li", { timeout: 20000 }).catch(() => {});

	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	logger.page(
		pageNum,
		label,
		`Found ${properties.length} properties on page ${pageNum}`,
		totalPages,
	);

	for (const property of properties) {
		if (!property.link || !property.price) continue;
		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			stats.totalSaved++;
			logger.property(
				pageNum,
				label,
				property.title,
				property.price,
				property.link,
				isRental,
				totalPages,
				"UPDATED",
			);
		} else if (result.isExisting) {
			logger.property(
				pageNum,
				label,
				property.title,
				property.price,
				property.link,
				isRental,
				totalPages,
				"UNCHANGED",
			);
		}

		if (!result.isExisting && !result.error) {
			await scrapePropertyDetail(page.context(), property, isRental, pageNum, label, totalPages);
			await sleep(300);
		}
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
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(
				`Failed listing page: ${request.url}`,
				null,
				request.userData.pageNum,
				request.userData.label,
			);
		},
	});
}

async function scrapePurplebricks() {
	logger.step(`Starting Purplebricks scraper (Agent ${AGENT_ID})`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? Math.max(1, parseInt(args[0], 10)) : 1;

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	for (const propertyType of PROPERTY_TYPES) {
		const effectiveStart = Math.min(startPage, propertyType.totalPages);
		for (let pg = effectiveStart; pg <= propertyType.totalPages; pg++) {
			let url = propertyType.urlBase;
			url = url.replace(/page=\d+/, `page=${pg}`);

			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					totalPages: propertyType.totalPages,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.warn("No pages to scrape.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Purplebricks - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);

	// Enhanced Remove-Status Strategy
	if (startPage === 1) {
		logger.step(`Performing cleanup of removed properties...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step(`Partial run detected (startPage: ${startPage}). Skipping remove status update.`);
	}
}

(async () => {
	try {
		await scrapePurplebricks();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error:", err);
		process.exit(1);
	}
})();
