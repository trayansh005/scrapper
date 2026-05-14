// Homesea scraper using Playwright with Crawlee
// Agent ID: 217 - FIXED VERSION
// Usage: node backend/scraper-agent-217.js

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
};

// Updated Property Types with clean URL
const PROPERTY_TYPES = [
	{
		urlBase: "https://homesea.co.uk/property-search/in-hampshire/page",
		totalPages: 25,
		isRental: false,
		label: "SALES",
	},
];

// ============================================================================
// REQUEST HANDLER - FIXED
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;

	logger.page(pageNum, label, request.url);

	await page.waitForTimeout(3000);

	// Optional: Take screenshot for debugging
	// await page.screenshot({ path: `debug/homesea-page-${pageNum}.png` });

	const properties = await page.evaluate(() => {
		const items = [];

		// Multiple possible selectors for property cards
		const cardSelectors = [
			'article',
			'div[class*="property"]',
			'div[class*="listing"]',
			'section',
			'.card',
			'a[href*="/properties-for-sale/"]'
		];

		let propertyCards = [];

		for (const selector of cardSelectors) {
			const found = document.querySelectorAll(selector);
			if (found.length > 0) {
				propertyCards = Array.from(found);
				break;
			}
		}

		propertyCards.forEach(el => {
			try {
				// Find property link
				let linkEl = el.querySelector('a[href*="/properties-for-sale/"]') ||
					el.closest('a[href*="/properties-for-sale/"]');

				if (!linkEl) return;

				const link = linkEl.href;
				if (!link.includes('/properties-for-sale/')) return;

				// Title
				const titleEl = el.querySelector('h1, h2, h3, h4, strong, .title, .property-title');
				const title = titleEl ? titleEl.textContent.trim() : '';

				// Price
				let priceRaw = '';
				const priceSelectors = ['h5', '.price', '[class*="price"]', 'strong', '.text-price', '.cost'];
				for (const sel of priceSelectors) {
					const p = el.querySelector(sel);
					if (p && p.textContent.trim()) {
						priceRaw = p.textContent.trim();
						break;
					}
				}

				// Bedrooms
				let bedrooms = null;
				const fullText = (el.textContent || '').toLowerCase();
				const bedMatch = fullText.match(/(\d+)\s*(?:bed|bedroom|bdr)/i);
				if (bedMatch) bedrooms = bedMatch[1];

				if (link) {
					items.push({ link, title, priceRaw, bedrooms });
				}
			} catch (e) { }
		});

		return items;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`);

	// Process each property
	for (const property of properties) {
		if (!property.link) continue;

		const status = (property.title + " " + (property.priceRaw || "")).toLowerCase();
		if (status.includes("sold") || status.includes("stc") || status.includes("let") || status.includes("agreed")) {
			logger.warn(`Skipping sold property: ${property.link}`);
			continue;
		}
		if (isSoldProperty(property.priceRaw || "")) continue;

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.warn(`Skipping (no price): ${property.link}`);
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
		logger.step(`Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}/${pg}/`;   // ← Clean URL
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	logger.step(
		`Completed Homesea - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}`
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