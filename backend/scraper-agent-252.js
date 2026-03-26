// Robsons Estate Agents scraper using Playwright with Crawlee
// Agent ID: 252
// Improved version - March 2026

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 252;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://robsonsweb.com/search-results/?department=residential-sales",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://robsonsweb.com/search-results/?department=residential-lettings",
		isRental: true,
		label: "RENTALS",
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITIES
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const type = route.request().resourceType();
		if (["image", "font", "media"].includes(type)) return route.abort();
		return route.continue();
	});
}

function getBrowserlessEndpoint() {
	return process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;
}

// ============================================================================
// IMPROVED DOM EXTRACTION
// ============================================================================

async function extractPropertiesFromDOM(page) {
	try {
		await page.waitForSelector('a[href*="/property/"]', { timeout: 20000 }).catch(() => null);

		const properties = await page.evaluate(() => {
			const results = [];
			const seen = new Set();

			const linkElements = Array.from(document.querySelectorAll('a[href*="/property/"]'))
				.filter(a => /\/property\/[a-z0-9-]+\/?$/.test(a.getAttribute("href")));

			for (const a of linkElements) {
				let link = a.href;
				if (seen.has(link)) continue;
				seen.add(link);

				// Best container
				let container = a.closest('div, article, li, section') || a.parentElement;
				for (let i = 0; i < 5; i++) {
					if (!container) break;
					const txt = container.textContent || "";
					if (txt.length > 150 && (/£|\d+\s*bedroom|pcm/i.test(txt))) break;
					container = container.parentElement;
				}

				const fullText = (container.textContent || "").toLowerCase();

				if (/we value your privacy|cookie|consent|gdpr/i.test(fullText)) continue;

				// Title
				let title = a.textContent.trim() || "";
				if (!title || title.length < 10) {
					const titleEl = container.querySelector('h1, h2, h3, strong, [class*="title"], [class*="address"]');
					title = titleEl ? titleEl.textContent.trim() : "";
				}
				if (!title || title.length < 8) {
					const slug = link.split('/').filter(Boolean).pop() || "";
					title = slug.replace(/-/g, ' ')
					           .replace(/\b\w/g, l => l.toUpperCase());
				}

				// Price
				let price = null;
				let priceRaw = "";
				const priceMatch = (container.textContent || "").match(/£\s*([\d,]+)/i);
				if (priceMatch) {
					priceRaw = priceMatch[0];
					price = parseInt(priceMatch[1].replace(/,/g, ''), 10);
				}

				// Bedrooms
				let bedrooms = null;
				const bedMatch = (container.textContent || "").match(/(\d+)\s*(?:bed|bedroom|bedrooms)\b/i);
				if (bedMatch) bedrooms = parseInt(bedMatch[1], 10);

				// Status
				let statusText = "";
				const statusMatch = (container.textContent || "").match(/(sold|let agreed|under offer|sold stc|reserved)/i);
				if (statusMatch) statusText = statusMatch[1].toLowerCase();

				results.push({ link, title, priceRaw, price, bedrooms, statusText });
			}

			return results;
		});

		return properties;
	} catch (err) {
		logger.error(`Failed to extract properties: ${err.message}`);
		return [];
	}
}

// ============================================================================
// DETAIL PAGE FOR COORDINATES
// ============================================================================

async function fetchDetailPageHtml(contextPage, url) {
	const detailPage = await contextPage.context().newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(url, { waitUntil: "networkidle", timeout: 30000 });
		await sleep(1200);
		return await detailPage.content();
	} catch (e) {
		logger.error(`Detail page error for ${url}: ${e.message}`);
		return null;
	} finally {
		await detailPage.close().catch(() => {});
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, baseUrl } = request.userData;
	logger.page(pageNum, label, request.url);

	const properties = await extractPropertiesFromDOM(page);
	logger.page(pageNum, label, `Found ${properties.length} properties`);

	for (const prop of properties) {
		if (processedUrls.has(prop.link)) continue;
		processedUrls.add(prop.link);

		const price = prop.price || parsePrice(prop.priceRaw);
		const status = (prop.statusText || "").toLowerCase();

		if (status && isSoldProperty(status)) {
			logger.property(pageNum, label, prop.title.substring(0, 45), "N/A", prop.link, isRental, "SKIPPED");
			continue;
		}

		if (!price || price < 100) {
			logger.property(pageNum, label, prop.title.substring(0, 45), "N/A", prop.link, isRental, "SKIPPED");
			continue;
		}

		try {
			const result = await updatePriceByPropertyURLOptimized(
				prop.link, price, prop.title, prop.bedrooms, AGENT_ID, isRental
			);

			let action = "UNCHANGED";

			if (result.updated) action = "UPDATED";
			if (!result.isExisting && !result.error) {
				const detailHtml = await fetchDetailPageHtml(page, prop.link);

				const coords = await processPropertyWithCoordinates(
					prop.link, price, prop.title, prop.bedrooms, AGENT_ID, isRental, detailHtml
				);

				counts.totalScraped++;
				counts.totalSaved++;
				if (isRental) counts.savedRentals++; else counts.savedSales++;

				action = "CREATED";

				logger.property(pageNum, label, prop.title.substring(0, 45),
					formatPriceDisplay(price, isRental), prop.link, isRental, action,
					coords?.latitude, coords?.longitude);

				await sleep(700);
			} else if (result.error) {
				action = "ERROR";
				logger.property(pageNum, label, prop.title.substring(0, 45),
					formatPriceDisplay(price, isRental), prop.link, isRental, "DB_ERROR");
			} else {
				logger.property(pageNum, label, prop.title.substring(0, 45),
					formatPriceDisplay(price, isRental), prop.link, isRental, action);
			}
		} catch (dbErr) {
			logger.error(`DB error for ${prop.link}: ${dbErr.message}`);
			logger.property(pageNum, label, prop.title.substring(0, 45), formatPriceDisplay(price, isRental), prop.link, isRental, "DB_ERROR");
		}
	}

	// Pagination - Robsons uses /page/N/
	if (properties.length > 8) {
		const nextNum = pageNum + 1;
		const dept = new URL(baseUrl).searchParams.get('department');
		const nextUrl = `https://robsonsweb.com/search-results/page/${nextNum}/?department=${dept}`;

		await crawler.addRequests([{
			url: nextUrl,
			userData: { pageNum: nextNum, isRental, label, baseUrl }
		}]);
		logger.page(pageNum, label, `Queued page ${nextNum}`);
	}
}

// ============================================================================
// CRAWLER
// ============================================================================

function createCrawler(wsEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 240,
		preNavigationHooks: [({ page }) => blockNonEssentialResources(page)],
		launchContext: {
			launchOptions: {
				browserWSEndpoint: wsEndpoint,
				args: ["--no-sandbox"],
			}
		},
		requestHandler: handleListingPage,
		failedRequestHandler: ({ request }) => logger.error(`Failed: ${request.url}`),
	});
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeRobsonsEstateAgents() {
	logger.step("Starting Robsons Estate Agents scraper (improved)...");

	const startPage = process.argv[2] ? parseInt(process.argv[2]) : 1;
	const isPartial = startPage > 1;
	const startTime = new Date();

	const ws = getBrowserlessEndpoint();
	logger.step(`Browserless: ${ws.split("?")[0]}`);

	const crawler = createCrawler(ws);

	const requests = PROPERTY_TYPES.map(type => {
		let url = type.baseUrl;
		if (startPage > 1) {
			const dept = new URL(type.baseUrl).searchParams.get('department');
			url = `https://robsonsweb.com/search-results/page/${startPage}/?department=${dept}`;
		}
		return {
			url,
			userData: { pageNum: startPage, isRental: type.isRental, label: type.label, baseUrl: type.baseUrl }
		};
	});

	await crawler.run(requests);

	logger.step(`Finished - Scraped: ${counts.totalScraped} | Saved: ${counts.totalSaved} (Sales: ${counts.savedSales}, Rentals: ${counts.savedRentals})`);

	if (!isPartial) {
		await updateRemoveStatus(AGENT_ID, startTime).catch(e => logger.error("updateRemoveStatus failed", e));
	}
}

(async () => {
	try {
		await scrapeRobsonsEstateAgents();
		logger.step("✅ All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();