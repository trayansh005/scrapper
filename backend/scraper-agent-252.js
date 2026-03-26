// Robsons Estate Agents scraper using Playwright with Crawlee
// Agent ID: 252
// Company: Robsons Estate Agents
// Usage: node backend/scraper-agent-252.js

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
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
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
// IMPROVED PROPERTY EXTRACTION FROM DOM
// ============================================================================

async function extractPropertiesFromDOM(page) {
	try {
		if (process.env.DEBUG_DOM === "1") {
			await page.evaluate(() => { window.__DEBUG_DOM = "1"; });
		}

		// Wait for listings
		await page.waitForSelector('a[href*="/property/"]', { timeout: 15000 }).catch(() => null);

		const properties = await page.evaluate(() => {
			const results = [];
			const seenLinks = new Set();

			// More targeted: Get all property links first
			const linkElements = Array.from(
				document.querySelectorAll('a[href*="/property/"]')
			).filter(el => {
				const href = el.getAttribute("href");
				return href && /\/property\/[a-z0-9-]+\/?$/.test(href);
			});

			if (window.__DEBUG_DOM === "1") {
				console.log(`[CLIENT] Found ${linkElements.length} potential property links`);
			}

			for (const linkEl of linkElements) {
				let link = linkEl.getAttribute("href");
				if (!link) continue;

				if (!link.startsWith("http")) {
					link = new URL(link, window.location.origin).href;
				}

				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				// Find the best container (property card)
				let container = linkEl.closest('div[class*="card"], div[class*="property"], article, li, section') || 
				                linkEl.parentElement;

				// Walk up to find rich content container
				for (let i = 0; i < 6; i++) {
					if (!container) break;
					const textLen = (container.textContent || "").length;
					if (textLen > 180 && textLen < 4000) {
						// Prefer container that has price or bedroom info
						if (/£|\d+\s*bedroom|guide price|pcm/i.test(container.textContent)) {
							break;
						}
					}
					container = container.parentElement;
				}

				if (!container) container = linkEl;

				const containerText = container.textContent || "";

				// Skip noise (privacy banner, etc.)
				if (/we value your privacy|cookie policy|consent/i.test(containerText)) {
					continue;
				}

				// ===== TITLE EXTRACTION =====
				let title = "Property";
				const titleSelectors = ['h1', 'h2', 'h3', '[class*="title"]', '[class*="address"]', 'strong'];
				
				for (const sel of titleSelectors) {
					const el = container.querySelector(sel);
					if (el && el.textContent.trim().length > 5) {
						title = el.textContent.trim();
						break;
					}
				}

				// Fallback from URL slug
				if (title === "Property" || title.length < 8) {
					const slug = link.split('/').filter(p => p.length > 3).pop() || "";
					title = slug.replace(/-/g, ' ')
					           .replace(/\b\w/g, c => c.toUpperCase());
				}

				// ===== PRICE EXTRACTION =====
				let price = null;
				let priceRaw = "";

				const priceMatch = containerText.match(/£\s*([\d,]+)(?:\s*(?:guide price|pcm|per month|pw))?/i);
				if (priceMatch) {
					priceRaw = priceMatch[0];
					const numStr = priceMatch[1].replace(/,/g, '');
					price = parseInt(numStr, 10);
				}

				// ===== BEDROOMS EXTRACTION =====
				let bedrooms = null;
				const bedMatch = containerText.match(/(\d+)\s*(?:bed|bedroom|bedrooms)\b/i);
				if (bedMatch) {
					const num = parseInt(bedMatch[1], 10);
					if (num >= 1 && num <= 15) bedrooms = num;
				}

				// ===== STATUS EXTRACTION =====
				let statusText = "";
				const statusMatch = containerText.match(/(sold|let agreed|under offer|sold stc|let|reserved)/i);
				if (statusMatch) {
					statusText = statusMatch[1].toLowerCase();
				}

				if (window.__DEBUG_DOM === "1") {
					console.log(`  Property: ${title.substring(0, 40)} | Price: ${priceRaw || 'N/A'} | Beds: ${bedrooms || 'N/A'}`);
				}

				results.push({
					link,
					title: title.replace(/\s+/g, " ").trim(),
					priceRaw,
					price,
					bedrooms,
					statusText,
				});
			}

			// Remove any duplicates
			return results.filter((v, i, a) => a.findIndex(t => t.link === v.link) === i);
		});

		return properties;
	} catch (err) {
		logger.error(`Failed to extract properties: ${err.message}`);
		return [];
	}
}

// ============================================================================
// FETCH DETAIL PAGE FOR COORDINATES
// ============================================================================

async function fetchDetailPageHtml(browserPage, propertyUrl) {
	const detailPage = await browserPage.context().newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(propertyUrl, { waitUntil: "networkidle", timeout: 30000 });
		await new Promise(r => setTimeout(r, 1200));

		return await detailPage.content();
	} catch (err) {
		logger.error(`Error fetching detail page: ${err.message}`);
		return null;
	} finally {
		await detailPage.close().catch(() => null);
	}
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, baseUrl } = request.userData;
	logger.page(pageNum, label, request.url);

	const properties = await extractPropertiesFromDOM(page);
	logger.page(pageNum, label, `Found ${properties.length} properties`);

	let processedCount = 0;
	let skippedCount = 0;

	for (const property of properties) {
		if (!property.link) {
			skippedCount++;
			continue;
		}

		const statusText = (property.statusText || "").toLowerCase();
		const price = property.price || parsePrice(property.priceRaw);

		// Skip sold/let agreed properties
		if (statusText && isSoldProperty(statusText)) {
			logger.property(pageNum, label, property.title?.substring(0, 40) || "N/A",
				price ? formatPriceDisplay(price, isRental) : "N/A", property.link, isRental, "SKIPPED");
			skippedCount++;
			continue;
		}

		if (processedUrls.has(property.link)) {
			skippedCount++;
			continue;
		}
		processedUrls.add(property.link);

		if (!price || price <= 100) {
			logger.property(pageNum, label, property.title?.substring(0, 40) || "N/A", "N/A", property.link, isRental, "SKIPPED");
			skippedCount++;
			continue;
		}

		// Update or create in DB
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental
		);

		let propertyAction = "UNCHANGED";

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const detailHtml = await fetchDetailPageHtml(page, property.link);

			const extractedCoords = await processPropertyWithCoordinates(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
				detailHtml,
				null,
				null
			);

			counts.totalScraped++;
			counts.totalSaved++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;

			propertyAction = "CREATED";

			logger.property(
				pageNum, label,
				property.title?.substring(0, 40) || "N/A",
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				propertyAction,
				extractedCoords?.latitude || null,
				extractedCoords?.longitude || null
			);

			processedCount++;
			await sleep(600);
		} else if (result.error) {
			propertyAction = "ERROR";
			logger.property(pageNum, label, property.title?.substring(0, 40) || "N/A",
				formatPriceDisplay(price, isRental), property.link, isRental, "ERROR");
		} else {
			logger.property(pageNum, label, property.title?.substring(0, 40) || "N/A",
				formatPriceDisplay(price, isRental), property.link, isRental, propertyAction);
		}
	}

	// ==================== DYNAMIC PAGINATION (Fixed) ====================
	if (properties.length > 0) {
		const nextPageNum = pageNum + 1;

		// Robsons uses clean path pagination: /search-results/page/2/
		let nextPageUrl;
		const urlObj = new URL(baseUrl);
		const department = urlObj.searchParams.get('department');

		if (baseUrl.includes('/search-results/')) {
			const basePath = baseUrl.replace(/\/page\/\d+\//, '/').replace(/\/$/, '');
			nextPageUrl = `${basePath}/page/${nextPageNum}/?department=${department}`;
		} else {
			nextPageUrl = `${baseUrl}${baseUrl.includes('?') ? '&' : '?'}page=${nextPageNum}`;
		}

		await crawler.addRequests([{
			url: nextPageUrl,
			userData: { pageNum: nextPageNum, isRental, label, baseUrl }
		}]);

		logger.page(pageNum, label, `Queued next page → ${nextPageNum}`);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [async ({ page }) => { await blockNonEssentialResources(page); }],
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeRobsonsEstateAgents() {
	logger.step("Starting Robsons Estate Agents scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} (starting page ${startPage})`);

		let startUrl = type.baseUrl;
		if (startPage > 1) {
			const dept = new URL(type.baseUrl).searchParams.get('department');
			startUrl = `https://robsonsweb.com/search-results/page/${startPage}/?department=${dept}`;
		}

		allRequests.push({
			url: startUrl,
			userData: {
				pageNum: startPage,
				isRental: type.isRental,
				label: type.label,
				baseUrl: type.baseUrl,
			},
		});
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	}

	logger.step(`Completed Robsons Estate Agents - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`);
	logger.step(`Breakdown → SALES: ${counts.savedSales} | LETTINGS: ${counts.savedRentals}`);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeRobsonsEstateAgents();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();