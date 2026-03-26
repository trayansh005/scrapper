// Robsons Estate Agents scraper using Playwright with Crawlee
// Agent ID: 252
// Company: Robsons Estate Agents
// Usage:
// node backend/scraper-agent-252.js

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
		// Block only heavy resources, allow scripts and stylesheets that might load listings
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
// PROPERTY EXTRACTION FROM DOM
// ============================================================================

async function extractPropertiesFromDOM(page) {
	try {
		// Set debug flag on page if needed
		if (process.env.DEBUG_DOM === "1") {
			await page.evaluate(() => {
				window.__DEBUG_DOM = "1";
			});
		}

		// Robsons uses Property Hive WP plugin: ul.properties > li.property
		await page.waitForSelector("ul.properties li.property, li.property", {
			timeout: 15000,
		}).catch(() => null);

		const properties = await page.evaluate(() => {
			try {
				const results = [];
				const seenLinks = new Set();

				// Robsons uses Property Hive plugin: ul.properties > li.property
				const propertyCards = Array.from(
					document.querySelectorAll("ul.properties li.property, li.property")
				);

				if (window.__DEBUG_DOM === "1") {
					console.log(`[CLIENT] Found ${propertyCards.length} li.property cards`);
				}

				for (const card of propertyCards) {
					// ===== LINK EXTRACTION =====
					// .address a and .thumbnail a both point to the same property detail page
					const addressAnchor = card.querySelector(".address a, .details a");
					let link = addressAnchor ? addressAnchor.getAttribute("href") : null;

					if (!link) {
						// Fallback: grab any /property/ link inside the card
						const anyA = card.querySelector("a[href*='/property/']");
						link = anyA ? anyA.getAttribute("href") : null;
					}

					if (!link) continue;

					// Ensure absolute URL
					if (!link.startsWith("http")) {
						link = new URL(link, window.location.origin).href;
					}

					if (seenLinks.has(link)) continue;
					seenLinks.add(link);

					// ===== TITLE EXTRACTION =====
					// .address a contains the property address text e.g. "Rogers Ruff, Northwood"
					let title = "Property";
					const titleEl = card.querySelector(".address a, .address, h2, h3");
					if (titleEl) {
						const t = titleEl.textContent.trim();
						if (t.length > 2) title = t;
					}

					// ===== PRICE EXTRACTION =====
					// .price contains e.g. "Guide Price\n\u00a34,750,000" or "\u00a31,500 pcm"
					let price = null;
					let priceRaw = "";
					const priceEl = card.querySelector(".price");
					if (priceEl) {
						priceRaw = priceEl.textContent.trim();
						const match = priceRaw.match(/\u00a3\s*([\d,]+)/);
						if (match) {
							price = parseInt(match[1].replace(/,/g, ""), 10);
							if (isNaN(price) || price <= 0) price = null;
						}
					}

					// ===== BEDROOM EXTRACTION =====
					// .amenities li.bedrooms contains e.g. "6 Bedrooms"
					let bedrooms = null;
					const bedroomEl = card.querySelector(".amenities li.bedrooms, li.bedrooms");
					if (bedroomEl) {
						const match = bedroomEl.textContent.match(/(\d+)/);
						if (match) {
							const num = parseInt(match[1], 10);
							if (num >= 1 && num <= 20) bedrooms = num;
						}
					}

					// ===== STATUS EXTRACTION =====
					// .property-status contains "For Sale", "Sold STC", "Let Agreed", etc.
					let statusText = "";
					const statusEl = card.querySelector(".property-status");
					if (statusEl) {
						statusText = statusEl.textContent.trim().toLowerCase();
					}

					if (window.__DEBUG_DOM === "1") {
						console.log(`  Property: ${title.substring(0, 40)} | Price: ${priceRaw} | Beds: ${bedrooms} | Status: ${statusText}`);
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

				return results;
			} catch (e) {
				console.error("DOM extraction error:", e);
				return [];
			}
		});

		return properties;
	} catch (err) {
		logger.error(`Failed to extract properties: ${err.message}`, "", "");
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
		await detailPage.goto(propertyUrl, {
			waitUntil: "networkidle",
			timeout: 30000,
		});

		// Wait for page to load
		await new Promise((r) => setTimeout(r, 1500));

		const html = await detailPage.content();
		if (process.env.DEBUG_DETAIL === "1") {
			logger.step(`[Detail] Fetched ${propertyUrl.substring(0, 50)}, size: ${html.length}`);
		}

		return html;
	} catch (err) {
		logger.error(`Error fetching detail page: ${err.message}`, "", "");
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

	// Extract properties from DOM
	const properties = await extractPropertiesFromDOM(page);
	logger.page(pageNum, label, `Found ${properties.length} properties`);

	let processedCount = 0;
	let skippedCount = 0;

	// Process each property
	for (let propIdx = 0; propIdx < properties.length; propIdx++) {
		const property = properties[propIdx];

		if (!property.link) {
			skippedCount++;
			continue;
		}

		const statusText = (property.statusText || "").trim().toLowerCase();
		const price = property.price || parsePrice(property.priceRaw);

		// Skip sold / under offer / let agreed properties
		if (statusText && isSoldProperty(statusText)) {
			logger.property(
				pageNum,
				label,
				property.title?.substring(0, 40) || "N/A",
				price ? formatPriceDisplay(price, isRental) : "N/A",
				property.link,
				isRental,
				"SKIPPED"
			);
			skippedCount++;
			continue;
		}

		// Skip if already processed in this run
		if (processedUrls.has(property.link)) {
			skippedCount++;
			continue;
		}
		processedUrls.add(property.link);

		// Skip if no price found
		if (!price || price <= 0) {
			logger.property(
				pageNum,
				label,
				property.title?.substring(0, 40) || "N/A",
				"N/A",
				property.link,
				isRental,
				"SKIPPED"
			);
			skippedCount++;
			continue;
		}

		// ===== UPDATE OR CREATE IN DB =====
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
			let detailHtml = null;

			// Fetch detail page to extract coordinates
			logger.page(pageNum, label, `[Detail] Fetching coordinates for ${property.title?.substring(0, 30)}`);
			detailHtml = await fetchDetailPageHtml(page, property.link.trim());

			// Process with coordinates extraction
			const extractedCoords = await processPropertyWithCoordinates(
				property.link.trim(),
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
				pageNum,
				label,
				property.title?.substring(0, 40) || "N/A",
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				propertyAction,
				extractedCoords?.latitude || null,
				extractedCoords?.longitude || null
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500);
			}

			processedCount++;
		} else if (result.error) {
			propertyAction = "ERROR";
			logger.property(
				pageNum,
				label,
				property.title?.substring(0, 40) || "N/A",
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				propertyAction
			);
		} else {
			logger.property(
				pageNum,
				label,
				property.title?.substring(0, 40) || "N/A",
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				propertyAction
			);
		}
	}

	if (process.env.DEBUG_EXTRACT === "1") {
		console.log(`[P${pageNum} SUMMARY] processed=${processedCount}, skipped=${skippedCount}, total=${properties.length}`);
	}

	// ===== DYNAMIC PAGINATION =====
	// Robsons uses WordPress-style pagination: /page/N/ before the query string
	// e.g. https://robsonsweb.com/search-results/?department=residential-sales
	//   -> https://robsonsweb.com/search-results/page/2/?department=residential-sales
	if (properties.length > 0) {
		const nextPageNum = pageNum + 1;
		const urlObj = new URL(baseUrl);
		const pathBase = urlObj.pathname.replace(/\/$/, ""); // strip trailing slash
		const nextPageUrl = `${urlObj.origin}${pathBase}/page/${nextPageNum}/${urlObj.search}`;

		await crawler.addRequests([{
			url: nextPageUrl,
			userData: { pageNum: nextPageNum, isRental, label, baseUrl }
		}]);
		logger.page(pageNum, label, `Queued next page (${nextPageNum})`);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`, "", "");
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
		logger.step(`Queueing ${type.label} (starting page ${startPage} - dynamic pagination)`);

		let pageUrl;
		if (startPage > 1) {
			// Build first URL with page number for partial runs
			const urlObj = new URL(type.baseUrl);
			const pathBase = urlObj.pathname.replace(/\/$/, "");
			pageUrl = `${urlObj.origin}${pathBase}/page/${startPage}/${urlObj.search}`;
		} else {
			pageUrl = type.baseUrl;
		}

		allRequests.push({
			url: pageUrl,
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
	} else {
		logger.warn("No requests to process.", "", "");
	}

	logger.step(
		`Completed Robsons Estate Agents - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, LETTINGS: ${counts.savedRentals}`);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.", "", "");
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
		logger.error("Fatal error", err, "");
		process.exit(1);
	}
})();