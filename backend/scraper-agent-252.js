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

		// Wait for property listings to be visible
		await page.waitForSelector("[data-property-id], .property-item, .property-listing, li[class*='property'], article, .result, [class*='listing'], a[href*='/property/']", {
			timeout: 15000,
		}).catch(() => null);

		// Debug: Log actual page structure
		if (process.env.DEBUG_DOM === "1") {
			console.log("\n[DOM DEBUG] Analyzing page structure...");
			const structure = await page.evaluate(() => {
				const divs = Array.from(document.querySelectorAll("div[class*='property'], div[class*='result'], article, div[class*='listing']")).slice(0, 5);
				return divs.map((el, i) => ({
					index: i,
					tagName: el.tagName,
					classes: el.className,
					text: el.textContent.substring(0, 100),
					html: el.innerHTML.substring(0, 200)
				}));
			});
			console.log("Top containers found:", JSON.stringify(structure, null, 2));
		}

		const properties = await page.evaluate(() => {
			try {
				const results = [];
				const seenLinks = new Set();

				// Robsons-specific: Look for actual property cards/containers
				// They typically use specific class names or data attributes
				const propertyElements = Array.from(
					document.querySelectorAll(
						"a[href*='/property/'], [data-property], [class*='property-card'], [class*='search-result'], .result-item, li[class*='result']"
					)
				).filter((el) => {
					// Get the link - could be the element itself or within it
					const link = el.tagName === 'A' ? el.getAttribute("href") : el.querySelector("a")?.getAttribute("href");
					return link && /\/property\//.test(link);
				});

				if (window.__DEBUG_DOM === "1") {
					console.log(`[CLIENT] Found ${propertyElements.length} property elements`);
					propertyElements.slice(0, 3).forEach((el, i) => {
						const link = el.tagName === 'A' ? el.getAttribute("href") : el.querySelector("a")?.getAttribute("href");
						console.log(`  [#${i+1}] Link: ${link}, Container text length: ${el.textContent.length}, Classes: ${el.className}`);
					});
				}

				for (const element of propertyElements) {
					// Get the link
					let link = element.tagName === 'A' 
						? element.getAttribute("href") 
						: element.querySelector("a")?.getAttribute("href");
					
					if (!link) continue;

					// Ensure absolute URL
					if (!link.startsWith("http")) {
						link = new URL(link, window.location.origin).href;
					}

					if (seenLinks.has(link)) continue;
					seenLinks.add(link);

					// Find the closest container that holds the property info
					let container = element.tagName === 'A' ? element : element.querySelector("a");
					if (!container) container = element;
					
					for (let i = 0; i < 6; i++) {
						if (!container) break;
						const textLen = (container.textContent || "").length;
						// Containers should be 150-3000 chars (not too small like privacy notices)
						if (textLen > 150 && textLen < 3000) break;
						container = container.parentElement;
					}

					if (!container) continue;

					const containerText = container.textContent || "";
					
					// Skip privacy notices and other non-property content
					if (containerText.includes("We value your privacy") || containerText.includes("cookie policy")) {
						continue;
					}

					// ===== TITLE EXTRACTION =====
					// Try multiple strategies for title
					let title = "Property";
					
					// Strategy 1: Look for h2, h3 with address-like content
					let titleEl = container.querySelector("h2, h3, [class*='title'], [class*='address'], .property-title");
					if (titleEl && titleEl.textContent.length > 3 && titleEl.textContent.length < 200) {
						title = titleEl.textContent.trim();
					} else {
						// Strategy 2: Extract from link href or first significant text
						const linkText = link.split('/').filter(p => p.length > 2).join(', ');
						if (linkText) title = linkText.replace(/-/g, ' ');
					}

					// ===== PRICE EXTRACTION =====
					let price = null;
					const pricePatterns = [
						/£\s*([\d,]+)(?:\s*(?:pcm|per month))?/i,  // £250,000 or £1,500 pcm
						/(\d+(?:,\d{3})*)\s*(?:pcm|per month)\b/i,  // 1,500 pcm
					];

					let priceRaw = "";
					// Search in entire container but prefer smaller price elements
					const priceEls = Array.from(container.querySelectorAll("*")).filter(el => {
						const text = el.textContent || "";
						return /£|pcm|per month/.test(text) && text.length < 100;
					});

					for (const priceEl of priceEls) {
						const text = priceEl.textContent;
						for (const pattern of pricePatterns) {
							const match = text.match(pattern);
							if (match) {
								priceRaw = match[0];
								price = parseInt(match[1].replace(/,/g, ""), 10);
								if (!isNaN(price) && price > 100) break;  // Minimum £100
							}
						}
						if (price) break;
					}

					// ===== BEDROOM EXTRACTION =====
					let bedrooms = null;
					const bedroomEls = Array.from(container.querySelectorAll("*")).filter(el => {
						const text = (el.textContent || "").toLowerCase();
						return /\d+\s*(?:bed|bedroom)/.test(text) && text.length < 100;
					});

					for (const bedEl of bedroomEls) {
						const text = bedEl.textContent;
						const match = text.match(/(\d+)\s*(?:bed|bedroom|bedrooms)\b/i);
						if (match) {
							const num = parseInt(match[1], 10);
							if (num >= 1 && num <= 15) {
								bedrooms = num;
								break;
							}
						}
					}

					// ===== STATUS EXTRACTION =====
					let statusText = "";
					const statusEl = container.querySelector("[class*='status'], [class*='badge'], .tag, .label");
					if (statusEl) {
						statusText = statusEl.textContent.trim().toLowerCase();
					}

					// If no status element, check text for keywords
					if (!statusText && containerText) {
						if (/sold|let agreed|under offer/i.test(containerText)) {
							const match = containerText.match(/(sold|let agreed|under offer)/i);
							if (match) statusText = match[1].toLowerCase();
						}
					}

					if (window.__DEBUG_DOM === "1") {
						console.log(`  Property: ${title.substring(0, 30)} | Price: ${priceRaw} | Beds: ${bedrooms}`);
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

				// Remove duplicates
				const uniqueResults = [];
				const seen = new Set();
				for (const result of results) {
					if (!seen.has(result.link)) {
						seen.add(result.link);
						uniqueResults.push(result);
					}
				}

				return uniqueResults;
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
	if (properties.length > 0) {
		const nextPageNum = pageNum + 1;
		const separator = baseUrl.includes("?") ? "&" : "?";
		const nextPageUrl = `${baseUrl}${separator}page=${nextPageNum}`;

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
		const separator = type.baseUrl.includes("?") ? "&" : "?";
		const pageUrl = startPage > 1 ? `${type.baseUrl}${separator}page=${startPage}` : type.baseUrl;

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
