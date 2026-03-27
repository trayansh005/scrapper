// Drewery Property Consultants scraper using Playwright with Crawlee
// Agent ID: 261
// Updated 2026-03: Drewery - sales and rentals property extraction
// Usage: node backend/scraper-agent-261.js [optional startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR); // Keep ERROR level for cleaner logs

const AGENT_ID = 261;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://drewery.co.uk/property-for-sale/property/any-bed/all-location?exclude=1",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://drewery.co.uk/property-to-rent/property/any-bed/all-location?exclude=1",
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
		// Block only heavy resources to speed up loading
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
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, baseUrl, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		// Wait for page to load network idle first
		await page.waitForLoadState("networkidle", { timeout: 30000 }).catch(() => null);

		// Try to wait for property links (non-blocking)
		await page.waitForSelector("a[href*='/property/']", {
			timeout: 5000,
		}).catch(() => {
			logger.page(pageNum, label, "Property links selector timeout - proceeding with evaluation");
		});
	} catch (e) {
		logger.warn("Error waiting for page load", e?.message, pageNum, label);
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seen = new Set();

			// Find all property links - more flexible selector
			const propertyLinks = Array.from(
				document.querySelectorAll("a[href*='/property/']")
			).filter((a) => {
				const href = a.getAttribute("href");
				return href && href.includes("/property/") && !href.includes("#");
			});

			// Process each unique property link
			for (const link of propertyLinks) {
				const href = link.getAttribute("href");
				if (!href || seen.has(href)) continue;
				seen.add(href);

				// Find the property card container - traverse up the DOM
				let container = link;
				let depth = 0;
				while (container && depth < 8) {
					container = container.parentElement;
					if (!container) break;
					const fullText = container.textContent || "";
					// Look for a container with substantial content
					if (fullText.length > 150 && fullText.length < 5000) break;
					depth++;
				}

				if (!container) continue;

				const fullLink = href.startsWith("http")
					? href
					: new URL(href, window.location.origin).href;

				// Extract title/address - usually the first heading or link text
				const titleEl = container.querySelector("h2, h3, [class*='title'], [class*='address']");
				let title = titleEl?.textContent?.trim() || link.textContent?.trim() || "Property";

				// Clean up title - remove extra whitespace
				title = title.replace(/\s+/g, " ").trim();
				if (title.length > 200) {
					title = title.substring(0, 200);
				}

				// Extract status badges (e.g., "NEW", "Sold", "Let")
				const badgeEl = container.querySelector(
					"[class*='badge'], [class*='status'], [class*='tag'], span[style*='background']"
				);
				let statusText = badgeEl?.textContent?.trim() || "";

				// If no badge found, check the container's immediate children for status text
				if (!statusText) {
					const statusCandidates = Array.from(container.querySelectorAll("*"))
						.filter(el => {
							const text = el.textContent?.trim() || "";
							return text.length < 30 && (
								text.toLowerCase().includes("new") ||
								text.toLowerCase().includes("sold") ||
								text.toLowerCase().includes("let") ||
								text.toLowerCase().includes("under offer")
							);
						})
						.map(el => el.textContent?.trim());
					if (statusCandidates.length > 0) {
						statusText = statusCandidates[0];
					}
				}

				// Extract price - look for £ symbol
				let priceRaw = "";
				const textContent = container.textContent || "";
				const priceMatch = textContent.match(/£[\d,]+(?:[.,]\d+)?/);
				if (priceMatch) {
					priceRaw = priceMatch[0];
				}

				// Extract bedrooms - look for "X bed" pattern
				let bedText = "";
				const bedMatch = textContent.match(/(\d+)\s+bed/i);
				if (bedMatch) {
					bedText = bedMatch[0];
				}

				results.push({
					link: fullLink,
					title,
					priceRaw,
					bedText,
					statusText,
				});
			}

			// Deduplicate by link
			const uniqueResults = [];
			const seenLinks = new Set();
			for (const result of results) {
				if (!seenLinks.has(result.link)) {
					seenLinks.add(result.link);
					uniqueResults.push(result);
				}
			}

			return uniqueResults;
		} catch (e) {
			console.error("Error in page.evaluate:", e);
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const prop of properties) {
		if (!prop.link) continue;

		// Skip sold or let properties
		if (isSoldProperty(prop.statusText || "")) {
			logger.property(
				pageNum,
				label,
				prop.title.substring(0, 40),
				prop.priceRaw ? formatPriceDisplay(parsePrice(prop.priceRaw), isRental) : "N/A",
				prop.link,
				isRental,
				totalPages,
				"SKIPPED",
			);
			logger.page(pageNum, label, `Skipped: Sold/Let - ${prop.title.substring(0, 40)}`);
			continue;
		}

		// Skip if already processed
		if (processedUrls.has(prop.link)) continue;
		processedUrls.add(prop.link);

		const price = parsePrice(prop.priceRaw);
		let bedrooms = null;
		if (prop.bedText) {
			const match = prop.bedText.match(/\d+/);
			if (match) bedrooms = parseInt(match[0], 10);
		}

		if (!price) {
			logger.page(pageNum, label, `Skipped: No price - ${prop.title.substring(0, 40)}`);
			continue;
		}

		let coords = { latitude: null, longitude: null };

		const result = await updatePriceByPropertyURLOptimized(
			prop.link,
			price,
			prop.title,
			bedrooms,
			AGENT_ID,
			isRental
		);

		let propertyAction = "UNCHANGED";

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		// For new properties, visit detail page to extract coordinates
		if (!result.isExisting && !result.error) {
			try {
				logger.page(
					pageNum,
					label,
					`[Detail] Fetching coordinates for ${prop.title.substring(0, 30)}`
				);
				const detailPage = await page.context().newPage();
				await blockNonEssentialResources(detailPage);

				await detailPage.goto(prop.link.trim(), {
					waitUntil: "networkidle",
					timeout: 30000,
				});

				// Try to extract latitude/longitude from page content or scripts
				const detailHtml = await detailPage.content();

				await processPropertyWithCoordinates(
					prop.link.trim(),
					price,
					prop.title,
					bedrooms,
					AGENT_ID,
					isRental,
					detailHtml,
					coords.latitude,
					coords.longitude
				);

				await detailPage.close().catch(() => null);

				counts.totalScraped++;
				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
				propertyAction = "CREATED";
			} catch (err) {
				logger.error(`Detail page error for ${prop.link}`, err, pageNum, label);
				propertyAction = "ERROR";
			}
		} else if (result.error) {
			propertyAction = "ERROR";
		}

		logger.property(
			pageNum,
			label,
			prop.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			prop.link,
			isRental,
			totalPages,
			propertyAction,
			coords.latitude && coords.longitude
				? `${coords.latitude.toFixed(6)}, ${coords.longitude.toFixed(6)}`
				: "NO_COORDS"
		);

		// Sleep only if we did actual work (CREATE or UPDATE)
		if (propertyAction !== "UNCHANGED" && propertyAction !== "SKIPPED") {
			await sleep(600);
		} else {
			await sleep(150);
		}
	}

	// Check for next page link using pagination
	const hasNextPage = await page.evaluate(() => {
		// Look for next page link - check for href with page parameter
		const nextBtn = document.querySelector("a[href*='page=']") ||
		                Array.from(document.querySelectorAll('a')).find(a => 
		                  a.textContent?.trim().includes('»') || 
		                  a.textContent?.trim().includes('Next')
		                );
		return nextBtn !== null;
	});

	if (hasNextPage && pageNum < totalPages) {
		const nextPageNum = pageNum + 1;
		const pageParam = `&page=${nextPageNum}`;
		const nextPageUrl = baseUrl + pageParam;

		await crawler.addRequests([
			{
				url: nextPageUrl,
				userData: {
					pageNum: nextPageNum,
					isRental,
					label,
					baseUrl,
					totalPages,
				},
			},
		]);
		logger.page(pageNum, label, `Queued next page (${nextPageNum}/${totalPages})`);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 60,
		requestHandlerTimeoutSecs: 180,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
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

async function scrapeDreweryProperty() {
	logger.step("Starting Drewery Property Consultants scraper (Agent 261)");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0], 10) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to Browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} listings`);
		// Estimate ~10 pages for Drewery (they show 1-24 of 37, ~2 pages)
		const estimatedPages = 10;
		for (let pg = Math.max(1, startPage); pg <= estimatedPages; pg++) {
			const pageParam = pg > 1 ? `&page=${pg}` : "";
			allRequests.push({
				url: `${type.baseUrl}${pageParam}`,
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: type.label,
					baseUrl: type.baseUrl,
					totalPages: estimatedPages,
				},
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	} else {
		logger.warn("No requests to process.");
	}

	logger.step(
		`Completed - Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved} ` +
			`(Sales: ${counts.savedSales}, Rentals: ${counts.savedRentals})`
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run — skipping updateRemoveStatus.");
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeDreweryProperty();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
