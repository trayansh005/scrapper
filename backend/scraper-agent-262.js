// REDAC Strattons Property scraper using Playwright with Crawlee
// Agent ID: 262
// Updated 2026-03: REDAC Strattons - sales and rentals with "Under Offer" exclusion
// Usage: node backend/scraper-agent-262.js [optional startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR); // Keep ERROR level for cleaner logs

const AGENT_ID = 262;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://redacstrattons.com/property-for-sale/?location&office&type&min_price&max_price&min_beds&exclude_sold=true",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://redacstrattons.com/property-to-rent/?location&office&type&min_price&max_price&min_beds&exclude_let=true",
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

// Check if property status indicates it should be skipped (Sold, Under Offer, Let)
function isSkippableProperty(statusText) {
	if (!statusText) return false;
	const text = statusText.toLowerCase().trim();
	return (
		text.includes("under offer") ||
		text.includes("sold") ||
		text.includes("let") ||
		text.includes("rented")
	);
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

		// Try to wait for property links (non-blocking, continue if timeout)
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

				// Extract title/address
				const titleEl = container.querySelector("h2, h3, [class*='title'], [class*='address']");
				const title = titleEl?.textContent?.trim() || "Property";

				// Extract status badge (e.g., "Under Offer", "Sold", "New Listing")
				const badgeEl = container.querySelector(
					"[class*='badge'], [class*='status'], [class*='tag'], span[style*='background']"
				);
				let statusText = badgeEl?.textContent?.trim() || "";

				// If no badge found, check for text directly in container
				if (!statusText) {
					const textNodes = Array.from(container.childNodes).filter(
						(n) => n.nodeType === Node.TEXT_NODE && n.textContent.trim().length > 0
					);
					const potentialStatus = textNodes
						.map((n) => n.textContent.trim())
						.find(
							(t) =>
								t.toLowerCase().includes("under") ||
								t.toLowerCase().includes("sold") ||
								t.toLowerCase().includes("let") ||
								t.toLowerCase().includes("new listing")
						);
					if (potentialStatus) statusText = potentialStatus;
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

		// Skip properties marked as "Under Offer"
		if (isSkippableProperty(prop.statusText)) {
			const reason = prop.statusText || "Filtered";
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
			logger.page(pageNum, label, `Skipped: ${reason} - ${prop.title.substring(0, 40)}`);
			continue;
		}

		// Also check with standard sold/let check
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

	// Check pagination - look for next page link
	const hasNextPage = await page.evaluate(() => {
		const nextBtn = document.querySelector("a[data-v-app] [rel='next'], a[aria-label*='next'], .pagination a.next");
		return nextBtn !== null;
	});

	if (hasNextPage && pageNum < totalPages) {
		const nextPageNum = pageNum + 1;
		const pageParam = nextPageNum > 1 ? `&page=${nextPageNum}` : "";
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

async function scrapeRedacStrattons() {
	logger.step("Starting REDAC Strattons Property scraper (Agent 262)");

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
		// Estimate ~20 pages unless pagination info is found
		const estimatedPages = 20;
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
		await scrapeRedacStrattons();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
