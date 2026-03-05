// Balgores Property scraper using Playwright with Crawlee
// Agent ID: 1
// Usage:
// node backend/scraper-agent-1.js
// node backend/scraper-agent-1.js 2 (start from page 2)

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

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 1;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.balgoresproperty.co.uk/properties-for-sale/essex-and-kent/",
		totalPages: 20,
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.balgoresproperty.co.uk/properties-to-rent/essex-and-kent/",
		totalPages: 15,
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
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
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
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	await sleep(700);

	const detailPage = await browserContext.newPage();

	try {
		// Balgores is a Gatsby/React SPA - coordinates are loaded asynchronously
		// via the Strapi CMS API. We intercept the API response to capture lat/lng.
		let strapiCoords = null;

		// Listen for Strapi API responses containing property data
		detailPage.on("response", async (response) => {
			try {
				const url = response.url();
				// Match the Strapi property endpoint (e.g. /properties?slug=...)
				if (
					url.includes("balgores-strapi.q.starberry.com") &&
					(url.includes("/properties") || url.includes("/property"))
				) {
					const json = await response.json().catch(() => null);
					if (!json) return;

					// Handle both array and single object responses
					const item = Array.isArray(json) ? json[0] : json;
					if (!item) return;

					// Strapi stores coords in various places
					const lat =
						item.latitude ||
						item.lat ||
						item.Latitude ||
						item.map?.lat ||
						item.geolocation?.lat ||
						null;
					const lng =
						item.longitude ||
						item.lng ||
						item.Longitude ||
						item.map?.lng ||
						item.geolocation?.lng ||
						null;

					if (lat && lng) {
						strapiCoords = { latitude: parseFloat(lat), longitude: parseFloat(lng) };
					}
				}
			} catch (_) {
				// Silently ignore response parsing errors
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 90000,
		});

		// Wait longer for the React/Gatsby app to hydrate and fire API requests
		await detailPage.waitForTimeout(3000);

		// If Strapi API intercept already gave us coordinates, use them
		if (strapiCoords) {
			return { coords: strapiCoords };
		}

		// Fallback: parse the fully hydrated HTML for embedded coordinate patterns
		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);

		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
			},
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER - LISTING PAGE
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		// Wait for property listings container
		await page.waitForSelector("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']", {
			timeout: 15000,
		});
	} catch (e) {
		logger.error("Listing container not found", e, pageNum, label);
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();

			// Find all property links - look for links containing property sale/rent pages
			const propertyLinks = Array.from(
				document.querySelectorAll("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']"),
			).filter((link) => {
				// Ensure it's a main property link (not a related or additional link)
				const href = link.getAttribute("href");
				return (
					href &&
					(href.includes("/property-for-sale/") || href.includes("/property-to-rent/")) &&
					!href.includes("#")
				);
			});

			// Deduplicate and extract property data
			const propertySet = new Set();
			for (const link of propertyLinks) {
				const href = link.getAttribute("href");
				if (href && !seenLinks.has(href)) {
					seenLinks.add(href);
					propertySet.add(href);
				}
			}

			for (const link of propertySet) {
				// Find the property card container for this link
				const linkEl = document.querySelector(`a[href="${link}"]`);
				if (!linkEl) continue;

				// Traverse up to find the property card container
				let container = linkEl;
				for (let i = 0; i < 5; i++) {
					container = container.parentElement;
					if (!container) break;
					// Get all text from the container
					const fullText = container.textContent || "";
					if (fullText.length > 100) break; // Found a reasonable container
				}

				if (!container) continue;

				const fullLink = link.startsWith("http") ? link : new URL(link, window.location.origin).href;

				// Extract title - usually comes from h2 or h3 with property address
				let title = "Property";
				const titleEl = container.querySelector("h2, h3");
				if (titleEl) {
					title = titleEl.textContent.trim();
				}

				// Extract price - look for £ symbol
				let priceRaw = "";
				const textContent = container.textContent;
				const priceMatch = textContent.match(/£[\d,]+(,\d{3})?/);
				if (priceMatch) {
					priceRaw = priceMatch[0];
				}

				// Extract bedrooms - look for "X bedroom" pattern in text
				let bedText = "";
				const bedMatch = textContent.match(/(\d+)\s+bedroom/i);
				if (bedMatch) {
					bedText = bedMatch[0];
				}

				// Extract status - look for status badges like "UNDER OFFER", "SOLD STC"
				const statusText = container.textContent;

				results.push({
					link: fullLink,
					title,
					priceRaw,
					bedText,
					statusText,
				});
			}

			// Remove duplicates
			const uniqueResults = [];
			const seenResultLinks = new Set();
			for (const result of results) {
				if (!seenResultLinks.has(result.link)) {
					seenResultLinks.add(result.link);
					uniqueResults.push(result);
				}
			}

			return uniqueResults;
		} catch (e) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link) continue;

		// Skip sold properties
		if (isSoldProperty(property.statusText || "")) continue;

		// Skip if already processed
		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		let bedrooms = null;
		const bedMatch = property.bedText.match(/\d+/);
		if (bedMatch) bedrooms = parseInt(bedMatch[0]);

		if (!price) {
			logger.page(
				pageNum,
				label,
				`Skipped: No price found - ${property.title.substring(0, 40)}`,
				totalPages,
			);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "UNCHANGED";

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			// New property - scrape detail page for coordinates
			const detail = await scrapePropertyDetail(page.context(), property);

			await processPropertyWithCoordinates(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				null,
				detail?.coords?.latitude || null,
				detail?.coords?.longitude || null,
			);

			counts.totalScraped++;
			counts.totalSaved++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
			propertyAction = "CREATED";
		} else if (result.error) {
			propertyAction = "ERROR";
		}

		logger.property(
			pageNum,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			property.link,
			isRental,
			totalPages,
			propertyAction,
		);

		// Only sleep if we did real work (CREATE or UPDATE)
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
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeBalgoresProperty() {
	logger.step("Starting Balgores Property scraper...");

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} (${type.totalPages} pages)`);
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			// Build page URL - Balgores uses query parameters for pagination
			const pageParam = pg > 1 ? `?page=${pg}` : "";
			allRequests.push({
				url: `${type.baseUrl}${pageParam}`,
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: type.label,
					totalPages: type.totalPages,
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
		`Completed Balgores Property - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, LETTINGS: ${counts.savedRentals}`);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeBalgoresProperty();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
