// Belvoir scraper using Playwright with Crawlee
// Agent ID: 107
// Website: belvoir.co.uk
// Usage:
// node backend/scraper-agent-107.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

const AGENT_ID = 107;
const logger = createAgentLogger(AGENT_ID);

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

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
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// Wait for the property content to load
		await detailPage.waitForSelector(".property--card", { timeout: 30000 }).catch(() => {
			// Detail page might have a different structure
		});

		const html = await detailPage.content();

		// Save property to database
		// Belvoir detail pages are simple, coordinates usually extracted from HTML by processPropertyWithCoordinates
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			html,
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNumber, totalPages } = request.userData;
	logger.page(pageNumber, label, `Processing ${request.url}`, totalPages || null);

	try {
		// Navigate and wait for content
		await page.goto(request.url, { waitUntil: "networkidle", timeout: 60000 }).catch(() => {
			return page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });
		});

		// Wait for properties to load dynamically
		try {
			await page.waitForFunction(
				() => {
					const containers = document.querySelectorAll(
						".property--card, a[href*='/properties/'], article[data-property]",
					);
					return containers.length > 0;
				},
				{ timeout: 15000 },
			);
		} catch (e) {
			logger.warn(`Timeout waiting for properties on page ${pageNumber}`, null, pageNumber, label);
		}

		await page.waitForTimeout(2000); // Additional wait for rendering

		// Extract properties - try multiple selector patterns since site structure may vary
		const properties = await page.evaluate(() => {
			const items = [];
			const seenLinks = new Set();

			// Try multiple selector patterns for property containers
			let containers = Array.from(document.querySelectorAll(".property--card"));
			if (containers.length === 0) {
				containers = Array.from(
					document.querySelectorAll(
						"a[href*='/properties/'][href*='/sales/'], a[href*='/properties/'][href*='/lettings/']",
					),
				);
			}
			if (containers.length === 0) {
				containers = Array.from(
					document.querySelectorAll("[data-property], article[class*='property']"),
				);
			}

			for (const container of containers) {
				let link = null;
				let title = "";
				let priceText = "";
				let statusText = "";
				let bedrooms = null;

				// Get link - handle both direct <a> elements and containers
				if (container.tagName === "A") {
					link = container.href;
				} else {
					const linkEl =
						container.querySelector("a[href*='/properties/']") || container.querySelector("a");
					link = linkEl ? linkEl.href : null;
				}

				if (!link || seenLinks.has(link)) continue;
				seenLinks.add(link);

				// Extract title (try multiple selector patterns)
				const titleEl =
					container.querySelector(".property--card-title") ||
					container.querySelector("h2, h3, h4, [class*='title']");
				title = titleEl ? titleEl.innerText.trim() : "";

				// Extract price (try multiple patterns)
				const priceEl =
					container.querySelector(".property--card-price") ||
					container.querySelector("[class*='price']");
				priceText = priceEl ? priceEl.innerText.trim() : "";

				// If no price in dedicated element, search in full text
				if (!priceText) {
					const fullText = container.innerText || "";
					const priceMatch = fullText.match(/£[\d,]+/);
					priceText = priceMatch ? priceMatch[0] : "";
				}

				// Extract status
				const statusEl =
					container.querySelector(".property--card-status") ||
					container.querySelector("[class*='status']");
				statusText = statusEl ? statusEl.innerText.trim() : "";

				// Extract bedrooms from text
				const searchText = (title + " " + (container.innerText || "")).toLowerCase();
				const bedMatch = searchText.match(/(\d+)\s*bed/);
				if (bedMatch) {
					bedrooms = parseInt(bedMatch[1]);
				}

				if (link && priceText) {
					items.push({ link, title: title || "Property", priceText, bedrooms, statusText });
				}
			}
			return items;
		});

		// De-duplicate properties on the same page
		const uniqueProperties = [];
		const seenLinks = new Set();
		for (const p of properties) {
			if (!seenLinks.has(p.link)) {
				seenLinks.add(p.link);
				uniqueProperties.push(p);
			}
		}

		if (uniqueProperties.length === 0) {
			logger.warn(
				`No properties found on page ${pageNumber} - may need selector update`,
				pageNumber,
				label,
			);
		} else {
			logger.page(
				pageNumber,
				label,
				`Found ${uniqueProperties.length} unique properties`,
				totalPages || null,
			);
		}

		for (const property of uniqueProperties) {
			if (isSoldProperty(property.statusText || "")) {
				logger.property(
					pageNumber,
					label,
					property.title || "Property",
					property.priceText || "N/A",
					property.link,
					isRental,
					totalPages || null,
					"UNCHANGED",
				);
				continue;
			}

			const price = parsePrice(property.priceText);
			if (!price) {
				logger.property(
					pageNumber,
					label,
					property.title || "Property",
					"N/A",
					property.link,
					isRental,
					totalPages || null,
					"ERROR",
				);
				continue;
			}

			const updateResult = await updatePriceByPropertyURLOptimized(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";

			if (updateResult.updated) {
				stats.totalSaved++;
				propertyAction = "UPDATED";
			}

			if (!updateResult.isExisting && !updateResult.error) {
				propertyAction = "CREATED";
				logger.property(
					pageNumber,
					label,
					property.title || "Property",
					`£${price}`,
					property.link,
					isRental,
					totalPages || null,
					propertyAction,
				);
				await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
				// Delay between detail requests
				await new Promise((r) => setTimeout(r, 2000));
			} else {
				// Log UNCHANGED for existing non-updated properties
				logger.property(
					pageNumber,
					label,
					property.title || "Property",
					`£${price}`,
					property.link,
					isRental,
					totalPages || null,
					propertyAction,
				);
			}
		}
		// Delay between listing pages
		await new Promise((r) => setTimeout(r, 3000));
	} catch (error) {
		logger.error("Error in handleListingPage", error, pageNumber, label);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1, // Be polite
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

async function scrapeBelvoir() {
	logger.step(`Starting Belvoir scraper (Agent ${AGENT_ID})`);
	const startPageArg = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	const startPage = Number.isFinite(startPageArg) && startPageArg > 0 ? startPageArg : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);
	const crawler = createCrawler(browserWSEndpoint);

	// Belvoir now uses specific URLs for "In United Kingdom" which support pagination via /page/N/
	const PROPERTY_TYPES = [
		{
			baseUrl: "https://www.belvoir.co.uk/properties/for-sale/in-united-kingdom/",
			isRental: false,
			label: "SALES",
			totalPages: 37,
		},
		{
			baseUrl: "https://www.belvoir.co.uk/properties/to-rent/in-united-kingdom/",
			isRental: true,
			label: "RENTALS",
			totalPages: 24,
		},
	];

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		for (let p = Math.max(1, startPage); p <= type.totalPages; p++) {
			const url = p === 1 ? type.baseUrl : `${type.baseUrl}page/${p}/`;
			allRequests.push({
				url,
				userData: {
					pageNumber: p,
					isRental: type.isRental,
					label: type.label,
					totalPages: type.totalPages,
				},
			});
		}
	}

	await crawler.run(allRequests);

	logger.step(
		`Finished Belvoir - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn(`Partial run detected (startPage=${startPage}). Skipping updateRemoveStatus.`);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeBelvoir();
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
