// Balgores Property scraper using Playwright with Crawlee
// Agent ID: 254
// Usage:
// node backend/scraper-agent-254.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 254;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.balgoresproperty.co.uk/properties-for-sale/essex-and-kent/",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.balgoresproperty.co.uk/properties-to-rent/essex-and-kent/",
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
		// Block only heavy resources, allow scripts and stylesheets that might load the iframe
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
// STRAPI COORDINATE MAP (populated on listing pages)
// ============================================================================

// Map from property slug → { latitude, longitude }
const strapiCoordsMap = new Map();
// Per-page captured /properties payload from live network responses.
const strapiListingsByPage = new WeakMap();
const strapiListenerAttached = new WeakSet();

function extractSlugFromPropertyUrl(url) {
	if (!url) return null;
	const cleanPath = url
		.replace(/^https?:\/\/[^/]+/i, "")
		.replace(/^\/property\/(residential|commercial)\//i, "")
		.replace(/\/?$/, "")
		.split("?")[0]
		.trim();
	return cleanPath || null;
}

function normalizeStrapiProperty(item) {
	const link = item?.property_url || item?.url || null;
	const slug = item?.slug || extractSlugFromPropertyUrl(link);
	const latitude =
		item?.latitude ||
		item?.lat ||
		item?.Latitude ||
		item?.map?.lat ||
		item?.geolocation?.lat ||
		null;
	const longitude =
		item?.longitude ||
		item?.lng ||
		item?.Longitude ||
		item?.map?.lng ||
		item?.geolocation?.lng ||
		null;

	if (slug && latitude && longitude) {
		strapiCoordsMap.set(slug, {
			latitude: parseFloat(latitude),
			longitude: parseFloat(longitude),
		});
	}

	return {
		link,
		title: item?.display_address || item?.title || "Property",
		price: item?.price ?? null,
		bedrooms: item?.bedroom ?? item?.bedrooms ?? null,
		status: item?.status || "",
		slug,
		latitude: latitude ? parseFloat(latitude) : null,
		longitude: longitude ? parseFloat(longitude) : null,
	};
}

function attachStrapiListener(page) {
	if (strapiListenerAttached.has(page)) return;
	strapiListenerAttached.add(page);

	page.on("response", async (response) => {
		try {
			const url = response.url();
			if (
				url.includes("balgores-strapi.q.starberry.com") &&
				url.includes("/properties?") &&
				!url.includes("/count")
			) {
				const json = await response.json().catch(() => null);
				if (!json || !Array.isArray(json)) return;

				const normalized = json.map(normalizeStrapiProperty).filter((item) => item.link);
				if (normalized.length > 0) {
					strapiListingsByPage.set(page, normalized);
				}
			}
		} catch (_) {
			// Silently ignore non-JSON or blocked responses
		}
	});
}

async function waitForStrapiListings(page, timeoutMs = 10000) {
	const startedAt = Date.now();
	while (Date.now() - startedAt < timeoutMs) {
		const listings = strapiListingsByPage.get(page);
		if (Array.isArray(listings) && listings.length > 0) {
			return listings;
		}
		await sleep(250);
	}
	return [];
}

async function fetchDetailPageHtml(browserPage, propertyUrl) {
	const detailPage = await browserPage.context().newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(propertyUrl, {
			waitUntil: "networkidle",
			timeout: 30000,
		});

		// Wait for locrating iframe to load
		try {
			await detailPage
				.waitForFunction(() => document.documentElement.innerHTML.includes("locrating"), {
					timeout: 5000,
				})
				.catch(() => null);
		} catch (e) {
			// iframe might not exist
		}

		// Extra wait
		await new Promise((r) => setTimeout(r, 1000));

		const html = await detailPage.content();

		// Check if iframe is present
		if (html && html.includes("locrating")) {
			if (process.env.DEBUG_DETAIL_HTML === "1" || process.env.DEBUG_COORDS === "1") {
				console.log(
					`[DETAIL] ✅ Detail page fetched for ${propertyUrl.substring(0, 50)}, HTML size: ${html.length}, contains locrating: true`,
				);
			}
		} else if (html) {
			if (process.env.DEBUG_COORDS === "1") {
				console.log(
					`[DETAIL] ⚠️ Detail page fetched but NO locrating iframe found (size: ${html.length})`,
				);
			}
		}

		return html;
	} catch (err) {
		console.log(`[DETAIL] ❌ Error fetching ${propertyUrl}: ${err.message}`);
		return null;
	} finally {
		await detailPage.close().catch(() => null);
	}
}

// ============================================================================
// REQUEST HANDLER - LISTING PAGE
// ============================================================================

// ============================================================================
// IMPROVED PROPERTY EXTRACTION (Replace inside handleListingPage)
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, baseUrl } = request.userData;
	logger.page(pageNum, label, request.url);
	attachStrapiListener(page);

	// Primary path: consume live Strapi payload
	let properties = await waitForStrapiListings(page, 20000);

	if (properties.length === 0) {
		logger.warn("No Strapi listings captured. Falling back to improved DOM extraction.", pageNum, label);

		try {
			await page.waitForSelector("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']", {
				timeout: 15000,
			});
		} catch (e) {
			logger.error("Listing container not found", e, pageNum, label);
		}

		properties = await page.evaluate(() => {
			try {
				const results = [];
				const seenLinks = new Set();

				const propertyLinks = Array.from(
					document.querySelectorAll("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']")
				).filter((link) => {
					const href = link.getAttribute("href");
					return href &&
						(href.includes("/property-for-sale/") || href.includes("/property-to-rent/")) &&
						!href.includes("#") &&
						!href.includes("related");
				});

				for (const link of propertyLinks) {
					const href = link.getAttribute("href");
					if (!href || seenLinks.has(href)) continue;
					seenLinks.add(href);

					let container = link;
					for (let i = 0; i < 6; i++) {
						container = container.parentElement;
						if (!container) break;
						if ((container.textContent || "").length > 150) break;
					}

					if (!container) continue;

					const fullLink = href.startsWith("http")
						? href
						: new URL(href, window.location.origin).href;

					// === IMPROVED TITLE EXTRACTION ===
					let title = "Property";
					const titleEl = container.querySelector("h1, h2, h3, .property-title, .address");
					if (titleEl) {
						title = titleEl.textContent.trim();
					}

					// === IMPROVED PRICE EXTRACTION ===
					let priceRaw = "";
					const priceMatch = container.textContent.match(/£[0-9,]+/);
					if (priceMatch) {
						priceRaw = priceMatch[0];
					}

					// === IMPROVED BEDROOM EXTRACTION (This was the main bug) ===
					let bedrooms = null;
					const bedroomRegex = /(\d+)\s*(?:bedroom|bedrooms|bed)\b/i;

					// Priority 1: Look in elements that are likely to contain bedroom info (badges, specs)
					const specElements = Array.from(container.querySelectorAll('div, span, p, strong, li, .spec, .feature, .bedroom'));
					for (const el of specElements) {
						const text = (el.textContent || "").trim();
						if (text.length > 100) continue; // skip very long text
						const match = text.match(bedroomRegex);
						if (match) {
							const num = parseInt(match[1], 10);
							if (num >= 1 && num <= 15) {  // realistic bedroom range
								bedrooms = num;
								break;
							}
						}
					}

					// Priority 2: Fallback to full container text
					if (!bedrooms) {
						const fullTextMatch = container.textContent.match(bedroomRegex);
						if (fullTextMatch) {
							const num = parseInt(fullTextMatch[1], 10);
							if (num >= 1 && num <= 15) {
								bedrooms = num;
							}
						}
					}

					// Priority 3: Look for "X bed" pattern (common shorthand)
					if (!bedrooms) {
						const bedShortMatch = container.textContent.match(/(\d+)\s*bed\b/i);
						if (bedShortMatch) {
							const num = parseInt(bedShortMatch[1], 10);
							if (num >= 1 && num <= 15) {
								bedrooms = num;
							}
						}
					}

					// === STATUS EXTRACTION ===
					const statusCandidates = Array.from(
						container.querySelectorAll("[class*='status'], [class*='badge'], .tag, .label")
					).map(el => (el.textContent || "").trim())
						.filter(text => text.length > 0 && text.length < 60);

					const statusText = statusCandidates.join(" ");

					results.push({
						link: fullLink,
						title: title.replace(/\s+/g, " ").trim(),
						priceRaw,
						bedrooms,           // Now properly extracted
						bedText: bedrooms ? `${bedrooms} bedroom` : "", // for backward compatibility
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
	}

	logger.page(pageNum, label, `Found ${properties.length} properties`);

	// Process each property
	for (const property of properties) {
		if (!property.link) continue;

		const statusText = (property.status || property.statusText || "").trim().toLowerCase();
		const price = typeof property.price === "number"
			? property.price
			: parsePrice(property.priceRaw);

		// Final validation for bedrooms
		let bedrooms = property.bedrooms;
		if (!bedrooms && property.bedText) {
			const match = (property.bedText || "").match(/(\d+)/);
			if (match) {
				const num = parseInt(match[1], 10);
				if (num >= 1 && num <= 15) bedrooms = num;
			}
		}

		// Skip sold / under offer / let agreed properties
		if (statusText && isSoldProperty(statusText)) {
			logger.property(pageNum, label, property.title?.substring(0, 40),
				price ? formatPriceDisplay(price, isRental) : "N/A",
				property.link, isRental, "SKIPPED");
			continue;
		}

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		if (!price) {
			logger.page(pageNum, label, `Skipped: No price found - ${property.title?.substring(0, 40)}`);
			continue;
		}

		// === Update or Create in DB ===
		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,           // ← Now correctly passed
			AGENT_ID,
			isRental
		);

		let propertyAction = "UNCHANGED";
		let coords = property.latitude && property.longitude
			? { latitude: property.latitude, longitude: property.longitude }
			: null;

		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const slug = property.slug || extractSlugFromPropertyUrl(property.link);

			if (!coords && slug) {
				coords = strapiCoordsMap.get(slug) || null;
			}

			let detailHtml = null;
			if (!coords) {
				logger.page(pageNum, label, `[Detail] Fetching coordinates for ${property.title?.substring(0, 30)}`);
				detailHtml = await fetchDetailPageHtml(page, property.link.trim());
			}

			const extractedCoords = await processPropertyWithCoordinates(
				property.link.trim(),
				price,
				property.title,
				bedrooms,                    // ← Fixed here too
				AGENT_ID,
				isRental,
				detailHtml,
				coords?.latitude || null,
				coords?.longitude || null
			);

			if (extractedCoords?.latitude && extractedCoords?.longitude) {
				coords = extractedCoords;
			}

			counts.totalScraped++;
			counts.totalSaved++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;

			propertyAction = "CREATED";
		} else if (result.error) {
			propertyAction = "ERROR";
		}

		logger.property(
			pageNum, label,
			property.title?.substring(0, 40) || "N/A",
			formatPriceDisplay(price, isRental),
			property.link,
			isRental,
			propertyAction,
			coords?.latitude || null,
			coords?.longitude || null
		);

		if (propertyAction !== "UNCHANGED") {
			await sleep(500);
		}
	}

	// Dynamic pagination
	if (properties.length > 0) {
		const nextPageNum = pageNum + 1;
		const pageParam = nextPageNum > 1 ? `?page=${nextPageNum}` : "";
		const nextPageUrl = `${request.userData.baseUrl}${pageParam}`;

		await crawler.addRequests([{
			url: nextPageUrl,
			userData: { pageNum: nextPageNum, isRental, label, baseUrl: request.userData.baseUrl }
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
				attachStrapiListener(page);
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
		// Only queue the first page - pagination will be dynamic
		logger.step(`Queueing ${type.label} (page 1 - dynamic pagination)`);
		allRequests.push({
			url: type.baseUrl,
			userData: {
				pageNum: 1,
				isRental: type.isRental,
				label: type.label,
				baseUrl: type.baseUrl,
			},
		});
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
