// Cherwell Property Services scraper using PlaywrightCrawler
// Agent ID: 256
// Usage: node backend/scraper-agent-256.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 256;
const logger = createAgentLogger(AGENT_ID);
const AGENT_NAME = "Cherwell Property Services";

const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.cherwellproperty.co.uk/properties/sales/status-available",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.cherwellproperty.co.uk/properties/lettings/status-available",
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
		// Allow stylesheets so JS can load; block only heavy resources
		if (["image", "font", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
}

async function fetchDetailPageHtml(browserPage, propertyUrl) {
	const detailPage = await browserPage.context().newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(propertyUrl, {
			waitUntil: "networkidle",
			timeout: 30000,
		});

		// Wait for Google Maps to load
		try {
			await detailPage
				.waitForFunction(() => document.documentElement.innerHTML.includes("google.maps.LatLng"), {
					timeout: 5000,
				})
				.catch(() => null);
		} catch (e) {
			// Maps might not exist
		}

		// Extra wait
		await new Promise((r) => setTimeout(r, 1000));

		const html = await detailPage.content();

		if (html && html.includes("LatLng")) {
			if (process.env.DEBUG_COORDS === "1") {
				console.log(`[DETAIL-256] ✅ Detail page has Google Maps coordinates`);
			}
		}

		return html;
	} catch (err) {
		if (process.env.DEBUG_COORDS === "1") {
			console.log(`[DETAIL-256] ❌ Error fetching ${propertyUrl}: ${err.message}`);
		}
		return null;
	} finally {
		await detailPage.close().catch(() => null);
	}
}

// ============================================================================
// COORDS (site does not expose them)
// ============================================================================

const coordsMap = new Map();

function attachJsonListener(page) {
	page.on("response", async (response) => {
		try {
			if (
				response.url().includes("/properties") &&
				response.headers()["content-type"]?.includes("json")
			) {
				const json = await response.json().catch(() => null);
				if (!json || !Array.isArray(json)) return;
				for (const item of json) {
					const lat = parseFloat(item.latitude || item.lat || null);
					const lng = parseFloat(item.longitude || item.lng || null);
					if (lat && lng && item.slug) {
						coordsMap.set(item.slug, { latitude: lat, longitude: lng });
					}
				}
			}
		} catch (_) {}
	});
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;
	logger.page(pageNum, label, request.url);

	attachJsonListener(page);

	try {
		await page.waitForSelector(".propList-inner", { timeout: 10000 });
	} catch (e) {
		logger.error("Property containers not found", e);
		return;
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const seen = new Set();

		document.querySelectorAll(".propList-inner").forEach((container) => {
			const link = container.querySelector(
				'a[href*="/properties/"][href$="/sales"], a[href*="/properties/"][href$="/lettings"]',
			);
			if (!link) return;

			const href = link.getAttribute("href");
			if (seen.has(href)) return;
			seen.add(href);

			const fullUrl = new URL(href, window.location.origin).href;

			// Title
			const title = container.querySelector("h4")?.innerText.trim() || "Unknown Property";

			// Price
			const priceRaw = container.querySelector(".propertyPrice")?.innerText.trim() || "";

			// Bedrooms from h6 (e.g. "3   1 ")
			let bedroomsRaw = null;
			const h6 = container.querySelector("h6");
			if (h6) {
				const text = h6.innerText.trim();
				const match = text.match(/^(\d+)/);
				if (match) bedroomsRaw = match[1];
			}

			if (fullUrl && priceRaw) {
				results.push({
					link: fullUrl,
					title: title.substring(0, 80),
					priceRaw,
					bedroomsRaw,
					statusText: container.innerText.toLowerCase(),
				});
			}
		});

		return results;
	});

	logger.page(pageNum, label, `Extracted ${properties.length} properties`);

	for (const prop of properties) {
		if (isSoldProperty(prop.statusText)) {
			logger.property(pageNum, label, prop.title, "", prop.link, isRental, "SKIPPED");
			continue;
		}

		if (processedUrls.has(prop.link)) continue;
		processedUrls.add(prop.link);

		const price = parsePrice(prop.priceRaw);
		const bedrooms = prop.bedroomsRaw ? parseInt(prop.bedroomsRaw, 10) : null;

		if (!price) {
			logger.property(pageNum, label, prop.title, "No price", prop.link, isRental, "SKIPPED");
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			prop.link,
			price,
			prop.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let action = "UNCHANGED";
		if (result.error) action = "ERROR";
		else if (result.updated) {
			counts.totalSaved++;
			action = "UPDATED";
		}

		let coords = null;
		if (!result.isExisting && !result.error) {
			// Try to find coordinates from API cache first
			const idMatch = prop.link.match(/\/properties\/(\d+)/);
			const propId = idMatch ? idMatch[1] : null;
			if (propId && coordsMap.has(propId)) {
				coords = coordsMap.get(propId);
			}

			// Fetch detail page if no coords
			let detailHtml = null;
			if (!coords) {
				logger.page(
					pageNum,
					label,
					`[Detail] Extracting coordinates from detail page for ${prop.title.substring(0, 30)}`,
				);
				detailHtml = await fetchDetailPageHtml(page, prop.link.trim());
			}

			const extractedCoords = await processPropertyWithCoordinates(
				prop.link,
				price,
				prop.title,
				bedrooms,
				AGENT_ID,
				isRental,
				detailHtml,
				coords?.latitude || null,
				coords?.longitude || null,
			);

			// Capture extracted coordinates
			if (extractedCoords?.latitude && extractedCoords?.longitude) {
				coords = extractedCoords;
				logger.page(
					pageNum,
					label,
					`✅ Coordinates extracted: ${coords.latitude}, ${coords.longitude}`,
				);
			}

			counts.totalScraped++;
			counts.totalSaved++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
			action = "CREATED";
		}

		logger.property(
			pageNum,
			label,
			prop.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			prop.link,
			isRental,
			action,
			coords?.latitude || null,
			coords?.longitude || null,
		);

		if (action !== "UNCHANGED") await sleep(800);
	}

	// Dynamic pagination: queue next page if properties found
	if (properties.length > 0) {
		const nextPageNum = pageNum + 1;
		const nextPageUrl = `${request.userData.baseUrl}/page-${nextPageNum}`;

		await crawler.addRequests([
			{
				url: nextPageUrl,
				userData: {
					pageNum: nextPageNum,
					isRental,
					label,
					baseUrl: request.userData.baseUrl,
				},
			},
		]);
		logger.page(pageNum, label, `Queued next page (${nextPageNum})`);
	} else {
		logger.page(pageNum, label, `No more properties found - stopping pagination for ${label}`);
	}
}
// ============================================================================
// CRAWLER & MAIN (unchanged)
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 60,
		requestHandlerTimeoutSecs: 120,
		preNavigationHooks: [({ page }) => blockNonEssentialResources(page)],
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox"],
				viewport: { width: 1920, height: 1080 },
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed request: ${request.url}`);
		},
	});
}

async function scrapeCherwell() {
	const scrapeStartTime = new Date();
	logger.step(`Starting ${AGENT_NAME} (Agent ${AGENT_ID})`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const requests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label}`);
		requests.push({
			url: type.baseUrl,
			userData: {
				pageNum: 1,
				isRental: type.isRental,
				label: type.label,
				baseUrl: type.baseUrl,
			},
		});
	}

	await crawler.run(requests);

	logger.step(
		`Completed ${AGENT_NAME} → Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved} ` +
			`(Sales: ${counts.savedSales}, Rentals: ${counts.savedRentals})`,
	);

	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	logger.step("Remove status updated.");
}

(async () => {
	try {
		await scrapeCherwell();
		logger.step("Success!");
		process.exit(0);
	} catch (err) {
		logger.error("Failed:", err);
		process.exit(1);
	}
})();
