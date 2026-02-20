// ============================================================================
// AGENT 244 - DISABLED
// ============================================================================
//
// Status: BROKEN - Website structure has changed
// Reason: mypropertybox.co.uk no longer renders property listings in the DOM.
//
// Issues Found (Feb 16, 2026):
// 1. The selector 'a[href*="property-details.php"]' finds 0 results
// 2. The HTML contains "198 properties found for sale" but no actual listings
// 3. URLs like /results-gallery.php?location=... no longer work
// 4. New URLs (/results-gallery.php?section=sales&ddm_order=2) exist but don't
//    render property data in the DOM
// 5. Properties appear to load dynamically via JavaScript/API but the actual
//    property links never appear in the rendered HTML
// 6. Page times out waiting for 'a[href*="property-details.php"]' selector
//
// To fix:
// - Investigate if site uses a REST/GraphQL API for property data
// - Check Network tab in browser DevTools for API endpoints
// - Review if listings are behind authentication or rate-LIMITED
// - Consider if site has migrated to a different URL structure entirely
//
// Last tested: 2026-02-16 - Site not scrapable in current form
// ============================================================================

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

const AGENT_ID = 244;
const BASE_URL = "https://www.mypropertybox.co.uk";
const AGENT_DISABLED = true; // ← SET TO FALSE TO RE-ENABLE

log.setLevel(log.LEVELS.ERROR);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

const PROPERTY_TYPES = [
	{
		type: "sales",
		// OLD (broken): "https://www.mypropertybox.co.uk/results-gallery.php?location=&section=sales&minPrice=&maxPrice=&pppw_max=99999&minBedrooms=&maxBedrooms="
		// NEW: Must use ddm_order=2 parameter instead of location/price/bedroom params
		// Status: Properties not rendering - DO NOT USE
		urlBase: "https://www.mypropertybox.co.uk/results-gallery.php?section=sales&ddm_order=2",
		totalPages: 17, // Updated from 16 (was 191 properties / 12 per page)
		isRental: false,
		label: "SALES",
	},
	{
		type: "lettings",
		// Status: Properties not rendering - DO NOT USE
		urlBase: "https://www.mypropertybox.co.uk/results-gallery.php?section=lets&ddm_order=2",
		totalPages: 17, // Updated from 26 - needs verification
		isRental: true,
		label: "RENTALS",
	},
];

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
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

async function scrapePropertyDetail(browserContext, property, isRental) {
	await sleep(500);

	const detailPage = await browserContext.newPage();

	try {
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.propertyUrl, {
			waitUntil: "domcontentloaded",
			timeout: 45000,
		});
		await detailPage.waitForTimeout(500);

		const html = await detailPage.content();

		const latLonMatch = html.match(
			/google\.maps\.LatLng\(\s*([-+]?\d*\.?\d+),\s*([-+]?\d*\.?\d+)\s*\)/,
		);
		const lat = latLonMatch ? parseFloat(latLonMatch[1]) : null;
		const lon = latLonMatch ? parseFloat(latLonMatch[2]) : null;

		await processPropertyWithCoordinates(
			property.propertyUrl,
			property.price,
			property.address,
			property.bedrooms,
			AGENT_ID,
			isRental,
			html,
			lat,
			lon,
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (err) {
		log.warn(`Failed to process detail page: ${property.propertyUrl} (${err.message})`);
	} finally {
		await detailPage.close();
	}
}

async function run() {
	if (AGENT_DISABLED) {
		console.log(`\n⛔ Agent ${AGENT_ID} is DISABLED\n`);
		console.log(`Reason: mypropertybox.co.uk site structure has changed.`);
		console.log(`Property listings no longer render in the DOM.\n`);
		console.log(`Last working: Unknown`);
		console.log(`Disabled: 2026-02-16\n`);
		return;
	}

	console.log(`\n🚀 Starting My Property Box scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 300,
		navigationTimeoutSecs: 60,
		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		async requestHandler({ page, request, log }) {
			const { isRental, pageNum, label } = request.userData;

			log.info(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Wait for the gallery items to load
			await page
				.waitForSelector('a[href*="property-details.php"]', { timeout: 30000 })
				.catch(() => log.warn(`⚠️ Property links not found on page ${pageNum}`));

			const properties = await page.evaluate((baseUrl) => {
				const results = [];
				// Select links that go to details and have an ID
				const links = Array.from(document.querySelectorAll('a[href*="property-details.php"]'));

				// Use a Set to avoid duplicates (sometimes there's an image link and a text link)
				const seenIds = new Set();

				links.forEach((link) => {
					const href = link.getAttribute("href");
					const idMatch = href.match(/id=(\d+)/);
					if (!idMatch) return;

					const id = idMatch[1];
					if (seenIds.has(id)) return;
					seenIds.add(id);

					// Check for SSTC badge - skip if found
					const hasSSTCBadge = link.querySelector('img[src="/images/sstc-badge.png"]') !== null;
					if (hasSSTCBadge) return;

					// Extract data from the link's text content (usually contains address and price)
					const text = link.innerText || "";

					// Address is usually the first line or bold text
					const address = text.split("\n")[0].trim() || "Address not found";

					// Price extraction
					const priceMatch = text.replace(/,/g, "").match(/£(\d+)/);
					const price = priceMatch ? parseInt(priceMatch[1], 10) : 0;

					// Bedrooms extraction
					const bedMatch = text.match(/(\d+)\s*Bedroom/i);
					const bedrooms = bedMatch ? parseInt(bedMatch[1], 10) : null;

					const propertyUrl = href.startsWith("http")
						? href
						: baseUrl + (href.startsWith("/") ? "" : "/") + href;

					results.push({
						propertyUrl,
						price,
						address,
						bedrooms,
					});
				});
				return results;
			}, BASE_URL);

			log.info(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 4;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.price) {
							log.info(`⏩ Skipping property (no price): ${property.propertyUrl}`);
							return;
						}

						const result = await updatePriceByPropertyURLOptimized(
							property.propertyUrl,
							property.price,
							property.address,
							property.bedrooms,
							AGENT_ID,
							isRental,
						);

						if (result.updated) {
							stats.totalSaved++;
						}

						if (!result.isExisting && !result.error) {
							log.info(`Scraping details: ${property.propertyUrl}`);
							await scrapePropertyDetail(page.context(), property, isRental);
						}
					}),
				);
			}
		},
	});

	for (const config of PROPERTY_TYPES) {
		const requests = [];
		for (let pg = 1; pg <= config.totalPages; pg++) {
			const url = new URL(config.urlBase);
			url.searchParams.set("page", pg);
			requests.push({
				url: url.toString(),
				uniqueKey: `${config.type}-page-${pg}`,
				userData: {
					isRental: config.isRental,
					pageNum: pg,
					label: config.label,
				},
			});
		}
		console.log(`🏠 Enqueuing ${config.label} (${config.totalPages} pages)`);
		await crawler.addRequests(requests);
	}

	await crawler.run();

	for (const config of PROPERTY_TYPES) {
		await updateRemoveStatus(AGENT_ID, config.isRental ? 1 : 0);
	}

	console.log(`Crawl finished. Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`);
}

run().catch(console.error);
