// Hamptons scraper using Playwright with Crawlee
// Agent ID: 108
// Website: hamptons.co.uk
// Usage:
// node backend/scraper-agent-108.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 108;

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
		// Block unnecessary resources
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// Important: Hamptons might need a small wait for Homeflow object to be populated
		await detailPage.waitForTimeout(2000);

		// Extract coordinates directly from page context for better accuracy
		const detailData = await detailPage.evaluate(() => {
			let lat = null;
			let lng = null;

			// Priority 1: Homeflow object (most accurate)
			if (typeof Homeflow !== "undefined" && Homeflow.get) {
				const prop = Homeflow.get("property");
				if (prop && prop.lat && prop.lng) {
					lat = prop.lat;
					lng = prop.lng;
				}
			}

			// Priority 2: GA4 property object (sometimes buggy on Hamptons)
			if ((lat === null || lng === null) && typeof propertyObject !== "undefined") {
				lat = parseFloat(propertyObject.ga4_property_latitude);
				lng = parseFloat(propertyObject.ga4_property_longitude);

				// If they are exactly the same and not null, it might be a bug
				if (lat === lng && lat !== null) {
					lat = null;
					lng = null;
				}
			}

			return {
				lat,
				lng,
				html: document.documentElement.innerHTML,
			};
		});

		// Save property to database
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			detailData.html,
			detailData.lat,
			detailData.lng,
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (error) {
		console.error(` Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNumber } = request.userData;
	console.log(`\n📍 Loading [${label}] Page ${pageNumber}: ${request.url}`);

	try {
		await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });
		await page.waitForTimeout(2000);

		// Wait for property cards
		await page.waitForSelector("article.property-card", { timeout: 30000 }).catch(() => {
			console.log(`   ⚠️ No properties found on page ${pageNumber}`);
		});

		// Extract properties
		const properties = await page.evaluate(() => {
			const containers = Array.from(document.querySelectorAll("article.property-card"));
			const items = [];

			for (const container of containers) {
				const linkEl = container.querySelector("a.property-card__link");
				const rawHref = linkEl ? linkEl.getAttribute("href") : null;
				const link = rawHref ? new URL(rawHref, window.location.origin).href : null;

				const priceText =
					container.querySelector(".property-card__price")?.textContent?.trim() || "";
				const title = container.querySelector(".property-card__title")?.textContent?.trim() || "";
				const statusText = container.textContent || "";

				let bedrooms = null;
				const bedEl = container.querySelector(
					".property-card__bedbath .property-card__bedbath-item",
				);
				if (bedEl) {
					const m = bedEl.textContent.match(/(\d+)/);
					if (m) bedrooms = parseInt(m[1]);
				}

				if (link && priceText) {
					items.push({ link, title, priceText, bedrooms, statusText });
				}
			}
			return items;
		});

		// De-duplicate properties on the same page (Hamptons often repeats featured properties)
		const uniqueProperties = [];
		const seenLinks = new Set();
		for (const p of properties) {
			if (!seenLinks.has(p.link)) {
				seenLinks.add(p.link);
				uniqueProperties.push(p);
			}
		}

		console.log(
			`   ✅ Found ${uniqueProperties.length} unique properties on [${label}] Page ${pageNumber}`,
		);

		for (const property of uniqueProperties) {
			if (isSoldProperty(property.statusText || "")) {
				console.log(`   ⏭️ Skipping sold/let: ${property.title}`);
				continue;
			}

			const price = parsePrice(property.priceText);
			if (!price) {
				console.log(`   ⚠️ Price not found for: ${property.title}`);
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

			if (updateResult.updated) {
				stats.totalSaved++;
			}

			if (!updateResult.isExisting && !updateResult.error) {
				console.log(`   🆕 New property: ${property.title} - £${price}`);
				await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
				await new Promise((r) => setTimeout(r, 2000));
			} else {
				// Don't log if it was an existing property to avoid noise
			}
		}

		// Add delay between listing pages to avoid 429
		await new Promise((r) => setTimeout(r, 3000));
	} catch (error) {
		console.error(`❌ Error in handleListingPage: ${error.message}`);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(` Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeHamptons() {
	console.log(` Starting Hamptons Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const PROPERTY_TYPES = [
		{
			baseUrl: "https://www.hamptons.co.uk/properties/sales/status-available",
			isRental: false,
			label: "SALES",
			totalPages: 213,
		},
		{
			baseUrl: "https://www.hamptons.co.uk/properties/lettings/status-available",
			isRental: true,
			label: "RENTALS",
			totalPages: 91,
		},
	];

	for (const type of PROPERTY_TYPES) {
		const requests = [];
		for (let p = 1; p <= type.totalPages; p++) {
			const url = p === 1 ? type.baseUrl : `${type.baseUrl}/page-${p}`;
			requests.push({
				url,
				userData: {
					pageNumber: p,
					isRental: type.isRental,
					label: type.label,
				},
			});
		}
		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n Finished Hamptons - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeHamptons();
		await updateRemoveStatus(AGENT_ID);
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
