const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 231;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
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

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
}

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
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		const detailData = await detailPage.evaluate(() => {
			try {
				const data = { lat: null, lng: null, bedrooms: null };

				// Try to find coordinates in script tags or text
				const html = document.documentElement.innerHTML;
				const latMatch = html.match(/lat["']?\s*:\s*(-?\d+\.\d+)/i) || html.match(/latitude\s*[:=]\s*(-?\d+\.\d+)/i);
				const lngMatch = html.match(/lng["']?\s*:\s*(-?\d+\.\d+)/i) || html.match(/longitude\s*[:=]\s*(-?\d+\.\d+)/i);

				if (latMatch) data.lat = parseFloat(latMatch[1]);
				if (lngMatch) data.lng = parseFloat(lngMatch[1]);

				// Bedrooms usually in a list or specific span
				const bedEl = Array.from(document.querySelectorAll('li, span, div')).find(el =>
					el.textContent.toLowerCase().includes('bedroom') && /\d+/.test(el.textContent)
				);
				if (bedEl) {
					const match = bedEl.textContent.match(/(\d+)\s*bedroom/i);
					if (match) data.bedrooms = match[1];
				}

				return data;
			} catch (e) {
				return null;
			}
		});

		const htmlContent = await detailPage.content();

		return {
			coords: {
				latitude: detailData?.lat || null,
				longitude: detailData?.lng || null,
			},
			bedrooms: detailData?.bedrooms || property.bedrooms || null,
			html: htmlContent,
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNum = 1, totalPages = 20 } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	await page.waitForTimeout(2000);
	await page.waitForSelector('.span4.eapow-row0, .span4.eapow-row1, .property-item', { timeout: 30000 }).catch(() => { });

	// Extract properties
	const properties = await page.evaluate(() => {
		try {
			const items = Array.from(document.querySelectorAll(".span4.eapow-row0, .span4.eapow-row1, .property-item"));
			return items.map((el) => {
				const thumbAnchor = el.querySelector(".eapow-property-thumb-holder a") || el.querySelector("a.readmoreBtn") || el.querySelector("a");
				const link = thumbAnchor ? thumbAnchor.href : null;
				const priceText = el.querySelector(".eapow-overview-price, .price, .property-price")?.innerText || "";
				const title = el.querySelector(".eapow-overview-title h3, .property-title, h3")?.innerText || "Property";

				// Sold/STC check
				const soldBanner = el.querySelector('img[src*="banner_sold"], img[src*="banner_soldstc"], img[alt*="Sold"], .sold-banner');
				const statusText = soldBanner ? "SOLD" : (el.querySelector(".status, .label")?.innerText?.trim() || "");

				// Icons
				const iconNums = Array.from(el.querySelectorAll(".IconNum")).map((s) => s.textContent.trim());
				const bedrooms = iconNums[0] || null;

				return { link, priceText, title, status: statusText, bedrooms };
			}).filter((p) => p.link);
		} catch (err) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);
	stats.totalFound += properties.length;

	for (const property of properties) {
		try {
			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			stats.totalScraped++;

			if (isSoldProperty(property.status)) {
				stats.totalSkipped++;
				logger.property(
					pageNum,
					label,
					property.title.substring(0, 40),
					formatPriceDisplay(null, isRental),
					property.link,
					isRental,
					totalPages,
					"SKIPPED",
				);
				continue;
			}

			const price = parsePrice(property.priceText);
			if (price === null || price === 0) {
				stats.totalSkipped++;
				continue;
			}

			const result = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let propertyAction = "UNCHANGED";
			if (result.updated) {
				stats.totalSaved++;
				propertyAction = "UPDATED";
				if (isRental) stats.savedRentals++;
				else stats.savedSales++;
			}

			let lat = null;
			let lng = null;
			let bedrooms = property.bedrooms;

			if (!result.isExisting && !result.error) {
				const detail = await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
				if (detail) {
					lat = detail.coords.latitude;
					lng = detail.coords.longitude;
					bedrooms = detail.bedrooms || bedrooms;

					await processPropertyWithCoordinates(
						property.link.trim(),
						price,
						property.title,
						bedrooms,
						AGENT_ID,
						isRental,
						detail.html,
						lat,
						lng,
					);

					stats.totalSaved++;
					propertyAction = "CREATED";
					if (isRental) stats.savedRentals++;
					else stats.savedSales++;
				}
			} else if (result.error) {
				propertyAction = "ERROR";
				stats.totalSkipped++;
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
				lat,
				lng,
			);

			if (propertyAction !== "UNCHANGED") {
				await sleep(500);
			}
		} catch (err) {
			logger.error(`Error processing property ${property.link}: ${err.message}`, err, pageNum, label);
		}
	}

	// Dynamic Pagination
	if (pageNum === 1) {
		const totalPagesDetected = await page.evaluate(() => {
			const paginationText = document.querySelector(".pagination")?.innerText || "";
			const match = paginationText.match(/Page \d+ of (\d+)/i);
			if (match) return parseInt(match[1], 10);

			const pages = Array.from(document.querySelectorAll(".pagination a")).map(a => parseInt(a.innerText.trim())).filter(n => !isNaN(n));
			return pages.length > 0 ? Math.max(...pages) : 1;
		});

		if (totalPagesDetected > 1) {
			logger.step(`Found ${totalPagesDetected} pages for ${label}. Queueing remaining pages...`);
			for (let p = 2; p <= totalPagesDetected; p++) {
				const startValue = (p - 1) * 12;
				const pageUrl = `${request.url.split('?')[0]}?start=${startValue}`;
				await crawler.addRequests([
					{
						url: pageUrl,
						userData: { isRental, label, pageNum: p, totalPages: totalPagesDetected },
					},
				]);
			}
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
		requestHandlerTimeoutSecs: 600,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			const { pageNum, label } = request.userData || {};
			logger.error(`Failed listing page: ${request.url}`, null, pageNum, label);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeMapEstateAgents() {
	const scrapeStartTime = new Date();
	const startPage = getStartPage();
	const isPartialRun = startPage > 1;

	logger.step(`Starting Map Estate Agents scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const PROPERTY_TYPES = [
		{
			urlBase: "https://www.mapestateagents.com/property-sales/properties-for-sale?start=0",
			isRental: false,
			label: "SALES",
		},
		{
			urlBase: "https://www.mapestateagents.com/property-lettings/properties-to-let?start=0",
			isRental: true,
			label: "RENTALS",
		},
	];

	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label}...`);
		const crawler = createCrawler(browserWSEndpoint);
		await crawler.addRequests([
			{
				url: propertyType.urlBase,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
					pageNum: startPage,
					totalPages: startPage
				},
			},
		]);
		await crawler.run();
	}

	logger.step(
		`Completed Map Estate Agents Agent ${AGENT_ID}`,
		`found=${stats.totalFound}, scraped=${stats.totalScraped}, saved=${stats.totalSaved}, skipped=${stats.totalSkipped}, sales=${stats.savedSales}, rentals=${stats.savedRentals}`,
	);

	if (!isPartialRun) {
		logger.step(`Updating remove status...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

(async () => {
	try {
		await scrapeMapEstateAgents();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
