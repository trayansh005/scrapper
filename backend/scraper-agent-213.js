// Ryder & Dutton scraper using Playwright with Crawlee
// Agent ID: 213
// Website: ryderdutton.co.uk
// Usage:
// node backend/scraper-agent-213.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 213;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
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
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		await new Promise((r) => setTimeout(r, 1000));

		const detailData = await detailPage.evaluate(() => {
			try {
				const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
				for (const s of scripts) {
					try {
						const data = JSON.parse(s.textContent);
						if (data && data.geo)
							return {
								lat: parseFloat(data.geo.latitude),
								lng: parseFloat(data.geo.longitude),
								html: document.documentElement.innerHTML,
							};
						const graph = data["@graph"];
						if (graph) {
							for (const node of graph) {
								if (node && node.geo)
									return {
										lat: parseFloat(node.geo.latitude),
										lng: parseFloat(node.geo.longitude),
										html: document.documentElement.innerHTML,
									};
							}
						}
					} catch (e) {}
				}

				const all = document.documentElement.innerHTML;
				const latM = all.match(/"latitude"\s*:\s*([0-9.+-]+)/i);
				const lngM = all.match(/"longitude"\s*:\s*([0-9.+-]+)/i);
				if (latM && lngM) return { lat: parseFloat(latM[1]), lng: parseFloat(lngM[1]), html: all };
			} catch (e) {}
			return { lat: null, lng: null, html: document.documentElement.innerHTML };
		});

		// Coordinate inversion heuristic for Ryder & Dutton
		let lat = detailData.lat;
		let lng = detailData.lng;
		if (lat !== null && lng !== null) {
			if (
				Math.abs(lat) <= 10 &&
				lng >= 49 &&
				lng <= 61 &&
				!(lat >= 49 && lat <= 61 && Math.abs(lng) <= 10)
			) {
				const t = lat;
				lat = lng;
				lng = t;
			}
		}

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			detailData.html,
			lat,
			lng,
		);

		stats.totalScraped++;
		stats.totalSaved++;
		if (isRental) stats.savedRentals++;
		else stats.savedSales++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNumber, totalPages } = request.userData;
	logger.page(pageNumber, label, request.url, totalPages);

	try {
		await page
			.waitForSelector('[class*="SearchResultCard_searchItem"]', { timeout: 30000 })
			.catch(() => {
				logger.error(`No property cards found on page ${pageNumber}`, null, pageNumber, label);
			});

		const properties = await page.evaluate(() => {
			const items = Array.from(document.querySelectorAll('[class*="SearchResultCard_searchItem"]'));
			return items.map((el) => {
				const linkEl = el.querySelector("a[href]");
				const href = linkEl ? linkEl.getAttribute("href") : null;
				const link = href
					? href.startsWith("http")
						? href
						: "https://ryderdutton.co.uk" + href
					: null;

				const title =
					el.querySelector('[class*="SearchResultCard_title"] h3')?.textContent?.trim() ||
					el.querySelector("h3")?.textContent?.trim() ||
					"";
				const address =
					el.querySelector('[class*="SearchResultCard_address"]')?.textContent?.trim() || "";
				const priceText =
					el.querySelector('[class*="SearchResultCard_price"]')?.textContent?.trim() || "";

				const bedLi = el.querySelector(".htype1");
				const bedrooms = bedLi ? parseInt(bedLi.textContent.replace(/\D+/g, "")) : null;

				const statusText = el.innerText || "";

				return { link, title: title || address, priceText, bedrooms, statusText };
			});
		});

		logger.page(pageNumber, label, `Found ${properties.length} properties`, totalPages);

		for (const property of properties) {
			if (!property.link || !property.priceText) continue;

			if (isSoldProperty(property.statusText)) continue;

			const price = parsePrice(property.priceText);
			if (!price) continue;

			const updateResult = await updatePriceByPropertyURLOptimized(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);

			let action = "SEEN";
			if (updateResult.updated) {
				stats.totalSaved++;
				action = "UPDATED";
			}

			if (!updateResult.isExisting && !updateResult.error) {
				action = "CREATED";
				await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
				await new Promise((r) => setTimeout(r, 1000));
			} else if (updateResult.error) {
				action = "ERROR";
			}

			logger.property(
				pageNumber,
				label,
				property.title.substring(0, 40),
				`£${price}`,
				property.link,
				isRental,
				totalPages,
				action,
			);
		}
	} catch (error) {
		logger.error(`Error in handleListingPage: ${error.message}`, error, pageNumber, label);
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
		requestHandlerTimeoutSecs: 600,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
				await page.setExtraHTTPHeaders({
					"User-Agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
				});
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: { browserWSEndpoint, args: ["--no-sandbox", "--disable-setuid-sandbox"] },
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

async function scrapeRyderDutton() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Ryder & Dutton Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const AREAS = [
		{
			label: "SALES",
			isRental: false,
			baseUrl: "https://ryderdutton.co.uk/properties-for-sale/All?excludeSstc=1",
			totalPages: 99,
		},
		{
			label: "RENTALS",
			isRental: true,
			baseUrl: "https://ryderdutton.co.uk/properties-to-rent/All?excludeSstc=1",
			totalPages: 7,
		},
	];

	for (const area of AREAS) {
		const requests = [];
		for (let pg = Math.max(1, startPage); pg <= area.totalPages; pg++) {
			const url = `${area.baseUrl}&page=${pg}`;
			requests.push({
				url,
				userData: {
					pageNumber: pg,
					isRental: area.isRental,
					label: area.label,
					totalPages: area.totalPages,
				},
			});
		}
		if (requests.length > 0) {
			logger.step(`Queueing ${requests.length} ${area.label} listing pages...`);
			await crawler.addRequests(requests);
		}
	}

	await crawler.run();

	logger.step(
		`Finished Ryder & Dutton - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, RENTALS: ${stats.savedRentals}`);

	if (startPage === 1) {
		logger.step("Updating remove status for properties not seen in this run...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeRyderDutton();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
