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

const AGENT_ID = 78;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// PROPERTY TYPES CONFIGURATION
// ============================================================================

const PROPERTY_TYPES = [
	{
		urlBase: "https://robertholmes.co.uk/search/",
		params: "address_keyword=&department=residential-sales&availability=2",
		totalPages: 10,
		recordsPerPage: 12,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://robertholmes.co.uk/search/",
		params: "address_keyword=&department=residential-lettings",
		totalPages: 10,
		recordsPerPage: 12,
		isRental: true,
		label: "RENTALS",
	}
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
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		const coords = await detailPage.evaluate(() => {
			try {
				// Look for GeoCoordinates JSON-LD data
				const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
				for (const s of scripts) {
					try {
						const data = JSON.parse(s.textContent);
						if (data && data["@type"] === "GeoCoordinates" && data.latitude && data.longitude) {
							if (Math.abs(data.latitude) > 0.1 && Math.abs(data.longitude) > 0.1) {
								return { lat: parseFloat(data.latitude), lng: parseFloat(data.longitude) };
							}
						}
					} catch (e) {}
				}

				// Regex search for GeoCoordinates pattern as fallback
				const html = document.documentElement.innerHTML;
				const geoMatch = html.match(/"@type":"GeoCoordinates","latitude":([0-9e.-]+),"longitude":([0-9e.-]+)/);
				if (geoMatch) {
					return { lat: parseFloat(geoMatch[1]), lng: parseFloat(geoMatch[2]) };
				}

				const geoMatch2 = html.match(/"latitude"\s*:\s*([0-9e.-]+)\s*,\s*"longitude"\s*:\s*([0-9e.-]+)/);
				if (geoMatch2) {
					return { lat: parseFloat(geoMatch2[1]), lng: parseFloat(geoMatch2[2]) };
				}

				return null;
			} catch (e) {
				return null;
			}
		});

		return {
			...property,
			coords: {
				latitude: coords?.lat || null,
				longitude: coords?.lng || null,
			},
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

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	await page.waitForTimeout(2000);
	await page.waitForSelector(".grid-box-card", { timeout: 30000 }).catch(() => {});

	const properties = await page.evaluate(() => {
		try {
			const items = Array.from(document.querySelectorAll(".grid-box-card"));
			return items.map((el) => {
				const linkEl = el.querySelector("a");
				let link = linkEl ? linkEl.getAttribute("href") : null;
				if (link && !link.startsWith("http")) {
					link = "https://robertholmes.co.uk" + link;
				}

				const title = el.querySelector(".property-archive-title h4")?.textContent?.trim() || "Property";
				const priceText = el.querySelector(".property-archive-price")?.textContent?.trim() || "";
				
				// Bedrooms extraction
				const icons = Array.from(el.querySelectorAll(".icons-list li"));
				const bedEl = icons.find(li => li.querySelector(".icon-bed") || li.innerText.toLowerCase().includes("bed"));
				const bedText = bedEl ? bedEl.innerText.trim() : "";
				const bedroomsMatch = bedText.match(/\d+/);
				const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[0]) : null;

				const statusText = el.innerText || "";

				return { link, title, priceText, bedrooms, statusText };
			}).filter(p => p.link);
		} catch (e) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		try {
			if (isSoldProperty(property.statusText)) {
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
			if (!price || price === 0) continue;

			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

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

			if (!result.isExisting && !result.error) {
				const detail = await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
				if (detail) {
					lat = detail.coords.latitude;
					lng = detail.coords.longitude;

					await processPropertyWithCoordinates(
						property.link.trim(),
						price,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
						null,
						lat,
						lng
					);

					stats.totalSaved++;
					propertyAction = "CREATED";
					if (isRental) stats.savedRentals++;
					else stats.savedSales++;
				}
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
					"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
				});
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
			const { pageNum, label } = request.userData || {};
			logger.error(`Failed listing page: ${request.url}`, null, pageNum, label);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeRobertHolmes() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Robert Holmes scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];
	
	for (const type of PROPERTY_TYPES) {
		const effectiveStartPage = Math.max(1, startPage);

		for (let pg = effectiveStartPage; pg <= type.totalPages; pg++) {
			const url = pg === 1
				? `${type.urlBase}?${type.params}`
				: `${type.urlBase}page/${pg}/?${type.params}`;

			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					totalPages: type.totalPages,
					isRental: type.isRental,
					label: type.label,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.run(allRequests);

	logger.step(
		`Completed Robert Holmes - Total saved: ${stats.totalSaved}, New rentals: ${stats.savedRentals}`,
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
		await scrapeRobertHolmes();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
