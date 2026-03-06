const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 78;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

const processedUrls = new Set();

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
				const geoMatch = html.match(
					/"@type":"GeoCoordinates","latitude":([0-9e.-]+),"longitude":([0-9e.-]+)/,
				);
				if (geoMatch) {
					return { lat: parseFloat(geoMatch[1]), lng: parseFloat(geoMatch[2]) };
				}

				const geoMatch2 = html.match(
					/"latitude"\s*:\s*([0-9e.-]+)\s*,\s*"longitude"\s*:\s*([0-9e.-]+)/,
				);
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
			return items
				.map((el) => {
					const linkEl = el.querySelector("a");
					let link = linkEl ? linkEl.getAttribute("href") : null;
					if (link && !link.startsWith("http")) {
						link = "https://robertholmes.co.uk" + link;
					}

					const title =
						el.querySelector(".property-archive-title h4")?.textContent?.trim() || "Property";
					const priceText = el.querySelector(".property-archive-price")?.textContent?.trim() || "";

					// Bedrooms extraction
					const icons = Array.from(el.querySelectorAll(".icons-list li"));
					const bedEl = icons.find(
						(li) => li.querySelector(".icon-bed") || li.innerText.toLowerCase().includes("bed"),
					);
					const bedText = bedEl ? bedEl.innerText.trim() : "";
					const bedroomsMatch = bedText.match(/\d+/);
					const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[0]) : null;

					const statusText = el.innerText || "";

					return { link, title, priceText, bedrooms, statusText };
				})
				.filter((p) => p.link);
		} catch (e) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		try {
			if (isSoldProperty(property.statusText)) continue;

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
						lng,
					);

					stats.totalSaved++;
					propertyAction = "CREATED";
				}
			}

			logger.property(
				pageNum,
				label,
				property.title,
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
			logger.error(
				`Error processing property ${property.link}: ${err.message}`,
				err,
				pageNum,
				label,
			);
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
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeRobertHolmes() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Robert Holmes scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const maxPages = 10; // Sales has ~8, Lettings has ~2

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Queue SALES (using 10 as safe max, it will handle missing pages via logger.warn/error if needed, or we can be precise)
	const salesPages = 10;
	logger.step(`Queueing SALES (${salesPages} pages) starting from page ${startPage}...`);
	for (let pg = Math.max(1, startPage); pg <= salesPages; pg++) {
		allRequests.push({
			url: `https://robertholmes.co.uk/search/page/${pg}/?address_keyword&department=residential-sales&availability=2`,
			userData: { pageNum: pg, isRental: false, label: "SALES", totalPages: salesPages },
		});
	}

	// Queue LETTINGS
	if (startPage === 1) {
		const lettingsPages = 4;
		logger.step(`Queueing LETTINGS (${lettingsPages} pages)...`);
		for (let pg = 1; pg <= lettingsPages; pg++) {
			allRequests.push({
				url: `https://robertholmes.co.uk/search/page/${pg}/?address_keyword&department=residential-lettings&availability=6`,
				userData: { pageNum: pg, isRental: true, label: "RENTALS", totalPages: lettingsPages },
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	}

	logger.step(`Finished Robert Holmes - Saved: ${stats.totalSaved}`);

	if (startPage === 1) {
		logger.step(`Updating remove status...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

(async () => {
	try {
		await scrapeRobertHolmes();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.message || err}`);
		process.exit(1);
	}
})();
