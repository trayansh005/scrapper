// Jacksons scraper using Playwright with Crawlee
// Agent ID: 210
// Website: jacksonsestateagents.com
// Usage:
// node backend/scraper-agent-210.js [startPage]

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

const AGENT_ID = 210;
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

		// small pause for dynamic scripts
		await new Promise((r) => setTimeout(r, 1000));

		const detailData = await detailPage.evaluate(() => {
			try {
				const map = document.querySelector("#propertyShowMap");
				if (map) {
					const lat = map.getAttribute("data-lat");
					const lng = map.getAttribute("data-lng");
					if (lat && lng)
						return {
							lat: parseFloat(lat),
							lng: parseFloat(lng),
							html: document.documentElement.innerHTML,
						};
				}

				const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
				for (const s of scripts) {
					try {
						const data = JSON.parse(s.textContent);
						if (data && data.geo && data.geo.latitude && data.geo.longitude) {
							return {
								lat: data.geo.latitude,
								lng: data.geo.longitude,
								html: document.documentElement.innerHTML,
							};
						}
					} catch (e) {}
				}
			} catch (e) {}
			return { lat: null, lng: null, html: document.documentElement.innerHTML };
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			detailData.html,
			detailData.lat,
			detailData.lng,
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
			.waitForSelector(".col-xs-12.col-sm-6.col-md-4.col-lg-3", { timeout: 30000 })
			.catch(() => {
				logger.error(`No property columns found on page ${pageNumber}`, null, pageNumber, label);
			});

		const properties = await page.evaluate(() => {
			const items = Array.from(document.querySelectorAll(".col-xs-12.col-sm-6.col-md-4.col-lg-3"));
			return items.map((el) => {
				const linkEl = el.querySelector("article.property-card a");
				const href = linkEl ? linkEl.getAttribute("href") : null;
				const link = href
					? href.startsWith("http")
						? href
						: "https://www.jacksonsestateagents.com" + href
					: null;

				const title = el.querySelector("h1")?.textContent?.trim() || "";
				const location = el.querySelector("h2")?.textContent?.trim() || "";

				const money = el.querySelector("data.money");
				const priceText = money
					? money.textContent.trim()
					: el.querySelector(".price")?.textContent?.trim() || "";

				const bedEl = el.querySelector(".bed-bath-icons__number");
				const bedrooms = bedEl ? parseInt(bedEl.textContent.trim()) : null;

				const statusText = el.innerText || "";

				return {
					link,
					title: title + (location ? ", " + location : ""),
					priceText,
					bedrooms,
					statusText,
				};
			});
		});

		logger.page(pageNumber, label, `Found ${properties.length} properties`, totalPages);

		for (const property of properties) {
			if (!property.link || !property.priceText) continue;

			if (isSoldProperty(property.statusText)) {
				continue;
			}

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

async function scrapeJacksons() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Jacksons Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const AREAS = [
		{
			label: "SALES",
			isRental: false,
			baseUrl: "https://www.jacksonsestateagents.com/properties/sales/status-available",
			totalPages: 33,
		},
		{
			label: "RENTALS",
			isRental: true,
			baseUrl: "https://www.jacksonsestateagents.com/properties/lettings/status-available",
			totalPages: 8,
		},
	];

	for (const area of AREAS) {
		const requests = [];
		for (let pg = Math.max(1, startPage); pg <= area.totalPages; pg++) {
			let url = area.baseUrl;
			if (pg > 1) url = `${area.baseUrl}/page-${pg}`;
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
		`Finished Jacksons - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
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
		await scrapeJacksons();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
