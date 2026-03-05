// Bourne Estate Agents scraper using Playwright with Crawlee
// Agent ID: 211
// Website: bourneestateagents.com
// Usage:
// node backend/scraper-agent-211.js [startPage]

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

const AGENT_ID = 211;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// USER AGENTS (rotated per request to bypass bot detection)
// ============================================================================

const USER_AGENTS = [
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:133.0) Gecko/20100101 Firefox/133.0",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
];

function getRandomUserAgent() {
	return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

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
				const yoast = document.querySelector("script.yoast-schema-graph");
				if (yoast) {
					const raw = JSON.parse(yoast.textContent);
					const items = raw["@graph"] || [raw];
					for (const node of items) {
						if (node && node.latitude && node.longitude)
							return {
								lat: parseFloat(node.latitude),
								lng: parseFloat(node.longitude),
								html: document.documentElement.innerHTML,
							};
						if (node && node.geo)
							return {
								lat: parseFloat(node.geo.latitude || node.geo.lat),
								lng: parseFloat(node.geo.longitude || node.geo.long),
								html: document.documentElement.innerHTML,
							};
					}
				}

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
		await page.waitForSelector(".archive-grid", { timeout: 30000 }).catch(() => {
			logger.error(`No archive-grid found on page ${pageNumber}`, null, pageNumber, label);
		});

		const properties = await page.evaluate(() => {
			const results = [];
			const container = document.querySelector(".archive-grid");
			if (!container) return results;

			const anchors = Array.from(container.querySelectorAll("a[href]"));
			const seen = new Set();

			for (const a of anchors) {
				const href = a.getAttribute("href");
				if (
					!href ||
					!/\/property\b|property-for-sale|property-to-rent|property-for-rent/i.test(href)
				)
					continue;

				const link = href.startsWith("http") ? href : "https://bourneestateagents.com" + href;
				if (seen.has(link)) continue;
				seen.add(link);

				const card =
					a.closest(".properties-block, .grid-box, .grid-box-card, article") || a.parentElement;
				const title = (
					card?.querySelector(".property-archive-title h4")?.textContent ||
					card?.querySelector("h4")?.textContent ||
					a.getAttribute("title") ||
					""
				).trim();
				const priceText = card?.querySelector(".property-archive-price")?.textContent?.trim() || "";
				const bedEl = card?.querySelector(".property-types li span");
				const bedrooms = bedEl ? parseInt(bedEl.textContent.trim()) : null;

				const statusText = card?.innerText || "";

				results.push({ link, title, priceText, bedrooms, statusText });
			}
			return results;
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
				// Hide automation fingerprint so StackCDN doesn't block VPS IP
				await page.addInitScript(() => {
					Object.defineProperty(navigator, "webdriver", { get: () => undefined });
				});
				await blockNonEssentialResources(page);
				const ua = getRandomUserAgent();
				await page.setExtraHTTPHeaders({
					"User-Agent": ua,
					Accept:
						"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
					"Accept-Language": "en-GB,en;q=0.9",
					"Accept-Encoding": "gzip, deflate, br",
					"Cache-Control": "no-cache",
					Pragma: "no-cache",
				});
			},
		],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: [
					"--no-sandbox",
					"--disable-setuid-sandbox",
					"--disable-blink-features=AutomationControlled",
				],
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

async function scrapeBourne() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Bourne Estate Agents Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const AREAS = [
		{
			label: "SALES",
			isRental: false,
			baseUrl:
				"https://bourneestateagents.com/search/?address_keyword&property_type&minimum_price&maximum_price&minimum_rent&maximum_rent&availability=2&minimum_bedrooms&department=residential-sales",
			totalPages: 34,
		},
		{
			label: "RENTALS",
			isRental: true,
			baseUrl:
				"https://bourneestateagents.com/search/?address_keyword=&property_type=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&availability=6&minimum_bedrooms=&department=residential-lettings",
			totalPages: 10,
		},
	];

	for (const area of AREAS) {
		const requests = [];
		for (let pg = Math.max(1, startPage); pg <= area.totalPages; pg++) {
			let url = area.baseUrl;
			if (pg > 1) url = area.baseUrl.replace("/search/", `/search/page/${pg}/`);
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
		`Finished Bourne - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
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
		await scrapeBourne();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
