// Leaders scraper using Playwright with Crawlee
// Agent ID: 54
// Usage:
// node backend/scraper-agent-54.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 54;
let totalScraped = 0;
let totalSaved = 0;
const processedUrls = new Set();
const logger = createAgentLogger(AGENT_ID);

// Configuration for Leaders
const PROPERTY_TYPES = [
	{
		// Sales
		urlBase: "https://www.leaders.co.uk/properties/for-sale",
		totalPages: 211,
		recordsPerPage: 8,
		isRental: false,
		label: "SALES",
	},
	{
		// Rentals
		urlBase: "https://www.leaders.co.uk/properties/to-rent",
		totalPages: 344,
		recordsPerPage: 8,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeLeaders() {
	logger.step(`Starting Leaders scraper (Agent ${AGENT_ID})...`);

	function getBrowserlessEndpoint() {
		return (
			process.env.BROWSERLESS_WS_ENDPOINT ||
			`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
		);
	}

	async function scrapePropertyDetail(browserContext, property) {
		await new Promise((r) => setTimeout(r, 700));

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

			await detailPage.goto(property.link, {
				waitUntil: "domcontentloaded",
				timeout: 90000,
			});

			await detailPage.waitForTimeout(1500);

			const htmlContent = await detailPage.content();
			const coords = await extractCoordinatesFromHTML(htmlContent);

			return {
				coords: {
					latitude: coords.latitude || null,
					longitude: coords.longitude || null,
				},
			};
		} catch (error) {
			return null;
		} finally {
			await detailPage.close();
		}
	}

	const crawler = new PlaywrightCrawler({
		navigationTimeoutSecs: 120,
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint: getBrowserlessEndpoint(),
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(2000);
			await page
				.waitForSelector(".property-card-wrapper", { timeout: 20000 })
				.catch(() => logger.step(`No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll(".property-card-wrapper"));

					return items
						.map((el) => {
							const linkEl = el.querySelector("a[href]");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: href.startsWith("/")
										? "https://www.leaders.co.uk" + href
										: "https://www.leaders.co.uk/" + href
								: null;

							const title = el.querySelector(".property-title h2")?.textContent?.trim() || "";
							const price = el.querySelector(".property-price")?.textContent?.trim() || "";

							// ==================== IMPROVED BEDROOMS EXTRACTION ====================
							let bedrooms = null;

							// Strategy 1: Look for elements containing "bed" or "bedroom"
							const featureItems = el.querySelectorAll('li.list-inline-item, .feature, .spec, span, div');

							for (const item of featureItems) {
								const text = item.textContent.trim();
								if (/bed/i.test(text)) {
									const match = text.match(/(\d+)\s*(?:bed|beds|bedroom|bedrooms)/i);
									if (match) {
										bedrooms = parseInt(match[1]);
										break;
									}
								}
							}

							// Strategy 2: Fallback - search entire card text
							if (!bedrooms) {
								const fullText = el.textContent || "";
								const match = fullText.match(/(\d+)\s*(?:bed|beds|bedroom|bedrooms)/i);
								if (match) {
									bedrooms = parseInt(match[1]);
								}
							}

							// Strategy 3: Look specifically for number near bed icon (common pattern)
							if (!bedrooms) {
								const bedIconParent = el.querySelector('li:has(svg), li:has(i[class*="bed"])');
								if (bedIconParent) {
									const match = bedIconParent.textContent.match(/(\d+)/);
									if (match) bedrooms = parseInt(match[1]);
								}
							}

							const statusText = el.innerText || "";

							return { 
								link, 
								price, 
								title, 
								bedrooms, 
								statusText 
							};
						})
						.filter((p) => p.link);
				} catch (e) {
					console.error("Error in page.evaluate:", e);
					return [];
				}
			});

			logger.page(pageNum, label, `Found ${properties.length} properties`);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						if (processedUrls.has(property.link)) return;
						processedUrls.add(property.link);

						if (isSoldProperty(property.statusText || "")) return;

						const price = formatPriceUk(property.price);
						if (!price) return;

						// Clean bedrooms
						if (property.bedrooms && isNaN(property.bedrooms)) {
							property.bedrooms = null;
						}

						const result = await updatePriceByPropertyURLOptimized(
							property.link,
							price,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
						);

						if (result.updated) {
							totalSaved++;
						}

						if (!result.isExisting && !result.error) {
							const detail = await scrapePropertyDetail(page.context(), property);

							await updatePriceByPropertyURL(
								property.link.trim(),
								price,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								detail?.coords?.latitude || null,
								detail?.coords?.longitude || null,
							);

							totalSaved++;
							totalScraped++;
						}
					}),
				);

				await new Promise((resolve) => setTimeout(resolve, 300));
			}
		},

		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],

		failedRequestHandler({ request }) {
			logger.error(`Failed: ${request.url}`);
		},
	});

	// Enqueue all pages
	const allRequests = [];
	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = pg === 1 
				? `${propertyType.urlBase}/` 
				: `${propertyType.urlBase}/page-${pg}/`;
			
			allRequests.push({
				url,
				userData: { 
					pageNum: pg, 
					isRental: propertyType.isRental, 
					label: propertyType.label 
				},
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	}

	logger.step(`Completed Leaders - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

(async () => {
	try {
		await scrapeLeaders();
		await updateRemoveStatus(AGENT_ID);
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.stack || err}`);
		process.exit(1);
	}
})();