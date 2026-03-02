// Kinleigh Folkard & Hayward (KFH) scraper using Playwright with Crawlee
// Agent ID: 75
// Modeled after agent 84 with full URL pagination (one by one)
//
// Usage:
// node backend/scraper-agent-75.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	extractCoordinatesFromHTML,
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce logging noise
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 75;
const logger = createAgentLogger(AGENT_ID);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const counts = {
	created: 0,
	updated: 0,
	unchanged: 0,
	errors: 0,
	sold: 0,
};

const processedUrls = new Set();
const scrapeStartTime = new Date();

// Configuration for sales and rentals — full KFH URLs
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.kfh.co.uk/property/for-sale/in-london/exclude-sale-agreed/",
		totalPages: 94,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.kfh.co.uk/property/to-rent/in-london/exclude-let-agreed/",
		totalPages: 39,
		isRental: true,
		label: "RENTALS",
	},
];

/**
 * Scrapes detail page for coordinates and sold status
 */
async function scrapePropertyDetail(browserContext, url) {
	const page = await browserContext.newPage();
	let results = { latitude: null, longitude: null, isSold: false };

	try {
		await blockNonEssentialResources(page);
		await page.goto(url, { waitUntil: "domcontentloaded", timeout: 30000 });

		// Wait briefly for content
		await page.waitForTimeout(2000);

		const html = await page.content();
		results.isSold = isSoldProperty(html);

		const coords = extractCoordinatesFromHTML(html);
		if (coords) {
			results.latitude = coords.latitude;
			results.longitude = coords.longitude;
		}
	} catch (err) {
		// Log error but don't crash
	} finally {
		await page.close();
	}

	return results;
}

async function scrapeKFH() {
	const args = process.argv.slice(2);
	const startPage = parseInt(args[0]) || 1;
	const isPartialRun = startPage > 1;

	logger.step(`Starting KFH scraper (Agent ${AGENT_ID})...`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 1,
		requestHandlerTimeoutSecs: 300,
		navigationTimeoutSecs: 60,

		launchContext: {
			launchOptions: {
				headless: true,
				args: [
					"--no-sandbox",
					"--disable-setuid-sandbox",
					"--disable-dev-shm-usage",
					"--disable-accelerated-2d-canvas",
					"--disable-gpu",
				],
			},
		},

		async requestHandler({ page, request, crawler: { requestQueue } }) {
			const { pageNum, isRental, label, totalPages } = request.userData;

			logger.page(pageNum, label, request.url, totalPages);

			// Accept cookies once per session/page if needed
			const cookieButton = await page.$("#onetrust-accept-btn-handler");
			if (cookieButton) {
				await cookieButton.click().catch(() => {});
				await page.waitForTimeout(1000);
			}

			// Wait for listing container
			await page
				.waitForSelector(".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0", {
					timeout: 20000,
				})
				.catch(() => {});

			// Extract properties
			const properties = await page.evaluate(() => {
				const cardSelector = ".sales-wrap, .PropertyCard__StyledPropertyCard-sc-1kiuolp-0";
				const cards = Array.from(document.querySelectorAll(cardSelector));

				return cards
					.map((card) => {
						try {
							let link = null;
							const anchors = card.querySelectorAll("a[href]");
							for (const a of anchors) {
								const href = a.getAttribute("href");
								if (
									href &&
									(href.includes("/property-for-sale/") || href.includes("/property-to-rent/"))
								) {
									link = a.href;
									break;
								}
							}

							const title =
								card.querySelector("h3")?.textContent?.trim() ||
								card
									.querySelector("a[class*='PropertyCard__StyledAddressLink']")
									?.textContent?.trim();

							const priceText =
								card.querySelector(".highlight-text")?.textContent?.trim() ||
								card
									.querySelector(".PropertyPriceAndStatus__StyledPrice-sc-1dv7ovq-0 h2")
									?.textContent?.trim() ||
								card
									.querySelector("p[class*='PropertyPriceAndStatus__StyledPrice']")
									?.textContent?.trim() ||
								card.querySelector(".PropertyCard__StyledPrice")?.textContent?.trim();

							// Bedroom extraction
							let bedrooms = null;
							const bedSpan = card.querySelector(".p-bed");
							if (bedSpan) {
								const text = bedSpan.parentElement?.textContent?.trim() || "";
								const match = text.match(/(\d+)\s+bedrooms?/i);
								if (match) bedrooms = match[1];
							}
							if (!bedrooms) {
								const bedIcon = card.querySelector(".icon-bed");
								if (bedIcon) bedrooms = bedIcon.nextElementSibling?.textContent?.trim();
							}
							if (!bedrooms) {
								const items = card.querySelectorAll("span[class*='PropertyMeta__StyledMetaItem']");
								for (const it of items) {
									if (it.textContent.toLowerCase().includes("bedroom")) {
										const m = it.textContent.match(/(\d+)/);
										if (m) {
											bedrooms = m[1];
											break;
										}
									}
								}
							}

							// Sold status from card
							const statusText =
								card.querySelector(".status-label, .sold-label")?.textContent?.trim() || "";
							const isSold =
								statusText.toLowerCase().includes("sold") ||
								statusText.toLowerCase().includes("agreed") ||
								statusText.toLowerCase().includes("let");

							return { link, title, priceText, bedrooms, isSold };
						} catch (e) {
							return null;
						}
					})
					.filter((p) => p && p.link);
			});

			for (const prop of properties) {
				if (processedUrls.has(prop.link)) continue;
				processedUrls.add(prop.link);

				if (prop.isSold) {
					counts.sold++;
					continue;
				}

				try {
					const numericPrice = parsePrice(prop.priceText);
					const formattedPrice = formatPriceDisplay(numericPrice, isRental);

					// 1. Try optimized update first
					const updateResult = await updatePriceByPropertyURLOptimized({
						link: prop.link,
						price: formattedPrice,
						title: prop.title,
						bedrooms: prop.bedrooms,
						agentId: AGENT_ID,
						isRental,
						isSold: false,
					});

					if (updateResult.isNew) {
						// 2. Scrape details if new
						const details = await scrapePropertyDetail(page.context(), prop.link);

						if (details.isSold) {
							counts.sold++;
							logger.property(
								pageNum,
								label,
								prop.title,
								formattedPrice,
								prop.link,
								isRental,
								totalPages,
								"SOLD",
							);
							continue;
						}

						await processPropertyWithCoordinates({
							link: prop.link,
							price: formattedPrice,
							title: prop.title,
							bedrooms: prop.bedrooms,
							agentId: AGENT_ID,
							isRental,
							latitude: details.latitude,
							longitude: details.longitude,
						});

						counts.created++;
						logger.property(
							pageNum,
							label,
							prop.title,
							formattedPrice,
							prop.link,
							isRental,
							totalPages,
							"CREATED",
						);
						await sleep(500); // Polite delay when creating
					} else if (updateResult.updated) {
						counts.updated++;
						logger.property(
							pageNum,
							label,
							prop.title,
							formattedPrice,
							prop.link,
							isRental,
							totalPages,
							"UPDATED",
						);
						await sleep(100);
					} else {
						counts.unchanged++;
						// logger.property(pageNum, label, prop.title, formattedPrice, prop.link, isRental, totalPages, "UNCHANGED");
					}
				} catch (err) {
					counts.errors++;
					logger.error(`Error processing ${prop.link}: ${err.message}`);
				}
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Failed to process ${request.url}`);
		},

		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
	});

	// Enqueue all pages
	const initialRequests = [];
	for (const type of PROPERTY_TYPES) {
		for (let p = startPage; p <= type.totalPages; p++) {
			initialRequests.push({
				url: `${type.urlBase}page-${p}/`,
				userData: {
					pageNum: p,
					totalPages: type.totalPages,
					isRental: type.isRental,
					label: type.label,
				},
			});
		}
	}

	if (initialRequests.length > 0) {
		await crawler.run(initialRequests);
	}

	logger.step(
		`Finished. Created: ${counts.created}, Updated: ${counts.updated}, Unchanged: ${counts.unchanged}, Sold: ${counts.sold}, Errors: ${counts.errors}`,
	);

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	}
}

(async () => {
	try {
		await scrapeKFH();
		process.exit(0);
	} catch (err) {
		console.error("Fatal Error:", err);
		process.exit(1);
	}
})();
