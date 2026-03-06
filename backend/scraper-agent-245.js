// Beresfords scraper using Playwright with Crawlee
// Agent ID: 245
// Usage:
// node backend/scraper-agent-245.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 245;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
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

// Redundant local function removed, now using property-helpers.js version

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
	await sleep(800);

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

		const detailData = await detailPage.evaluate(() => {
			try {
				const data = {
					price: null,
					bedrooms: null,
					address: null,
					lat: null,
					lng: null,
				};

				const scripts = Array.from(document.querySelectorAll("script[type='application/ld+json']"));
				for (const script of scripts) {
					try {
						const json = JSON.parse(script.textContent);

						if (json["@graph"] && Array.isArray(json["@graph"])) {
							const findOffer = (obj) => {
								if (!obj) return null;
								if (Array.isArray(obj)) {
									for (const item of obj) {
										const found = findOffer(item);
										if (found) return found;
									}
								}
								if (obj["@type"] === "Offer") return obj;
								if (obj["@graph"]) return findOffer(obj["@graph"]);
								if (obj.itemOffered) return findOffer(obj.itemOffered);
								return null;
							};

							const offerObj = findOffer(json);
							if (offerObj) {
								const item = offerObj.itemOffered || offerObj;
								if (item.numberOfBedrooms) data.bedrooms = item.numberOfBedrooms;
								if (item.address) {
									if (typeof item.address === "string") data.address = item.address;
									else if (item.address.streetAddress) {
										data.address = `${item.address.streetAddress}, ${
											item.address.addressLocality || ""
										} ${item.address.postalCode || ""}`.trim();
									}
								}
								if (offerObj.price) data.price = offerObj.price.toString();
							}
						}
					} catch (e) {}
				}

				if (!data.address) {
					const h1 = document.querySelector("h1");
					if (h1) data.address = h1.textContent.trim();
				}

				if (!data.price) {
					const priceEl = document.querySelector(".property-price, .asking-price, h3.price");
					if (priceEl) {
						data.price = priceEl.textContent;
					}
				}

				if (!data.bedrooms) {
					const text = document.body.innerText;
					const bedMatch = text.match(/(\d+)\s*Bed/i);
					if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
				}

				// Coordinates extraction
				const allScripts = Array.from(document.querySelectorAll("script"));
				for (const s of allScripts) {
					const content = s.textContent;
					if (content.includes("loadLocratingPlugin")) {
						const latMatch = content.match(/lat:\s*([-0-9.]+)/);
						const lngMatch = content.match(/lng:\s*([-0-9.]+)/);
						if (latMatch && lngMatch) {
							data.lat = parseFloat(latMatch[1]);
							data.lng = parseFloat(lngMatch[1]); // Beresfords sometimes has typo in script or we use first match
							break;
						}
					}
				}

				if (!data.lat) {
					const mapsLink = document.querySelector("a[href*='maps.google.com/maps?ll=']");
					if (mapsLink) {
						const href = mapsLink.getAttribute("href");
						const match = href.match(/ll=([-0-9.]+),([-0-9.]+)/);
						if (match) {
							data.lat = parseFloat(match[1]);
							data.lng = parseFloat(match[2]);
						}
					}
				}

				return data;
			} catch (e) {
				return null;
			}
		});

		if (!detailData) return null;

		const priceText = detailData.price;
		const price = formatPriceUk(priceText);
		const title = detailData.address || property.title || "Property";

		return {
			price,
			bedrooms: detailData.bedrooms || null,
			title,
			coords: {
				latitude: detailData.lat || null,
				longitude: detailData.lng || null,
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
	const { pageNum, totalPages, isRental, label } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector("a[href*='/property/']", { timeout: 15000 });
	} catch (e) {
		logger.warn(`No property links found on page ${pageNum} - attempting fallback`, pageNum, label);
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();
			const items = Array.from(document.querySelectorAll("a[href*='/property/']"));

			for (const el of items) {
				let href = el.getAttribute("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				if (!link.includes("/property/")) continue;

				const container = el.closest(".property-card") || el.closest("div") || el;
				const title =
					container.querySelector("h2, h3, .address")?.textContent?.trim() || "Property";
				const statusText = container.innerText || "";
				
				const priceText = container.querySelector(".property-price, .asking-price, h3.price")?.textContent?.trim() || "";
				const bedText = container.innerText || "";

				results.push({ link, title, statusText, priceText, bedText });
			}

			return results;
		} catch (e) {
			return [];
		}
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		try {
			if (!property.link) continue;

			if (isSoldProperty(property.statusText || "")) continue;

			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			const price = parsePrice(property.priceText);
			let bedrooms = null;
			const bedMatch = property.bedText.match(/(\d+)\s*Bed/i);
			if (bedMatch) bedrooms = parseInt(bedMatch[1]);

			// 1. Try to update price first without visiting detail page
			const result = await updatePriceByPropertyURLOptimized(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
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
			let finalPrice = price;
			let finalTitle = property.title;
			let finalBedrooms = bedrooms;

			// 2. Only visit detail page if property is NEW (to get coordinates)
			if (!result.isExisting && !result.error) {
				const detail = await scrapePropertyDetail(page.context(), property, isRental);

				if (detail && detail.price) {
					await updatePriceByPropertyURL(
						property.link.trim(),
						detail.price || price,
						detail.title || property.title,
						detail.bedrooms || bedrooms,
						AGENT_ID,
						isRental,
						detail.coords.latitude,
						detail.coords.longitude,
					);

					stats.totalSaved++;
					stats.totalScraped++;
					if (isRental) stats.savedRentals++;
					else stats.savedSales++;
					
					propertyAction = "CREATED";
					lat = detail.coords.latitude;
					lng = detail.coords.longitude;
					finalPrice = detail.price || price;
					finalTitle = detail.title || property.title;
					finalBedrooms = detail.bedrooms || bedrooms;
				} else {
					propertyAction = "ERROR";
				}
			} else if (result.error) {
				propertyAction = "ERROR";
			}

			logger.property(
				pageNum,
				label,
				finalTitle,
				formatPriceDisplay(finalPrice, isRental),
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
			logger.error(`Error processing property ${property.link || "unknown"}: ${err.message}`, err, pageNum, label);
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
		requestHandlerTimeoutSecs: 300,
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

async function scrapeBeresfords() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Beresfords scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const totalSalesPages = 69;
	const totalLettingsPages = 10;

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let pg = Math.max(1, startPage); pg <= totalSalesPages; pg++) {
		const url = `https://www.beresfords.co.uk/find-a-property/for-sale/page/${pg}/?location=&radius=0&bedsMin=0&priceMin=0&priceMax=0&type=all&branch=&tag=&showUnavailable=false&order=highest-to-lowest`;

		allRequests.push({
			url,
			userData: {
				pageNum: pg,
				totalPages: totalSalesPages,
				isRental: false,
				label: "SALES",
			},
		});
	}

	// Build Lettings requests
	if (startPage === 1) {
		for (let pg = 1; pg <= totalLettingsPages; pg++) {
			const url = `https://www.beresfords.co.uk/find-a-property/to-rent/page/${pg}/?location=&radius=0&bedsMin=0&priceMin=0&priceMax=0&type=all&branch=&tag=&showUnavailable=false&order=highest-to-lowest`;

			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					totalPages: totalLettingsPages,
					isRental: true,
					label: "LETTINGS",
				},
			});
		}
	}

	if (allRequests.length === 0) {
		logger.step("No pages to scrape with current arguments.");
		return;
	}

	logger.step(`Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	logger.step(`Completed Beresfords - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`);
	logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);

	if (startPage === 1) {
		logger.step(`Updating remove status for Agent ${AGENT_ID}...`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step("Partial run detected, skipping updateRemoveStatus.");
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeBeresfords();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.message || err}`);
		process.exit(1);
	}
})();
