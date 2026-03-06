// Newton Fallowell scraper using Playwright with Crawlee
// Agent ID: 248
// Usage:
// node backend/scraper-agent-248.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 248;
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
	await sleep(700);

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

				const mapEl = document.querySelector(
					"#leaflet-map-single-property-container[data-lat][data-lng]",
				);
				if (mapEl) {
					const lat = parseFloat(mapEl.getAttribute("data-lat"));
					const lng = parseFloat(mapEl.getAttribute("data-lng"));
					if (!Number.isNaN(lat)) data.lat = lat;
					if (!Number.isNaN(lng)) data.lng = lng;
				}

				const scripts = Array.from(document.querySelectorAll("script[type='application/ld+json']"));
				for (const script of scripts) {
					try {
						const json = JSON.parse(script.textContent);

						if (json["@type"] === "RealEstateAgent" && json.geo) {
							data.lat = json.geo.latitude;
							data.lng = json.geo.longitude;
						}

						if (json["@type"] === "Offer" || (json["@graph"] && Array.isArray(json["@graph"]))) {
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
								if (offerObj.price) data.price = offerObj.price;
							}
						}
					} catch (e) {}
				}

				if (!data.address) {
					const h1 = document.querySelector("h1");
					if (h1) {
						const parts = h1.textContent
							.split("\n")
							.map((p) => p.trim())
							.filter((p) => p);
						data.address = parts.length >= 2 ? parts.slice(0, 2).join(", ") : h1.textContent.trim();
					}
				}

				if (!data.price) {
					const priceEl = document.querySelector("[class*='price']");
					if (priceEl) data.price = priceEl.textContent;
				}

				if (!data.bedrooms) {
					const bedroomEl = document.querySelector('[class*="bedroom"]');
					if (bedroomEl) {
						const bedText = bedroomEl.textContent.trim();
						const bedNum = bedText.match(/^\d+/);
						if (bedNum) data.bedrooms = parseInt(bedNum[0]);
					}

					if (!data.bedrooms) {
						const text = document.body.innerText;
						const bedMatch = text.match(/(\d+)\s*Bedroom/i);
						if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
					}
				}

				return data;
			} catch (e) {
				return null;
			}
		});

		if (!detailData) return null;

		const price = formatPriceUk(detailData.price);
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
		const propertyListSelector = ".sales-wrap, .sales-wrapper, .property--card, .property--card__results";
		await page.waitForSelector(propertyListSelector, { timeout: 15000 });
	} catch (e) {
		logger.warn(`Property containers not found on page ${pageNum} - attempting fallback`, pageNum, label);
	}

	await page.waitForTimeout(1500);

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();

			const containers = Array.from(document.querySelectorAll(".sales-wrap, .sales-wrapper, .property--card, .property--card__results"));

			for (const container of containers) {
				const anchor = container.querySelector("a.property--card__image-wrapper, .property-title--search a") || container.querySelector("a");
				const href = anchor?.getAttribute("href");
				if (!href) continue;

				if (href.includes("/book-a-viewing/") || href.includes("/myaccount")) continue;

				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				const statusText = 
					container.querySelector(".property-card-status")?.textContent?.trim() || 
					container.querySelector(".status, .property-status, [class*='status']")?.textContent?.trim() || 
					"";
				
				const title =
					container.querySelector(".property-title--search a")?.textContent?.trim() ||
					container.querySelector("h3")?.textContent?.trim() ||
					anchor.textContent?.trim() ||
					"Property";

				const priceText = 
					container.querySelector(".property-price--search")?.textContent?.trim() ||
					container.querySelector(".highlight-text")?.textContent?.trim() ||
					container.querySelector(".price, .property-price, [class*='price']")?.textContent?.trim() || 
					"";

				const bedText = container.querySelector(".property-type--search")?.textContent?.trim() || "";

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
			const bedMatch = property.bedText.match(/\d+/);
			if (bedMatch) bedrooms = parseInt(bedMatch[0]);

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

async function scrapeNewtonFallowell() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Newton Fallowell scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const totalSalesPages = 187;
	const totalLettingsPages = 20;

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	const buildListingUrl = (basePath, pageNum) => {
		const pageSegment = pageNum > 1 ? `page-${pageNum}/` : "";
		return `https://www.newtonfallowell.co.uk${basePath}${pageSegment}?orderby=price_desc&radius=3`;
	};

	// Build Sales requests
	for (let pg = Math.max(1, startPage); pg <= totalSalesPages; pg++) {
		const url = buildListingUrl("/properties/for-sale/in-the-midlands/", pg);

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
			const url = buildListingUrl("/properties/for-letting/in-the-midlands/", pg);

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

	logger.step(`Completed Newton Fallowell - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`);
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
		await scrapeNewtonFallowell();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.message || err}`);
		process.exit(1);
	}
})();
