// Charters Estate Agents scraper using Playwright with Crawlee
// Agent ID: 250
// Usage:
// node backend/scraper-agent-250.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 250;

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

function buildPagedUrl(urlBase, pageNum) {
	if (pageNum === 1) return urlBase;
	return `${urlBase.endsWith("/") ? urlBase : urlBase + "/"}page-${pageNum}/`;
}

function formatPriceDisplay(price, isRental) {
	if (!price) return isRental ? "£0 pcm" : "£0";
	return `£${price}${isRental ? " pcm" : ""}`;
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
	if (!property.link) return null;

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

		// Scroll to find map or coordinates
		try {
			const locationHeading = await detailPage
				.locator(".h3, h2, h3")
				.filter({ hasText: /location/i })
				.first();

			if ((await locationHeading.count?.()) > 0) {
				await locationHeading.scrollIntoViewIfNeeded({ timeout: 8000 });
			} else {
				await detailPage.evaluate(() => {
					const scrollEl = document.scrollingElement || document.documentElement;
					if (scrollEl) window.scrollTo(0, scrollEl.scrollHeight);
				});
			}

			await detailPage.waitForTimeout(1500);
			await detailPage.waitForFunction(
				() => {
					const iframe = document.querySelector("iframe");
					const src = iframe && (iframe.getAttribute("src") || iframe.src);
					return !!src && (src.includes("lat=") || src.includes("location"));
				},
				{ timeout: 8000 },
			);
		} catch (e) {
			// Location not found, continue anyway
		}

		const detailData = await detailPage.evaluate(() => {
			try {
				const data = { price: null, bedrooms: null, address: null, lat: null, lng: null };

				// Get coordinates from iframe
				for (const iframe of document.querySelectorAll("iframe")) {
					const src = iframe.getAttribute("src") || "";
					if (src.includes("lat=") && src.includes("lng=")) {
						const latMatch = src.match(/lat=([0-9.-]+)/);
						const lngMatch = src.match(/lng=([0-9.-]+)/);
						if (latMatch) data.lat = parseFloat(latMatch[1]);
						if (lngMatch) data.lng = parseFloat(lngMatch[1]);
						if (data.lat && data.lng) break;
					}
				}

				// JSON-LD parsing
				for (const script of document.querySelectorAll("script[type='application/ld+json']")) {
					try {
						const json = JSON.parse(script.textContent);

						if (json["@graph"] && Array.isArray(json["@graph"])) {
							for (const item of json["@graph"]) {
								if ((item["@type"] === "Offer" || item["@type"] === "Product") && item.price) {
									data.price = item.price.toString();
								}
								if (
									(item["@type"] === "Residence" || item["@type"] === "SingleFamilyResidence") &&
									item.name
								) {
									if (!data.address) data.address = item.name;
								}
							}
						}

						if ((json["@type"] === "Offer" || json["@type"] === "Product") && json.price) {
							data.price = json.price.toString();
						}
						if (
							(json["@type"] === "Residence" || json["@type"] === "SingleFamilyResidence") &&
							json.name
						) {
							if (!data.address) data.address = json.name;
						}
					} catch (e) {}
				}

				// Fallback address
				if (!data.address) {
					const h1 = document.querySelector("h1");
					if (h1) data.address = h1.textContent.trim();
				}

				// Fallback price extraction
				if (!data.price) {
					const getText = (el) => (el ? el.innerText || el.textContent : "");
					const checkSelectors = [
						"div[class*='price']",
						"span[class*='price']",
						"h2, h3, .banner-text",
					];

					for (const sel of checkSelectors) {
						const els = document.querySelectorAll(sel);
						for (const el of els) {
							const txt = getText(el);
							const match = txt.match(/£\s*([\d,]+)\s*p\.?c\.?m\.?/i);
							if (match) {
								data.price = match[1].replace(/,/g, "");
								break;
							}
						}
						if (data.price) break;
					}

					if (!data.price) {
						const bodyText = document.body?.innerText || "";
						const rawMatches = bodyText.matchAll(/(?:^|\s|>)£\s*([\d,]+)(?:\s|<|$)/g);
						for (const m of rawMatches) {
							const val = parseInt(m[1].replace(/,/g, ""));
							if (val > 300 && val < 20000) {
								data.price = m[1].replace(/,/g, "");
								break;
							}
						}
					}
				}

				// Bedrooms
				const h4 = document.querySelector("h4");
				if (h4) {
					const bedMatch = h4.textContent.match(/(\d+)\s*bedroom/i);
					if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
				}
				if (!data.bedrooms) {
					const text = document.body.innerText;
					const bedMatch = text.match(/(\d+)\s*bedroom/i);
					if (bedMatch) data.bedrooms = parseInt(bedMatch[1]);
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
		log.warn(` Error scraping detail page ${property.link}: ${error.message}`);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(` [${label}] Page ${pageNum} - ${request.url}`);

	try {
		const propertyListSelector = "a[href*='/property-for-sale/'], a[href*='/property-to-rent/']";
		await page.waitForSelector(propertyListSelector, { timeout: 15000 });
	} catch (e) {
		log.warn(` Property links not found on page ${pageNum}`);
	}

	await page.waitForTimeout(1500);

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();

			const anchors = Array.from(
				document.querySelectorAll("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']"),
			);

			for (const anchor of anchors) {
				const href = anchor.getAttribute("href");
				if (!href) continue;

				if (href.includes("/book-a-viewing/") || href.includes("/myaccount")) continue;

				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				const container = anchor.closest("article") || anchor.closest("li") || anchor;
				const statusText = container.innerText || "";

				const title =
					anchor.querySelector("h3")?.textContent?.trim() ||
					anchor.textContent?.trim() ||
					"Property";

				results.push({ link, title, statusText });
			}
			return results;
		} catch (e) {
			return [];
		}
	});

	console.log(` 🔗 Found ${properties.length} properties on page ${pageNum}`);

	const batchSize = 2;
	for (let i = 0; i < properties.length; i += batchSize) {
		const batch = properties.slice(i, i + batchSize);

		await Promise.all(
			batch.map(async (property) => {
				if (!property.link) return;
				if (isSoldProperty(property.statusText || "")) return;

				if (processedUrls.has(property.link)) {
					log.info(` Skipping duplicate URL: ${property.link.substring(0, 60)}...`);
					return;
				}
				processedUrls.add(property.link);

				const detail = await scrapePropertyDetail(page.context(), property, isRental);

				if (!detail || !detail.price) {
					log.warn(` No price found: ${property.link}`);
					return;
				}

				const result = await updatePriceByPropertyURLOptimized(
					property.link.trim(),
					detail.price,
					detail.title,
					detail.bedrooms,
					AGENT_ID,
					isRental,
				);

				let propertyAction = "UNCHANGED";
				if (result.updated) {
					stats.totalSaved++;
					propertyAction = "UPDATED";
				}

				if (!result.isExisting && !result.error) {
					await updatePriceByPropertyURL(
						property.link.trim(),
						detail.price,
						detail.title,
						detail.bedrooms,
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
				}

				const categoryLabel = isRental ? "LETTINGS" : "SALES";
				console.log(
					` [${categoryLabel}] [${propertyAction}] ${detail.title.substring(0, 40)} - ${formatPriceDisplay(
						detail.price,
						isRental,
					)} - ${property.link}`,
				);

				if (propertyAction !== "UNCHANGED") {
					await sleep(500);
				}
			}),
		);
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
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			log.error(` Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeCharters() {
	console.log(`\n 🚀 Starting Charters Estate Agents scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const PROPERTY_TYPES = [
		{
			urlBase:
				"https://www.chartersestateagents.co.uk/property/for-sale/in-hampshire-and-surrey/exclude-sale-agreed/",
			totalPages: 52,
			isRental: false,
			label: "SALES",
		},
		{
			urlBase:
				"https://www.chartersestateagents.co.uk/property/to-rent/in-hampshire-and-surrey/exclude-let-agreed/",
			totalPages: 11,
			isRental: true,
			label: "RENTALS",
		},
	];

	const allRequests = [];

	for (const config of PROPERTY_TYPES) {
		console.log(` 🏠 Enqueuing ${config.label} (${config.totalPages} pages)`);
		for (let pg = 1; pg <= config.totalPages; pg++) {
			allRequests.push({
				url: buildPagedUrl(config.urlBase, pg),
				userData: {
					pageNum: pg,
					isRental: config.isRental,
					label: config.label,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		console.log(" No pages to scrape.");
		return;
	}

	console.log(` Queueing ${allRequests.length} listing pages...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n Completed Charters - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}\n`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeCharters();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		log.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
