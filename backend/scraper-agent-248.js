// Newton Fallowell scraper using Playwright with Crawlee
// Agent ID: 248
// Usage:
// node backend/scraper-agent-248.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 248;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
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

async function scrapePropertyDetail(browserContext, property) {
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
		console.log(` Error scraping detail page ${property.link}: ${error.message}`);
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

	await page
		.waitForSelector("a[href*='/property/']", { timeout: 30000 })
		.catch(() => console.log(` No properties found on page ${pageNum}`));

	const properties = await page.evaluate(() => {
		try {
			const items = Array.from(document.querySelectorAll("a[href*='/property/']"));
			const seenLinks = new Set();
			const results = [];

			for (const el of items) {
				let href = el.getAttribute("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				if (!link.includes("/property/")) continue;

				const container =
					el.closest("div[class*='property']") || el.closest("article") || el.closest("div");
				const title = el.querySelector("h")?.textContent?.trim() || "Property";

				const cardHtml = container?.innerHTML || el.innerHTML;
				if (
					cardHtml.includes("Sale Agreed") ||
					cardHtml.includes("Let Agreed") ||
					cardHtml.includes("Sold STC")
				) {
					continue;
				}

				const statusText = container?.innerText || el.innerText || "";
				results.push({ link, title, statusText });
			}

			return results;
		} catch (e) {
			return [];
		}
	});

	console.log(` Found ${properties.length} properties on page ${pageNum}`);

	for (const property of properties) {
		if (!property.link) continue;
		if (isSoldProperty(property.statusText || "")) continue;

		const detail = await scrapePropertyDetail(page.context(), property);
		if (!detail || !detail.price) {
			console.log(` Skipping update (no price found): ${property.link}`);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			detail.price,
			detail.title,
			detail.bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			stats.totalSaved++;
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
		}

		const categoryLabel = isRental ? "LETTINGS" : "SALES";
		console.log(
			` [${categoryLabel}] ${detail.title.substring(0, 40)} - ${formatPriceDisplay(
				detail.price,
				isRental,
			)} - ${property.link}`,
		);

		await sleep(500);
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
			console.error(` Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeNewtonFallowell() {
	console.log(`\n Starting Newton Fallowell scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const totalSalesPages = 139;
	const totalLettingsPages = 18;

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let pg = Math.max(1, startPage); pg <= totalSalesPages; pg++) {
		const url = `${"https://www.newtonfallowell.co.uk/properties/sales"}/?per_page=11&drawMap=&address=&address_lat_lng=&price_min=&price_max=&bedrooms_min=-1&hide_under_offer=on&yield_min=&yield_max=&pg=${pg}`;

		allRequests.push({
			url,
			userData: {
				pageNum: pg,
				isRental: false,
				label: `SALES_PAGE_${pg}`,
			},
		});
	}

	// Build Lettings requests
	if (startPage === 1) {
		for (let pg = 1; pg <= totalLettingsPages; pg++) {
			const url = `${"https://www.newtonfallowell.co.uk/properties/lettings"}/?per_page=11&drawMap=&address=&address_lat_lng=&price_min=&price_max=&bedrooms_min=-1&hide_let_agreed=on&yield_min=&yield_max=&pg=${pg}`;

			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					isRental: true,
					label: `LETTINGS_PAGE_${pg}`,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		console.log(" No pages to scrape with current arguments.");
		return;
	}

	console.log(` Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n Completed Newton Fallowell - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeNewtonFallowell();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
