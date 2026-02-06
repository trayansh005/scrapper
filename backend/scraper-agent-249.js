// Linley & Simpson scraper using Playwright with Crawlee
// Agent ID: 249
// Usage:
// node backend/scraper-agent-249.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 249;

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

		await detailPage.waitForTimeout(1500);

		try {
			const locationAnchor = detailPage.locator("#map-holder, iframe#location-map").first();
			if ((await locationAnchor.count()) > 0) {
				await locationAnchor.scrollIntoViewIfNeeded({ timeout: 8000 });
			} else {
				await detailPage.evaluate(() => {
					const scrollEl = document.scrollingElement || document.documentElement;
					if (scrollEl) window.scrollTo(0, scrollEl.scrollHeight);
				});
			}

			await detailPage.waitForTimeout(1500);
			await detailPage.waitForFunction(
				() => {
					const iframe = document.querySelector("iframe#location-map");
					const src = iframe && (iframe.getAttribute("src") || iframe.src);
					return !!src && src.includes("lat=") && src.includes("lng=");
				},
				{ timeout: 10000 },
			);
		} catch (e) {
			console.log(` Location iframe not ready: ${e?.message || e}`);
		}

		const detailData = await detailPage.evaluate(() => {
			try {
				const data = {
					price: null,
					bedrooms: null,
					address: null,
					lat: null,
					lng: null,
				};

				const mapForm = document.querySelector("form[action*='all_plugins.aspx?']");
				if (mapForm) {
					const action = mapForm.getAttribute("action") || "";
					try {
						const actionUrl = new URL(action, window.location.origin);
						const latParam = actionUrl.searchParams.get("lat");
						const lngParam = actionUrl.searchParams.get("lng");
						if (latParam) data.lat = parseFloat(latParam);
						if (lngParam) data.lng = parseFloat(lngParam);
					} catch (e) {}
				}

				const locationIframe = document.querySelector("iframe#location-map");
				if (locationIframe) {
					const src = locationIframe.getAttribute("src") || locationIframe.src;
					if (src) {
						const latMatch = src.match(/[?&]lat=([0-9.-]+)/);
						const lngMatch = src.match(/[?&]lng=([0-9.-]+)/);
						if (latMatch) data.lat = parseFloat(latMatch[1]);
						if (lngMatch) data.lng = parseFloat(lngMatch[1]);
					}
				}

				const mapElements = Array.from(
					document.querySelectorAll(".streetview_toggle, .map_toggle, [data-lat]"),
				);
				for (const el of mapElements) {
					let lat = el.getAttribute("data-lat") || el.getAttribute("lat");
					let lng = el.getAttribute("data-lng") || el.getAttribute("lng");
					if (!lat) {
						const onclick = el.getAttribute("onclick");
						if (onclick) {
							const matches = onclick.match(/([0-9.-]{4,}),\s*([0-9.-]{4,})/);
							if (matches) {
								lat = matches[1];
								lng = matches[2];
							}
						}
					}

					if (lat && lng) {
						data.lat = parseFloat(lat);
						data.lng = parseFloat(lng);
						break;
					}
				}

				if (!data.lat) {
					const iframes = Array.from(document.querySelectorAll("iframe"));
					for (const iframe of iframes) {
						const src = iframe.src;
						if (src && (src.includes("lat=") || src.includes("maps?q="))) {
							const latMatch = src.match(/lat=([0-9.-]+)/);
							const lngMatch = src.match(/lng=([0-9.-]+)/);
							if (latMatch) data.lat = parseFloat(latMatch[1]);
							if (lngMatch) data.lng = parseFloat(lngMatch[1]);

							if (!data.lat) {
								const qMatch = src.match(/q=([0-9.-]+),([0-9.-]+)/);
								if (qMatch) {
									data.lat = parseFloat(qMatch[1]);
									data.lng = parseFloat(qMatch[2]);
								}
							}

							if (data.lat) break;
						}
					}
				}

				const scripts = Array.from(
					document.querySelectorAll("script[type='application/ld+json']"),
				);
				for (const script of scripts) {
					try {
						const json = JSON.parse(script.textContent);

						if (json["@graph"] && Array.isArray(json["@graph"])) {
							for (const item of json["@graph"]) {
								if (item["@type"] === "Place" && item.address) {
									const addr = item.address;
									if (addr.streetAddress) {
										data.address = `${addr.streetAddress}, ${
											addr.addressLocality || ""
										} ${addr.postalCode || ""}`.trim();
									}
								}
								if (item["@type"] === "Offer" && item.price) {
									data.price = item.price.toString();
								}
							}
						}
						if (
							(json["@type"] === "Residence" ||
								json["@type"] === "SingleFamilyResidence") &&
							json.name
						) {
							if (!data.address) data.address = json.name;
						}
						if ((json["@type"] === "Offer" || json["@type"] === "Product") && json.price) {
							data.price = json.price.toString();
						}
					} catch (e) {}
				}

				if (!data.address) {
					const h1 = document.querySelector("h1");
					if (h1) data.address = h1.textContent.trim();
				}

				if (!data.price) {
					const getText = (el) => (el ? el.innerText || el.textContent : "");
					const checkSelectors = [
						"div[class*='price']",
						"span[class*='price']",
						"h2",
						"h3",
						".banner-text",
						".overlay-text",
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
						const bodyText = `${document.body?.innerText || ""}\n${
							document.body?.textContent || ""
						}`;
						const pcmMatch = bodyText.match(/£\s*([\d,]+)\s*p\.?c\.?m\.?/i);
						if (pcmMatch) {
							data.price = pcmMatch[1].replace(/,/g, "");
						} else {
							const rawMatches = bodyText.matchAll(
								/(?:^|\s|>)£\s*([\d,]+)(?:\s|<|$)/g,
							);
							for (const m of rawMatches) {
								const val = parseInt(m[1].replace(/,/g, ""));
								if (val > 300 && val < 20000) {
									data.price = m[1].replace(/,/g, "");
									break;
								}
							}
						}
					}
				}

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

	try {
		const propertyListSelector = "ul#property-list, div.listing, main";
		await page.waitForSelector(propertyListSelector, { timeout: 15000 });
	} catch (e) {
		console.log(` Listing container not found on page ${pageNum}`);
	}

	const properties = await page.evaluate((isRental) => {
		try {
			const results = [];
			const seenLinks = new Set();
			
			// Select all items that look like property links
			const anchors = Array.from(document.querySelectorAll("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']"));
			
			for (const anchor of anchors) {
				const href = anchor.getAttribute("href");
				if (!href || href.includes("/book-a-viewing/") || href.includes("/request-a-valuation/")) continue;
				
				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				// Find the closest container that holds this property's info
				const container = anchor.closest("li") || anchor.closest("div > div") || anchor;
				
				const title = container.querySelector("h3")?.textContent?.trim() || 
							 anchor.textContent?.trim() || 
							 "Property";
				
				const statusText = container.innerText || "";
				
				results.push({ link, title, statusText });
			}
			return results;
		} catch (e) {
			return [];
		}
	}, isRental);

	console.log(` Found ${properties.length} properties on page ${pageNum}`);

	for (const property of properties) {
		if (!property.link) continue;

		if (isSoldProperty(property.statusText || "")) continue;

		if (processedUrls.has(property.link)) {
			console.log(` Skipping duplicate URL: ${property.link.substring(0, 60)}...`);
			continue;
		}
		processedUrls.add(property.link);

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

async function scrapeLinleyAndSimpson() {
	console.log(`\n Starting Linley & Simpson scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const totalSalesPages = 19;
	const totalLettingsPages = 35;

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	// for (let pg = Math.max(1, startPage); pg <= totalSalesPages; pg++) {
	// 	const url =
	// 		pg === 1
	// 			? "https://www.linleyandsimpson.co.uk/property/for-sale/in-yorkshire/exclude-sale-agreed/"
	// 			: `https://www.linleyandsimpson.co.uk/property/for-sale/in-yorkshire/exclude-sale-agreed/page-${pg}/`;

	// 	allRequests.push({
	// 		url,
	// 		userData: {
	// 			pageNum: pg,
	// 			isRental: false,
	// 			label: `SALES_PAGE_${pg}`,
	// 		},
	// 	});
	// }

	// Build Lettings requests
	if (startPage === 1) {
		for (let pg = 1; pg <= totalLettingsPages; pg++) {
			const url =
				pg === 1
					? "https://www.linleyandsimpson.co.uk/property/to-rent/in-yorkshire/exclude-let-agreed/"
					: `https://www.linleyandsimpson.co.uk/property/to-rent/in-yorkshire/exclude-let-agreed/page-${pg}/`;

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
		`\n Completed Linley & Simpson - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeLinleyAndSimpson();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
