// VHHomes scraper using Playwright with Crawlee
// Agent ID: 11
// Usage:
// node backend/scraper-agent-11.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 11;

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

		const detailData = await detailPage.evaluate(() => {
			try {
				const data = {
					address: null,
					price: null,
					bedrooms: null,
					lat: null,
					lng: null,
				};

				// Try to extract from h1 title
				const h1 = document.querySelector("h1");
				if (h1) {
					data.address = h1.textContent
						.trim()
						.replace(/\s*-\s*For Sale.*$/i, "")
						.replace(/\s*-\s*To Rent.*$/i, "");
				}

				// Try to extract price from visible text
				const bodyText = document.body.innerText;
				const priceMatch = bodyText.match(/£\s*([\d,]+(?:,\d{3})*)/);
				if (priceMatch) {
					data.price = priceMatch[1].replace(/,/g, "");
				}

				// Extract bedrooms from icon blocks first (supports img icons and svg title="rooms")
				const roomContainer =
					document.querySelector("._property-rooms-container") ||
					document.querySelector("[class*='rooms-container']");
				if (roomContainer) {
					const spans = Array.from(roomContainer.querySelectorAll("span"));
					for (const span of spans) {
						const titleText =
							span.querySelector("svg title")?.textContent?.trim()?.toLowerCase() ||
							span.querySelector("img")?.getAttribute("alt")?.trim()?.toLowerCase() ||
							"";
						if (!titleText || titleText.includes("bath")) continue;
						if (!titleText.includes("room")) continue;

						const numberMatch = (span.textContent || "").match(/\d+/);
						if (numberMatch) {
							data.bedrooms = parseInt(numberMatch[0], 10);
							break;
						}
					}
				}

				if (data.bedrooms == null) {
					const iconRows = Array.from(
						document.querySelectorAll("img[alt*='room' i], img[alt*='bath' i]"),
					);
					for (const img of iconRows) {
						const alt = (img.getAttribute("alt") || "").toLowerCase();
						if (alt.includes("bath")) continue;
						const parentText = img.parentElement?.textContent || "";
						const numberMatch = parentText.match(/\d+/);
						if (numberMatch) {
							data.bedrooms = parseInt(numberMatch[0], 10);
							break;
						}
					}
				}

				if (data.bedrooms == null) {
					const bedMatch = bodyText.match(/(\d+)\s*bed(room)?/i);
					if (bedMatch) data.bedrooms = parseInt(bedMatch[1], 10);
				}

				// Primary coordinate source on V&H: `_coordinates` JSON in inline script
				const scripts = Array.from(document.querySelectorAll("script"));
				for (const script of scripts) {
					const text = script.textContent || "";
					if (!text.includes("_coordinates")) continue;
					const coordMatch = text.match(
						/"latitude"\s*:\s*"?(-?\d+\.\d+)"?[\s\S]*?"longitude"\s*:\s*"?(-?\d+\.\d+)"?/i,
					);
					if (coordMatch) {
						data.lat = parseFloat(coordMatch[1]);
						data.lng = parseFloat(coordMatch[2]);
						break;
					}
				}

				// Fallback: map links with q=lat,lng or @lat,lng
				if (data.lat == null || data.lng == null) {
					const mapLink = document.querySelector(
						'a[href*="google.com/maps"], a[href*="goo.gl/maps"]',
					);
					const href = mapLink?.getAttribute("href") || "";
					const coordsMatch =
						href.match(/q=(-?\d+\.\d+),(-?\d+\.\d+)/i) || href.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/i);
					if (coordsMatch) {
						data.lat = parseFloat(coordsMatch[1]);
						data.lng = parseFloat(coordsMatch[2]);
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
			bedrooms: detailData.bedrooms || property.bedrooms || null,
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
		await page.waitForSelector("a", { timeout: 15000 });
	} catch (e) {
		console.log(` Page load issue on page ${pageNum}`);
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();

			// VHHomes uses generic divs with links inside. Properties are in structure like:
			// <div><a href="/buy/[id]-[slug]">... property details ...</a></div>
			const propertyLinks = Array.from(
				document.querySelectorAll('a[href*="/buy/"], a[href*="/rent/"]'),
			).filter((link) => {
				const href = link.getAttribute("href") || "";
				return !href.includes("#") && !href.includes("property-maintenance");
			});

			for (const link of propertyLinks) {
				const href = link.getAttribute("href");
				if (!href) continue;

				const fullUrl = href.startsWith("http") ? href : new URL(href, window.location.origin).href;

				// Skip duplicates
				if (seenLinks.has(fullUrl)) continue;
				seenLinks.add(fullUrl);

				// Extract title from link text or nearest heading
				let title = link.textContent?.trim() || "Property";
				if (title.length > 200) {
					title = title.substring(0, 100);
				}

				// Get price and status from the same container/sibling text
				const container = link.closest("div, li") || link;
				const containerText = container.innerText || "";
				let bedrooms = null;

				const roomContainer =
					container.querySelector("._property-rooms-container") ||
					container.querySelector("[class*='rooms-container']");
				if (roomContainer) {
					const spans = Array.from(roomContainer.querySelectorAll("span"));
					for (const span of spans) {
						const titleText =
							span.querySelector("svg title")?.textContent?.trim()?.toLowerCase() ||
							span.querySelector("img")?.getAttribute("alt")?.trim()?.toLowerCase() ||
							"";
						if (!titleText || titleText.includes("bath")) continue;
						if (!titleText.includes("room")) continue;

						const numberMatch = (span.textContent || "").match(/\d+/);
						if (numberMatch) {
							bedrooms = parseInt(numberMatch[0], 10);
							break;
						}
					}
				}

				if (bedrooms == null) {
					const infoRows = Array.from(container.querySelectorAll("div"));
					for (const row of infoRows) {
						const roomImg = row.querySelector('img[alt*="room" i], img[src*="room" i]');
						const bathImg = row.querySelector('img[alt*="bath" i], img[src*="bath" i]');
						if (roomImg && !bathImg) {
							const numberMatch = (row.textContent || "").match(/\d+/);
							if (numberMatch) {
								bedrooms = parseInt(numberMatch[0], 10);
								break;
							}
						}
					}
				}

				if (bedrooms == null) {
					const bedWordMatch = `${title} ${containerText}`.match(/(\d+)\s*bed(room)?/i);
					if (bedWordMatch) bedrooms = parseInt(bedWordMatch[1], 10);
				}

				results.push({
					link: fullUrl,
					title,
					bedrooms,
					statusText: containerText,
				});
			}

			return results;
		} catch (e) {
			console.log("Error extracting properties:", e.message);
			return [];
		}
	});

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
		} else if (result.isExisting && result.updated) {
			// If it's existing but updated (price change)
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

	// Check for next page and queue it
	const nextPageNum = pageNum + 1;
	const nextPageUrl = isRental
		? `https://vhhomes.co.uk/search?type=rent&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394&page=${nextPageNum}`
		: `https://vhhomes.co.uk/search?type=buy&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394&page=${nextPageNum}`;

	// Simple check: if we found properties on this page, try next
	if (properties.length >= 10 && pageNum < 50) {
		console.log(` Queuing next page: ${nextPageNum}`);
		await crawler.addRequests([
			{
				url: nextPageUrl,
				userData: { pageNum: nextPageNum, isRental, label },
			},
		]);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

let crawler; // Global crawler instance for recursion

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

async function scrapeVHHomes() {
	console.log(`\n Starting VHHomes scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	// Scrape sales
	console.log(`\n\n SALES\n`);
	crawler = createCrawler(browserWSEndpoint);
	try {
		await crawler.addRequests([
			{
				url: `https://vhhomes.co.uk/search?type=buy&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394&page=${startPage}`,
				userData: { pageNum: startPage, isRental: false, label: "SALES" },
			},
		]);
		await crawler.run();
	} catch (error) {
		console.error(`Error during sales scraping: ${error.message}`);
	} finally {
		await crawler.teardown();
	}

	// Clear processed URLs for rentals
	processedUrls.clear();

	// Scrape rentals
	console.log(`\n\n LETTINGS\n`);
	crawler = createCrawler(browserWSEndpoint);
	try {
		await crawler.addRequests([
			{
				url: `https://vhhomes.co.uk/search?type=rent&status=available&per-page=10&sort=price-high&status-ids=371,385,391,1394&page=${startPage}`,
				userData: { pageNum: startPage, isRental: true, label: "LETTINGS" },
			},
		]);
		await crawler.run();
	} catch (error) {
		console.error(`Error during rentals scraping: ${error.message}`);
	} finally {
		await crawler.teardown();
	}

	// Print summary
	console.log(`\n
========================================
 AGENT ${AGENT_ID} SUMMARY
========================================
Total scraped: ${stats.totalScraped}
Total updated: ${stats.totalSaved}
New sales: ${stats.savedSales}
New rentals: ${stats.savedRentals}
========================================\n`);
}

// Run the scraper
scrapeVHHomes().catch(console.error);
