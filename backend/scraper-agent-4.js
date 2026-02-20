// Marsh & Parsons scraper using Playwright with Crawlee
// Agent ID: 4
// Usage:
// node backend/scraper-agent-4.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 4;

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

		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);

		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
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
		await page.waitForSelector("a[href*='/property/'] h3", { timeout: 15000 });
	} catch (e) {
		console.log(` Listing container not found on page ${pageNum}`);
	}

	const properties = await page.evaluate(() => {
		try {
			const results = [];
			const seenLinks = new Set();

			// Find all property links - they contain h3 and property info
			const propertyLinks = Array.from(document.querySelectorAll("a[href*='/property/']")).filter(
				(link) => {
					return link.querySelector("h3") !== null; // Ensure it has a title (h3)
				},
			);

			for (const linkEl of propertyLinks) {
				let href = linkEl.getAttribute("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				// Extract title from h3
				const title = linkEl.querySelector("h3")?.textContent?.trim() || "Property";

				// Extract price - it's in a generic with £ symbol
				let priceRaw = "";
				const allText = linkEl.innerText;
				const priceMatch = allText.match(/£[\d,]+(?:,\d{3})*/);
				if (priceMatch) {
					priceRaw = priceMatch[0];
				}

				// Extract bedrooms - text after bed icon
				let bedText = "";
				const bedImg = linkEl.querySelector("img[src*='bed']");
				if (bedImg && bedImg.parentElement) {
					bedText = bedImg.parentElement.textContent?.trim() || "";
				}

				const statusText = linkEl.innerText || "";

				results.push({ link, title, priceRaw, bedText, statusText });
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

		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = formatPriceUk(property.priceRaw);
		let bedrooms = null;
		const bedMatch = property.bedText.match(/\d+/);
		if (bedMatch) bedrooms = parseInt(bedMatch[0]);

		if (!price) {
			console.log(` Skipping update (no price found): ${property.link}`);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			stats.totalSaved++;
		}

		if (!result.isExisting && !result.error) {
			const detail = await scrapePropertyDetail(page.context(), property);

			await updatePriceByPropertyURL(
				property.link.trim(),
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				detail?.coords?.latitude || null,
				detail?.coords?.longitude || null,
			);

			stats.totalSaved++;
			stats.totalScraped++;
			if (isRental) stats.savedRentals++;
			else stats.savedSales++;
		}

		const categoryLabel = isRental ? "LETTINGS" : "SALES";
		console.log(
			` [${categoryLabel}] ${property.title.substring(0, 40)} - ${formatPriceDisplay(
				price,
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

async function scrapeMarshParsons() {
	console.log(`\n Starting Marsh & Parsons scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const totalSalesPages = 30; // Based on original script

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let pg = Math.max(1, startPage); pg <= totalSalesPages; pg++) {
		const url = `https://www.marshandparsons.co.uk/properties-for-sale/london/?filters=exclude_sold%2Cexclude_under_offer&page=${pg}`;

		allRequests.push({
			url,
			userData: {
				pageNum: pg,
				isRental: false,
				label: `SALES_PAGE_${pg}`,
			},
		});
	}

	if (allRequests.length === 0) {
		console.log(" No pages to scrape with current arguments.");
		return;
	}

	console.log(` Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n Completed Marsh & Parsons - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeMarshParsons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
