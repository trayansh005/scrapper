// Dixons Estate Agents scraper using Playwright with Crawlee
// Agent ID: 243
// Usage:
// node backend/scraper-agent-243.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 243;

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
	await sleep(1000); // 1s second delay on Dixons

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
					lat: null,
					lng: null,
				};

				const html = document.documentElement.innerHTML;
				const latMatch = html.match(/"latitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
				const lonMatch = html.match(/"longitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);

				if (latMatch) data.lat = parseFloat(latMatch[1]);
				if (lonMatch) data.lng = parseFloat(lonMatch[1]);

				return data;
			} catch (e) {
				return null;
			}
		});

		return {
			coords: {
				latitude: detailData?.lat || null,
				longitude: detailData?.lng || null,
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
		.waitForSelector(".card", { timeout: 30000 })
		.catch(() => console.log(` No properties found on page ${pageNum}`));

	const properties = await page.evaluate(() => {
		try {
			const cards = document.querySelectorAll(".card");
			const results = [];
			const baseUrl = window.location.origin;

			cards.forEach((card) => {
				const priceText = card.querySelector(".card__heading")?.innerText || "";
				if (!priceText) return;

				const title = card.querySelector(".card__text-content")?.innerText || "Property";
				const relativeUrl = card.querySelector("a.card__link")?.getAttribute("href");
				if (!relativeUrl) return;

				const link = relativeUrl.startsWith("http") ? relativeUrl : baseUrl + relativeUrl;

				// Status check
				const statusText = card.innerText || "";
				
				// Bedrooms count
				let bedrooms = null;
				const specs = card.querySelectorAll(".card-content__spec-list-item");
				specs.forEach((spec) => {
					if (spec.querySelector(".icon-bedroom")) {
						const val = spec.querySelector(".card-content__spec-list-number")?.innerText;
						if (val) bedrooms = parseInt(val, 10);
					}
				});

				results.push({ link, title, statusText, priceText, bedrooms });
			});
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

		const detail = await scrapePropertyDetail(page.context(), property);
		const price = formatPriceUk(property.priceText);

		if (!price) {
			console.log(` Skipping update (no price found): ${property.link}`);
			continue;
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
			stats.totalSaved++;
		}

		if (!result.isExisting && !result.error) {
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

async function scrapeDixons() {
	console.log(`\n Starting Dixons Estate Agents scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	// Approx counts: 710 records / 10 per page = 71 pages sales
	// 170 records / 10 per page = 17 pages lettings
	const totalSalesPages = 75; 
	const totalLettingsPages = 20;

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let pg = Math.max(1, startPage); pg <= totalSalesPages; pg++) {
		const url = `https://www.dixonsestateagents.co.uk/properties/sales/status-available/most-recent-first/page-${pg}#/`;

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
			const url = `https://www.dixonsestateagents.co.uk/properties/lettings/status-available/most-recent-first/page-${pg}#/`;

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
		`\n Completed Dixons - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeDixons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
