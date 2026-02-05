// Hamptons lettings scraper using Playwright with Crawlee
// Agent ID: 108
//
// Usage:
// node backend/scraper-agent-108.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 108;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePrice(priceText) {
	if (!priceText) return null;
	const digits = priceText.replace(/[^0-9]/g, "");
	if (!digits) return null;

	return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
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
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		// Block unnecessary resources
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
			timeout: 30000,
		});

		const htmlContent = await detailPage.content();

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (error) {
		console.error(` Error scraping detail page ${property.link}:`, error.message);
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

	await page.waitForTimeout(2000);
	await page.waitForSelector("article.property-card", { timeout: 20000 }).catch(() => {
		console.log(` No property cards found on page ${pageNum}`);
	});

	const properties = await page.evaluate(() => {
		const containers = Array.from(document.querySelectorAll("article.property-card"));
		const map = new Map();

		for (const container of containers) {
			const linkEl = container.querySelector("a.property-card__link");
			const rawHref = linkEl ? linkEl.getAttribute("href") : null;
			const link = rawHref ? new URL(rawHref, "https://www.hamptons.co.uk").href : null;

			const propId = linkEl ? linkEl.getAttribute("data-property-id") || null : null;
			const priceText = container.querySelector(".property-card__price")?.textContent?.trim() || "";
			const title = container.querySelector(".property-card__title")?.textContent?.trim() || "";

			let bedrooms = null;
			const bedEl = container.querySelector(".property-card__bedbath .property-card__bedbath-item");
			if (bedEl) {
				const bedText = bedEl.textContent?.trim() || "";
				const m = bedText.match(/(\d+)/);
				if (m) bedrooms = parseInt(m[1]);
			}

			const key = propId || link;
			if (!key) continue;

			if (!map.has(key)) {
				map.set(key, { id: propId, link, title, priceText, bedrooms });
			}
		}

		return Array.from(map.values());
	});

	console.log(` Found ${properties.length} properties on page ${pageNum}`);

	for (const property of properties) {
		if (!property.link) continue;

		const price = parsePrice(property.priceText);
		if (!price) continue;

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
			console.log(` Scraping detail for new property: ${property.title}`);
			await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
		}

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

async function scrapeHamptons() {
	console.log(`\n Starting Hamptons scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	// Config
	const totalLettingsPages = 100;

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	for (let p = Math.max(1, startPage); p <= totalLettingsPages; p++) {
		const url =
			p === 1
				? "https://www.hamptons.co.uk/properties/lettings/status-available"
				: `https://www.hamptons.co.uk/properties/lettings/status-available/page-${p}`;

		allRequests.push({
			url,
			userData: {
				pageNum: p,
				isRental: true,
				label: `HAMPTONS_LETTINGS_${p}`,
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
		`\n Completed Hamptons - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeHamptons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
