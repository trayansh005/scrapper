// Mistoria Estate Agents scraper using Playwright with Crawlee
// Agent ID: 224
// Usage:
// node backend/scraper-agent-224.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 224;

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

function parsePrice(rawPrice) {
	return formatPriceUk(rawPrice);
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
	await page
		.waitForSelector("li.type-property", { timeout: 15000 })
		.catch(() => console.log(` No property cards found on page ${pageNum}`));

	const properties = await page.evaluate(() => {
		try {
			const items = Array.from(document.querySelectorAll("li.type-property"));

			return items
				.map((el) => {
					try {
						const flagEl = el.querySelector(".flag");
						if (flagEl) {
							const flagText = flagEl.textContent.trim();
							if (flagText.includes("Sold") || flagText.includes("Let Agreed")) {
								return null;
							}
						}

						const linkEl = el.querySelector(".thumbnail a");
						const link = linkEl ? linkEl.href : null;
						if (!link) return null;

						const title = el.querySelector("h3 a")?.textContent?.trim() || "";
						const rawPrice = el.querySelector("div.price")?.textContent?.trim() || "";

						const bedrooms =
							el.querySelector(".room-bedrooms .room-count")?.textContent?.trim() || null;
						const bathrooms =
							el.querySelector(".room-bathrooms .room-count")?.textContent?.trim() || null;
						const receptions =
							el.querySelector(".room-receptions .room-count")?.textContent?.trim() || null;

						const statusText = `${flagEl?.textContent || ""} ${title} ${rawPrice}`.trim();

						return { link, title, priceText: rawPrice, bedrooms, bathrooms, receptions, statusText };
					} catch (e) {
						return null;
					}
				})
				.filter(Boolean);
		} catch (err) {
			return [];
		}
	});

	console.log(` Found ${properties.length} properties on page ${pageNum}`);

	for (const property of properties) {
		if (isSoldProperty(property.statusText || "")) continue;

		const price = parsePrice(property.priceText);
		if (!property.link || !price) continue;

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

		await sleep(350);
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

async function scrapeMistoriaEstateAgents() {
	console.log(`\n Starting Mistoria Estate Agents scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const totalSalesPages = 5;
	const totalLettingsPages = 6;

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	// Build Sales requests
	for (let pg = Math.max(1, startPage); pg <= totalSalesPages; pg++) {
		const url = `${
			"https://mistoriaestateagents.co.uk/property-search/page/"
		}${pg}/?address_keyword&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&department=residential-sales&availability&maximum_bedrooms`;

		allRequests.push({
			url,
			userData: {
				pageNum: pg,
				isRental: false,
				label: `FOR_SALE_${pg}`,
			},
		});
	}

	// Build Lettings requests
	if (startPage === 1) {
		for (let pg = 1; pg <= totalLettingsPages; pg++) {
			const url = `${
				"https://mistoriaestateagents.co.uk/property-search/page/"
			}${pg}/?address_keyword=&department=residential-lettings&availability=&minimum_bedrooms=&maximum_bedrooms=`;

			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					isRental: true,
					label: `TO_LET_${pg}`,
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
		`\n Completed Mistoria Estate Agents - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeMistoriaEstateAgents();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
