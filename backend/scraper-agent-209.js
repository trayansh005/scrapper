// FrostWeb scraper using Playwright with Crawlee
// Agent ID: 209
// Usage:
// node backend/scraper-agent-209.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 209;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const recentPageSignatures = new Map();
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
// PROPERTY TYPES CONFIGURATION
// ============================================================================

const PROPERTY_TYPES = [
	{
		// Sales
		urlBase:
			"https://www.frostweb.co.uk/search/?showstc=+off&showsold=off&department=%21commercial&instruction_type=Sale&ajax_polygon=&ajax_radius=&minprice=&maxprice=",
		totalPages: 62, // 491 properties, 8 per page -> 62 pages
		recordsPerPage: 8,
		isRental: false,
		label: "SALES",
	},
	{
		// Lettings
		urlBase:
			"https://www.frostweb.co.uk/search/?showstc=+off&showsold=off&department=%21commercial&instruction_type=Letting&ajax_polygon=&ajax_radius=&minprice=&maxprice=",
		totalPages: 13, // 98 properties, 8 per page -> 13 pages
		recordsPerPage: 8,
		isRental: true,
		label: "RENTALS",
	},
];

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(` [${label}] Page ${pageNum} - ${request.url}`);

	try {
		await page.waitForSelector("#search-results", { timeout: 20000 });
	} catch (e) {
		console.log(` Search container not found on page ${pageNum}`);
	}

	const properties = await page.evaluate(() => {
		try {
			const container = document.querySelector("#search-results");
			if (!container) return [];
			const items = Array.from(container.querySelectorAll(".row.thing"));
			return items
				.map((el) => {
					const linkEl = el.querySelector(".col-sm-4 a");
					const href = linkEl ? linkEl.getAttribute("href") : null;
					const link = href
						? href.startsWith("http")
							? href
							: "https://www.frostweb.co.uk" + href
						: null;

					const title = el.querySelector("h3")?.textContent?.trim() || "";

					// Price is inside h4 text (may include labels)
					const h4 = el.querySelector("h4");
					const priceText = h4 ? h4.textContent.replace(/\n|\r/g, " ").trim() : "";
					const priceMatch = priceText.match(/£[0-9,]+/);
					const price = priceMatch ? priceMatch[0] : priceText;

					const bedrooms = el.querySelector(".property-bedrooms")?.textContent?.trim() || null;

					return { link, price, title, bedrooms };
				})
				.filter((p) => p.link);
		} catch (e) {
			return [];
		}
	});

	console.log(` Found ${properties.length} properties on page ${pageNum}`);

	const pageSignature = properties
		.map((p) => p.link)
		.slice(0, 5)
		.join("|");
	const signatureKey = isRental ? "LETTINGS" : "SALES";
	const previousSignature = recentPageSignatures.get(signatureKey);
	if (pageSignature && previousSignature === pageSignature) {
		console.log(
			` Warning: ${signatureKey} page ${pageNum} has the same leading links as previous page.`,
		);
	}
	recentPageSignatures.set(signatureKey, pageSignature);

	const batchSize = 2;
	for (let i = 0; i < properties.length; i += batchSize) {
		const batch = properties.slice(i, i + batchSize);

		await Promise.all(
			batch.map(async (property) => {
				if (!property.link) return;

				if (processedUrls.has(property.link)) return;
				processedUrls.add(property.link);

				const price = formatPriceUk(property.price);
				let bedrooms = null;
				const bedMatch = property.bedrooms ? property.bedrooms.match(/\d+/) : null;
				if (bedMatch) bedrooms = parseInt(bedMatch[0]);

				if (!price) {
					console.log(` Skipping update (no price found): ${property.link}`);
					return;
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
			}),
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

async function scrapeFrostWeb() {
	console.log(`\n Starting FrostWeb scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		console.log(` Processing ${type.label} (${type.totalPages} pages)`);

		for (let pg = 1; pg <= type.totalPages; pg++) {
			// For FrostWeb page 1 uses the base URL with query string, subsequent pages use /search/{n}.html?
			let url;
			if (pg === 1) {
				url = type.urlBase;
			} else {
				url = type.urlBase.replace("/search/", `/search/${pg}.html?`);
			}

			allRequests.push({
				url,
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: `${type.label}_PAGE_${pg}`,
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
		`\n Completed FrostWeb - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeFrostWeb();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
