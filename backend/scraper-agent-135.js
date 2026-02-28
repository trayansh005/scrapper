// Taylors scraper using Playwright with Crawlee
// Agent ID: 135
//
// Usage:
// node backend/scraper-agent-135.js

const { PlaywrightCrawler, log } = require("crawlee");
const { chromium } = require("playwright");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML: genericExtractCoords } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 135;

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
		// first try generic extractor
		let coords = genericExtractCoords(htmlContent);
		// fallback to Taylors-specific script
		if (!coords.latitude && !coords.longitude) {
			const latMatch = htmlContent.match(/ga4_property_latitude:\s*([0-9.-]+)/);
			const lngMatch = htmlContent.match(/ga4_property_longitude:\s*([0-9.-]+)/);
			if (latMatch && lngMatch) {
				coords = { latitude: parseFloat(latMatch[1]), longitude: parseFloat(lngMatch[1]) };
			}
		}
		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
			},
		};
	} catch (err) {
		console.log(` Error scraping detail page ${property.link}: ${err.message}`);
		return null;
	} finally {
		await detailPage.close();
	}
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlPath: "properties/sales/status-available/most-recent-first",
		totalRecords: 1075,
		recordsPerPage: 10,
		isRental: false,
		label: "SALES",
	},
	{
		urlPath: "properties/lettings/status-available/most-recent-first",
		totalRecords: 208,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(` [${label}] Page ${pageNum} - ${request.url}`);

	try {
		await page.waitForSelector(".card--list .card, .hf-property-results .card", { timeout: 30000 });
	} catch (e) {
		console.log(` Listing container not found on page ${pageNum}`);
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const cards = Array.from(
			document.querySelectorAll(".card--list .card, .hf-property-results .card"),
		);
		for (const card of cards) {
			try {
				let linkEl = card.querySelector("a.card__link");
				if (!linkEl) linkEl = card.querySelector("a");
				let link = linkEl ? linkEl.getAttribute("href") : null;
				if (!link) continue;
				const fullLink = link.startsWith("http")
					? link
					: "https://www.taylorsestateagents.co.uk" + link;

				let title = "";
				let titleEl = card.querySelector(".card__text-title");
				if (!titleEl) titleEl = card.querySelector(".card__text-content");
				if (titleEl) title = titleEl.textContent.trim();

				let price = null;
				const priceEl = card.querySelector(".card__heading");
				if (priceEl) {
					price = priceEl.textContent.trim().replace(/[^0-9]/g, "");
				}

				let bedrooms = null;
				const bedroomEls = card.querySelectorAll(".card-content__spec-list-number");
				if (bedroomEls.length > 0) {
					const m = bedroomEls[0].textContent.trim().match(/(\d+)/);
					if (m) bedrooms = m[1];
				}

				if (fullLink && price && title) {
					results.push({ link: fullLink, title, price, bedrooms });
				}
			} catch (err) {
				// ignore
			}
		}
		return results;
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
		const batchActions = await Promise.all(
			batch.map(async (property) => {
				if (!property.link) return "UNCHANGED";

				if (processedUrls.has(property.link)) return "UNCHANGED";
				processedUrls.add(property.link);

				const numericPrice = Number(property.price.toString().replace(/,/g, ""));
				const price = numericPrice.toLocaleString("en-GB");
				let bedrooms = null;
				if (property.bedrooms) {
					const m = property.bedrooms.match(/\d+/);
					if (m) bedrooms = parseInt(m[0]);
				}

				if (!price) {
					console.log(` Skipping update (no price found): ${property.link}`);
					return "UNCHANGED";
				}

				const result = await updatePriceByPropertyURLOptimized(
					property.link,
					price,
					property.title,
					bedrooms,
					AGENT_ID,
					isRental,
				);

				let action = "UNCHANGED";
				if (result.updated) {
					stats.totalSaved++;
					action = "UPDATED";
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
					action = "CREATED";
				}

				const categoryLabel = isRental ? "LETTINGS" : "SALES";
				console.log(
					` [${categoryLabel}] [${action}] ${property.title.substring(0, 40)} - ${formatPriceDisplay(
						price,
						isRental,
					)} - ${property.link}`,
				);
				return action;
			}),
		);
		if (batchActions.some((a) => a !== "UNCHANGED")) {
			await sleep(500);
		}
	}
}

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: chromium,
			launchOptions: {
				headless: true,
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

async function scrapeTaylors() {
	console.log(`\n Starting Taylors scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
		for (let pg = 1; pg <= totalPages; pg++) {
			allRequests.push({
				url: `https://www.taylorsestateagents.co.uk/${type.urlPath}/page-${pg}#/`,
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
		`\n Completed Taylors - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// Main execution
(async () => {
	try {
		await scrapeTaylors();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
