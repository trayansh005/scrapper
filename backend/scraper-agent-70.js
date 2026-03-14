const { PlaywrightCrawler, sleep, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, formatPriceUk, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 70;
const logger = createAgentLogger(AGENT_ID);

const BASE_URL = "https://www.fineandcountry.co.uk";

const PROPERTY_TYPES = [
	{
		urlPath: "sales/property-for-sale",
		isRental: false,
		label: "SALES",
		totalPages: 355,
	},
	{
		urlPath: "lettings/property-to-rent",
		isRental: true,
		label: "LETTINGS",
		totalPages: 21,
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITIES
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

/**
 * Scrapes detail page inline (Agent 4 style)
 */
async function scrapePropertyDetail(browserContext, property) {
	await sleep(500);
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// Extract coordinates from JSON-LD or HTML
		const data = await detailPage.evaluate(() => {
			const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
			for (const script of scripts) {
				try {
					const json = JSON.parse(script.textContent);
					const findGeo = (obj) => {
						if (!obj || typeof obj !== "object") return null;
						if (obj["@type"] === "GeoCoordinates" && obj.latitude && obj.longitude) {
							return { latitude: parseFloat(obj.latitude), longitude: parseFloat(obj.longitude) };
						}
						if (Array.isArray(obj)) {
							for (const item of obj) {
								const res = findGeo(item);
								if (res) return res;
							}
						} else {
							for (const key in obj) {
								const res = findGeo(obj[key]);
								if (res) return res;
							}
						}
						return null;
					};
					const res = findGeo(json);
					if (res) return res;
				} catch (e) {}
			}
			return { latitude: null, longitude: null };
		});

		return {
			coords: data,
			html: await detailPage.content(),
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// MAIN HANDLER
// ============================================================================

async function handleListingPage({ request, page }) {
	const { pageNum, label, isRental, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	await page.waitForSelector(".cards-properties", { timeout: 30000 }).catch(() => null);

	const properties = await page.evaluate(() => {
		const cards = Array.from(document.querySelectorAll(".card-property"));
		return cards.map((card) => {
			const linkEl = card.querySelector(".property-title-link");
			const titleEl = card.querySelector(".property-title-link span");
			const priceEl = card.querySelector(".property-price .text-gold");
			
			const rooms = Array.from(card.querySelectorAll(".card__list-rooms li p")).map(p => p.textContent.trim());
			const bedroomsText = rooms.find(r => /^\s*\d+\s*$/.test(r) || r.toLowerCase().includes("bed"));

			let priceText = priceEl ? priceEl.textContent.trim() : "";
			if (priceEl && priceEl.querySelector(".converted_price")) {
				const temp = priceEl.cloneNode(true);
				const converted = temp.querySelector(".converted_price");
				if (converted) converted.remove();
				priceText = temp.textContent.trim();
			}

			return {
				link: linkEl ? linkEl.href : "",
				title: titleEl ? titleEl.textContent.trim() : "",
				priceText: priceText,
				bedrooms: bedroomsText || "",
			};
		});
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const prop of properties) {
		if (!prop.link || processedUrls.has(prop.link)) continue;
		processedUrls.add(prop.link);

		if (isSoldProperty(prop.title)) {
			logger.property(pageNum, label, prop.title, prop.priceText, prop.link, isRental, totalPages, "SKIPPED");
			continue;
		}

		const formattedPrice = formatPriceUk(prop.priceText);
		if (!formattedPrice) {
			logger.error(`No price found for ${prop.link}`, null, pageNum, label);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			prop.link,
			formattedPrice,
			prop.title,
			prop.bedrooms,
			AGENT_ID,
			isRental
		);

		let action = "UNCHANGED";

		if (result.updated) {
			action = "UPDATED";
			counts.totalSaved++;
		}

		if (!result.isExisting && !result.error) {
			logger.step(`[Detail] Scraping coordinates: ${prop.title}`);
			const detail = await scrapePropertyDetail(page.context(), prop);
			
			const coords = detail?.coords || { latitude: null, longitude: null };

			await processPropertyWithCoordinates(
				prop.link,
				formattedPrice,
				prop.title,
				prop.bedrooms,
				AGENT_ID,
				isRental,
				detail?.html || null,
				coords.latitude,
				coords.longitude
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
			action = "CREATED";
			logger.property(
				pageNum,
				label,
				prop.title.substring(0, 50),
				formatPriceDisplay(formattedPrice, isRental),
				prop.link,
				isRental,
				totalPages,
				action,
				coords.latitude,
				coords.longitude
			);
		} else if (result.error) {
			action = "ERROR";
		}

		if (action !== "CREATED") {
			logger.property(
				pageNum,
				label,
				prop.title.substring(0, 50),
				formatPriceDisplay(formattedPrice, isRental),
				prop.link,
				isRental,
				totalPages,
				action
			);
		}

		if (action !== "UNCHANGED") {
			await sleep(500);
		}
	}
}

// ============================================================================
// RUNNER
// ============================================================================

async function run() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Fine & Country Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	
	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 600, // Large timeout for inline detail scraping

		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1920, height: 1080 },
			},
		},

		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],

		requestHandler: handleListingPage,

		failedRequestHandler: ({ request }) => {
			logger.error(`Failed: ${request.url}`);
		},
	});

	const initialRequests = [];
	for (const type of PROPERTY_TYPES) {
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			initialRequests.push({
				url: `${BASE_URL}/${type.urlPath}/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${pg}`,
				userData: {
					pageNum: pg,
					label: type.label,
					totalPages: type.totalPages,
					isRental: type.isRental,
				},
			});
		}
	}

	if (initialRequests.length > 0) {
		await crawler.run(initialRequests);
	}

	logger.step(`Completed Fine & Country - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, LETTINGS: ${counts.savedRentals}`);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Bypassing updateRemoveStatus.");
	}

	logger.step("All done!");
}

run().catch((err) => {
	logger.error("Fatal error", err);
	process.exit(1);
});
