const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
	formatPriceUk,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

const AGENT_ID = 228;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://www.starkingsandwatson.co.uk/buying/property-search/page/",
		params: "/?department=sales&location&lat&lng&radius=3&min-price&max-price&bedrooms",
		totalPages: 57,
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://www.starkingsandwatson.co.uk/letting/property-search/page/",
		params: "",
		totalPages: 5,
		isRental: true,
		label: "RENTALS",
	},
];

/**
 * Scrapes detail page coordinates and property information
 */
async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();
	try {
		await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 60000 });
		const html = await detailPage.content();

		await processPropertyWithCoordinates(
			property.link.trim(),
			formatPriceUk(property.price),
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			html,
		);
		return true;
	} catch (err) {
		logger.error(`Detail scrape failed: ${err.message}`, err);
		return false;
	} finally {
		await detailPage.close();
	}
}

/**
 * Handles listing page results
 */
async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page
			.waitForSelector(".card.inview-trigger-animation-fade-in-up-sm", { timeout: 30000 })
			.catch(() => {
				logger.warn(`No listing container found on page ${pageNum}`, pageNum, label);
			});

		const properties = await page.evaluate(() => {
			try {
				const items = Array.from(
					document.querySelectorAll(".card.inview-trigger-animation-fade-in-up-sm"),
				);
				return items
					.map((el) => {
						const statusLabel = el.querySelector(".card__label")?.innerText?.trim() || "";
						const imageFlash = el.querySelector(".image-flash")?.innerText?.trim() || "";
						const combinedLabel = (statusLabel + " " + imageFlash).toUpperCase();

						const linkEl = el.querySelector("a[href*='/property/']");
						const link = linkEl ? linkEl.href : null;
						const title = el.querySelector(".card__title")?.innerText.trim() || "";
						const rawPrice = el.querySelector(".card__text")?.innerText.trim() || "";

						const iconItems = Array.from(el.querySelectorAll(".icons__item"));
						const bedrooms = iconItems[0]?.querySelector(".icons__text")?.innerText.trim() || null;

						return { link, title, rawPrice, bedrooms, combinedLabel };
					})
					.filter((p) => p.link);
			} catch (err) {
				return [];
			}
		});

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

		for (const property of properties) {
			try {
				if (isSoldProperty(property.combinedLabel)) {
					continue;
				}

				const priceNum = parsePrice(property.rawPrice);
				if (!priceNum) continue;

				const result = await updatePriceByPropertyURLOptimized(
					property.link.trim(),
					priceNum,
					property.title,
					property.bedrooms,
					AGENT_ID,
					isRental,
				);

				let action = "UNCHANGED";
				if (!result.isExisting && !result.error) {
					action = "CREATED";
					logger.page(
						pageNum,
						label,
						`New property, scraping details: ${property.link}`,
						totalPages,
					);
					property.price = priceNum;
					await scrapePropertyDetail(page.context(), property, isRental);
				} else if (result.updated) {
					action = "UPDATED";
				}

				logger.property(
					pageNum,
					label,
					property.title,
					formatPriceUk(priceNum),
					property.link,
					isRental,
					totalPages,
					action,
				);
			} catch (err) {
				logger.error(
					`Error processing property ${property.link}: ${err.message}`,
					err,
					pageNum,
					label,
				);
			}
		}
	} catch (error) {
		logger.error(`Error in ${label} page ${pageNum}: ${error.message}`, error, pageNum, label);
	}
}

async function run() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(
		`Starting Starkings and Watson scraper${isPartialRun ? ` from page ${startPage}` : ""}...`,
	);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launchOptions: {
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ request, page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed to process request: ${request.url}`);
		},
	});

	const allRequests = [];
	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Queueing ${propertyType.label} (${propertyType.totalPages} pages)`);
		for (let pg = Math.max(1, startPage); pg <= propertyType.totalPages; pg++) {
			allRequests.push({
				url: `${propertyType.baseUrl}${pg}${propertyType.params}`,
				userData: {
					pageNum: pg,
					isRental: propertyType.isRental,
					label: propertyType.label,
					totalPages: propertyType.totalPages,
				},
			});
		}
	}

	if (allRequests.length > 0) {
		await crawler.run(allRequests);
	} else {
		logger.warn("No requests to process.");
	}

	if (!isPartialRun) {
		logger.step("Scraping complete. Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
	logger.step("All done!");
}

run().catch((err) => {
	logger.error(`Fatal error: ${err.message}`);
	process.exit(1);
});
