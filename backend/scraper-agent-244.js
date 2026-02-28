const { PlaywrightCrawler } = require("crawlee");
const { updateRemoveStatus } = require("./db");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");

const AGENT_ID = 244;
const logger = createAgentLogger(AGENT_ID);
const AGENT_DISABLED = false; // ← RE-ENABLED

const PROPERTY_TYPES = [
	{
		type: "sales",
		urlBase: "https://www.mypropertybox.co.uk/results-gallery.php?section=sales&ddm_order=2",
		totalPages: 17, // Adjusted based on current site
		isRental: false,
		label: "SALES",
	},
	{
		type: "lettings",
		urlBase: "https://www.mypropertybox.co.uk/results-gallery.php?section=lets&ddm_order=2",
		totalPages: 22, // Adjusted based on current site
		isRental: true,
		label: "RENTALS",
	},
];

/**
 * Scrapes detail page coordinates and property information
 */
async function scrapePropertyDetail(browserContext, property, isRental, price) {
	const detailPage = await browserContext.newPage();
	try {
		await detailPage.goto(property.propertyUrl, { waitUntil: "domcontentloaded", timeout: 45000 });
		const html = await detailPage.content();

		const latLonMatch = html.match(
			/google\.maps\.LatLng\(\s*([-+]?\d*\.?\d+),\s*([-+]?\d*\.?\d+)\s*\)/,
		);
		const lat = latLonMatch ? parseFloat(latLonMatch[1]) : null;
		const lon = latLonMatch ? parseFloat(latLonMatch[2]) : null;

		await processPropertyWithCoordinates(
			property.propertyUrl,
			price,
			property.address,
			property.bedrooms,
			AGENT_ID,
			isRental,
			html,
			lat,
			lon,
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
	const { isRental, pageNum, label, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		// Wait for the property containers
		await page
			.waitForSelector(".col-xl-3.col-lg-6, .col-xl-3, .details", { timeout: 30000 })
			.catch(() => {
				logger.warn(`Property containers not found on page ${pageNum}`, pageNum, label);
			});

		const properties = await page.evaluate((baseUrl) => {
			const results = [];
			// Select all columns that typically contain property cards
			const items = Array.from(document.querySelectorAll('div[class*="col-"]')).filter((el) =>
				el.querySelector(".details"),
			);

			items.forEach((el) => {
				const linkEl = el.querySelector('a[href*="property-details.php"]');
				if (!linkEl) return;

				const href = linkEl.getAttribute("href");

				// Check for sold badge
				const isSold =
					el.querySelector('img.status[alt="Sold"]') !== null ||
					el.querySelector(".image-flash") !== null ||
					el.innerText.match(/Sold|Let Agreed|Under Offer/i);

				if (isSold) return;

				const address = el.querySelector(".address")?.innerText.trim() || "Address not found";
				const priceText = el.querySelector(".price")?.innerText.trim() || "";
				const bedText = el.querySelector(".bedrooms")?.innerText.trim() || "";

				// Parse bedrooms out of text like "2 Bedroom Ground Floor Flat"
				const bedMatch = bedText.match(/(\d+)\s*Bedroom/i);
				const bedrooms = bedMatch ? parseInt(bedMatch[1], 10) : null;

				const propertyUrl = href.startsWith("http") ? href : new URL(href, baseUrl).href;

				results.push({ propertyUrl, priceText, address, bedrooms });
			});
			return results;
		}, "https://www.mypropertybox.co.uk");

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

		for (const property of properties) {
			try {
				if (isSoldProperty(property.address) || isSoldProperty(property.priceText)) {
					continue;
				}

				const priceNum = parsePrice(property.priceText);
				if (!priceNum && !isRental) continue;

				const result = await updatePriceByPropertyURLOptimized(
					property.propertyUrl,
					priceNum,
					property.address,
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
						`New property, scraping details: ${property.propertyUrl}`,
						totalPages,
					);
					await scrapePropertyDetail(page.context(), property, isRental, priceNum);
				}

				logger.property(
					pageNum,
					label,
					property.address,
					formatPriceDisplay(priceNum, isRental) || "Price TBD",
					property.propertyUrl,
					isRental,
					totalPages,
					action,
				);
			} catch (err) {
				logger.error(
					`Error processing property ${property.propertyUrl}: ${err.message}`,
					err,
					pageNum,
					label,
				);
			}
		}
	} catch (err) {
		logger.error(`Error on page ${pageNum}: ${err.message}`, err, pageNum, label);
	}
}

async function run() {
	if (AGENT_DISABLED) {
		logger.warn("Agent is DISABLED. Reason: mypropertybox.co.uk site structure has changed.");
		return;
	}

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(
		`Starting My Property Box scraper${isPartialRun ? ` from page ${startPage}` : ""}...`,
	);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
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
	for (const config of PROPERTY_TYPES) {
		logger.step(`Queueing ${config.label} (${config.totalPages} pages)`);
		for (let pg = Math.max(1, startPage); pg <= config.totalPages; pg++) {
			const url = new URL(config.urlBase);
			url.searchParams.set("page", pg);
			allRequests.push({
				url: url.toString(),
				userData: {
					isRental: config.isRental,
					pageNum: pg,
					label: config.label,
					totalPages: config.totalPages,
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
		logger.warn("Partial run detected. Skipping updateRemoveStatus to prevent data loss.");
	}
	logger.step("All done!");
}

run().catch((err) => {
	logger.error(`Fatal error: ${err.message}`);
	process.exit(1);
});
