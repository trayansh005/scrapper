// Harrods Estates scraper with Playwright extraction of propertyData
// Agent ID: 215
// Website: harrodsestates.com
// Usage:
// node backend/scraper-agent-215.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { updatePriceByPropertyURLOptimized, processPropertyWithCoordinates } = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceUk } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

const AGENT_ID = 215;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
};

const scrapeStartTime = new Date();
const startPageArgument = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
const isPartialRun = startPageArgument > 1;

async function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

const crawler = new PlaywrightCrawler({
	maxConcurrency: 1, // Be polite
	maxRequestRetries: 2,
	navigationTimeoutSecs: 90,
	requestHandlerTimeoutSecs: 600,

	launchContext: {
		launchOptions: {
			headless: true,
			args: ["--no-sandbox", "--disable-setuid-sandbox"],
		},
	},

	preNavigationHooks: [
		async ({ page }) => {
			await blockNonEssentialResources(page);
		},
	],

	async requestHandler({ page, request, crawler }) {
		const { pageNum, isRental, label } = request.userData;

		logger.page(pageNum, request.userData.totalPages || "?", `Processing ${label}`);

		// Extract propertyData from the script tag
		const data = await page.evaluate(() => {
			const scripts = Array.from(document.querySelectorAll("script"));
			for (const s of scripts) {
				const text = s.textContent;
				if (text && text.includes("var propertyData =")) {
					try {
						const match = text.match(/var propertyData = ({.*?});/s);
						if (match) {
							return JSON.parse(match[1]);
						}
					} catch (e) {
						// Continue to next script
					}
				}
			}
			return null;
		});

		if (!data || !data.properties) {
			logger.error(`No propertyData found on page ${pageNum}`);
			return;
		}

		const properties = data.properties;

		// Handle pagination discovery on first page
		if (pageNum === 1) {
			const totalCount = data.pagination?.total_count || 0;
			const pageSize = data.pagination?.page_size || 9;
			const totalPages = Math.ceil(totalCount / pageSize);
			request.userData.totalPages = totalPages;

			for (let p = 2; p <= totalPages; p++) {
				const pagedUrl = request.url.replace(/\/$/, "") + `/page-${p}#/`;
				await crawler.addRequests([
					{
						url: pagedUrl,
						userData: { ...request.userData, pageNum: p, totalPages },
					},
				]);
			}
		}

		for (const item of properties) {
			const link = item.property_url.startsWith("http")
				? item.property_url
				: `https://www.harrodsestates.com${item.property_url}`;
			
			const status = (item.status || "").toLowerCase();
			if (isSoldProperty(status)) {
				logger.property(link, "SKIP", `Status is: ${status}`);
				stats.totalSkipped++;
				continue;
			}

			const numericPrice = parsePrice(item.price);
			const title = item.display_address || "Harrods Property";
			const bedrooms = item.bedrooms || null;
			const lat = item.lat || null;
			const lng = item.lng || null;

			stats.totalFound++;

			// Optimized price check
			const priceCheck = await updatePriceByPropertyURLOptimized(link, numericPrice, AGENT_ID);
			
			if (priceCheck.isExisting) {
				if (priceCheck.updated) {
					logger.property(link, "UPDATED", `Price: ${formatPriceUk(numericPrice)}`);
					stats.totalSaved++;
					await sleep(100);
				} else {
					logger.property(link, "UNCHANGED");
				}
			} else {
				// We have everything in propertyData, including coordinates and bedrooms
				// Skip details page and save directly
				const dbResult = await processPropertyWithCoordinates(
					link,
					numericPrice,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					"", // HTML not needed as we already have data
					lat,
					lng
				);

				if (dbResult.updated || !dbResult.isExisting) {
					logger.property(link, dbResult.updated ? "UPDATED" : "CREATED");
					stats.totalSaved++;
					await sleep(200);
				}
				stats.totalScraped++;
			}
		}
	},
});

async function run() {
	logger.step(`Starting Harrods Estates scraper (Agent ${AGENT_ID})`);

	const startUrls = [
		{
			url: "https://www.harrodsestates.com/properties/sales/status-available#/",
			userData: { pageNum: 1, isRental: false, label: "SALES" },
		},
		{
			url: "https://www.harrodsestates.com/properties/lettings/status-available#/",
			userData: { pageNum: 1, isRental: true, label: "RENTALS" },
		},
	];

	if (isPartialRun) {
		logger.step(`Partial run detected (startPage=${startPageArgument}). Remove status update will be skipped.`);
		// If user passes startPage, we should ideally adjust the startUrls to only include that page onwards.
		// However, Homeflow sites usually work best when we let discovery happen from page 1.
	}

	await crawler.run(startUrls);

	if (!isPartialRun) {
		logger.step("Updating removed status for inactive properties...");
		const removedCount = await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step(`Marked ${removedCount} properties as removed`);
	} else {
		logger.step("Skipping remove status update (Partial run)");
	}

	logger.step(
		`Scrape completed. Found: ${stats.totalFound}, Saved/Updated: ${stats.totalSaved}, Skipped: ${stats.totalSkipped}`
	);
}

run().catch((err) => {
	logger.error(`Fatal error: ${err.message}`);
	process.exit(1);
});
