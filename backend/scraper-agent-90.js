// OpenRent scraper using Playwright with direct API batch calls
// Agent ID: 90
// Website: openrent.co.uk

const { chromium } = require("playwright");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

const AGENT_ID = 90;
const logger = createAgentLogger(AGENT_ID);

const USER_AGENT =
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	totalCreated: 0,
	totalUpdated: 0,
	totalSkipped: 0,
	totalErrors: 0,
};

const BATCH_SIZE = 50;

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
	if (process.env.USE_LOCAL_BROWSER === "true") {
		return undefined;
	}
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

async function getBrowser() {
	const wsEndpoint = getBrowserlessEndpoint();
	if (wsEndpoint) {
		try {
			const browser = await chromium.connect(wsEndpoint);
			logger.step("Connected to Browserless service successfully.");
			return browser;
		} catch (err) {
			logger.warn(`Browserless connection failed (${err.message}), falling back to local chromium...`);
		}
	}

	return await chromium.launch({
		headless: true,
		args: ["--no-sandbox", "--disable-setuid-sandbox"],
	});
}

function normalizePropertyUrl(rawUrl) {
	if (!rawUrl) return null;
	try {
		const url = new URL(rawUrl, "https://www.openrent.co.uk");
		return `${url.origin}${url.pathname}`.replace(/\/+$/, "");
	} catch (error) {
		return rawUrl.trim();
	}
}

async function scrapePropertyDetail(context, propertyUrl, property, isRental) {
	const detailPage = await context.newPage();

	try {
		await blockNonEssentialResources({ page: detailPage });

		await detailPage.goto(propertyUrl, { waitUntil: "domcontentloaded", timeout: 45000 });
		await detailPage.waitForTimeout(1000);

		const detailHtml = await detailPage.content();

		const detailData = await detailPage.evaluate(() => {
			const parseFloatSafe = (v) => (Number.isFinite(parseFloat(v)) ? parseFloat(v) : null);
			let lat = null,
				lng = null;
			const mapDiv = document.querySelector("[data-lat][data-lng]");
			if (mapDiv) {
				lat = parseFloatSafe(mapDiv.getAttribute("data-lat"));
				lng = parseFloatSafe(mapDiv.getAttribute("data-lng"));
			}

			const titleHeading = document.querySelector("h1, .property-title");
			const title = titleHeading ? titleHeading.innerText.trim() : null;

			return { lat, lng, title };
		});

		const finalTitle = detailData.title && detailData.title.length > 5 ? detailData.title : property.title;

		const result = await processPropertyWithCoordinates(
			propertyUrl,
			property.price,
			finalTitle,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			detailHtml,
			detailData.lat,
			detailData.lng,
		);

		return { success: true, coordsFound: !!(detailData.lat && detailData.lng), result };
	} catch (error) {
		logger.error(`Detail page error for ${propertyUrl}`, error);
		return { success: false };
	} finally {
		await detailPage.close().catch(() => {});
	}
}

async function fetchMasterPropertyIds(page) {
	const masterUrl = "https://www.openrent.co.uk/properties-to-rent/greater-london?term=Greater%20London&isLive=true";

	await page.goto(masterUrl, {
		waitUntil: "domcontentloaded",
		timeout: 60000,
	});

	const propertyIds = await page.evaluate(() => {
		if (window.PROPERTYIDS && Array.isArray(window.PROPERTYIDS) && window.PROPERTYIDS.length > 0) {
			return window.PROPERTYIDS;
		}

		const scripts = Array.from(document.querySelectorAll("script"));
		for (const s of scripts) {
			const text = s.innerHTML || s.textContent || "";
			const m = text.match(/var\s+PROPERTYIDS\s*=\s*\[([\s\S]*?)\];/);
			if (m) {
				return m[1]
					.split(",")
					.map((i) => parseInt(i.trim()))
					.filter((i) => Number.isInteger(i) && i > 0);
			}
		}
		return [];
	});

	return propertyIds;
}

async function fetchPropertyBatch(context, idsBatch) {
	const query = idsBatch.map((id) => `ids=${id}`).join("&");
	const apiUrl = `https://www.openrent.co.uk/search/propertiesbyid?${query}`;

	const response = await context.request.get(apiUrl, {
		headers: {
			"X-Requested-With": "XMLHttpRequest",
		},
		timeout: 30000,
	});

	if (!response.ok()) {
		throw new Error(`Batch API response returned status ${response.status()}`);
	}

	const data = await response.json();
	return Array.isArray(data) ? data : [];
}

function parseBedroomsFromDetails(details) {
	if (!Array.isArray(details)) return null;

	for (const detail of details) {
		const text = String(detail).trim();
		const match = text.match(/(\d+)\s*(?:bed|bedroom|bedrooms|room|rooms)/i);
		if (match) {
			return parseInt(match[1]);
		}
	}
	return null;
}

async function scrapeOpenRent() {
	logger.step(`Starting OpenRent Scraper (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const isRental = true;
	const label = "RENTALS";

	let browser;
	try {
		browser = await getBrowser();
		const context = await browser.newContext({
			userAgent: USER_AGENT,
			extraHTTPHeaders: {
				"Accept-Language": "en-US,en;q=0.9",
			},
		});

		const page = await context.newPage();

		logger.step("Fetching master property list from OpenRent via Playwright...");
		const allPropertyIds = await fetchMasterPropertyIds(page);
		await page.close().catch(() => {});

		if (!allPropertyIds || allPropertyIds.length === 0) {
			throw new Error("Failed to extract master PROPERTYIDS list from OpenRent DOM.");
		}

		logger.step(`Found ${allPropertyIds.length} active property IDs!`);

		const totalBatches = Math.ceil(allPropertyIds.length / BATCH_SIZE);
		const effectiveStartBatch = Math.max(1, startPage);

		for (let b = effectiveStartBatch - 1; b < totalBatches; b++) {
			const pageNum = b + 1;
			const startIdx = b * BATCH_SIZE;
			const batchIds = allPropertyIds.slice(startIdx, startIdx + BATCH_SIZE);

			logger.page(pageNum, label, `Fetching batch of ${batchIds.length} properties via API...`, totalBatches);

			try {
				const propertiesData = await fetchPropertyBatch(context, batchIds);
				logger.page(pageNum, label, `Received ${propertiesData.length} property items`, totalBatches);

				if (propertiesData.length === 0) continue;

				for (const item of propertiesData) {
					const rawLink = `https://www.openrent.co.uk/${item.id}`;
					const propertyLink = normalizePropertyUrl(rawLink);

					if (!propertyLink) continue;

					if (item.letAgreed || isSoldProperty(item.description || "")) {
						counts.totalSkipped++;
						logger.property(
							pageNum,
							label,
							(item.title || "Property").substring(0, 30),
							"",
							propertyLink,
							isRental,
							totalBatches,
							"SKIPPED",
						);
						continue;
					}

					let price = item.rentPerMonth ? parseFloat(item.rentPerMonth) : null;
					if (!price && item.rentPerWeek) {
						price = parsePrice(item.rentPerWeek);
					}

					if (!price) {
						counts.totalSkipped++;
						continue;
					}

					const title = (item.title || "Property")
						.replace(/\s*[•-]\s*£[\d,]+.*$/gi, "")
						.replace(/£[\d,]+.*$/gi, "")
						.replace(/\s+/g, " ")
						.trim()
						.substring(0, 150);

					const bedrooms = parseBedroomsFromDetails(item.details);

					const propertyObj = {
						link: propertyLink,
						price,
						title,
						bedrooms,
					};

					const dbResult = await updatePriceByPropertyURLOptimized(
						propertyLink,
						price,
						title,
						bedrooms,
						AGENT_ID,
						isRental,
					);

					if (dbResult?.error) {
						counts.totalErrors++;
						continue;
					}

					let propertyAction = "UNCHANGED";
					if (dbResult.updated) propertyAction = "UPDATED";

					if (dbResult?.isExisting && !dbResult.missingData) {
						counts.totalScraped++;
						if (dbResult.updated) counts.totalUpdated++;

						logger.property(
							pageNum,
							label,
							title.substring(0, 30),
							formatPriceDisplay(price, isRental),
							propertyLink,
							isRental,
							totalBatches,
							propertyAction,
						);
						continue;
					}

					// New property or missing data -> visit detail page
					if (!dbResult.isExisting) propertyAction = "CREATED";

					await scrapePropertyDetail(context, propertyLink, propertyObj, isRental);

					counts.totalScraped++;
					counts.totalSaved++;
					if (dbResult?.isExisting) counts.totalUpdated++;
					else counts.totalCreated++;

					logger.property(
						pageNum,
						label,
						title.substring(0, 30),
						formatPriceDisplay(price, isRental),
						propertyLink,
						isRental,
						totalBatches,
						propertyAction,
					);

					if (propertyAction !== "UNCHANGED") {
						await sleep(100);
					}
				}
			} catch (error) {
				counts.totalErrors++;
				logger.error(`Error processing batch ${pageNum}`, error);
			}

			await sleep(100);
		}

		await browser.close().catch(() => {});
	} catch (err) {
		if (browser) await browser.close().catch(() => {});
		throw err;
	}

	logger.step(
		`Finished OpenRent - Total scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Created: ${counts.totalCreated}, Updated: ${counts.totalUpdated}, Skipped: ${counts.totalSkipped}, Errors: ${counts.totalErrors}`,
	);

	if (!isPartialRun) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Skipping updateRemoveStatus.");
	}
}

(async () => {
	try {
		await scrapeOpenRent();
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
