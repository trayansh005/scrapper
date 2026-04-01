"use strict";

const { PlaywrightCrawler, sleep } = require("crawlee");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
	extractCoordinatesFromHTML,
} = require("./lib/property-helpers.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { updateRemoveStatus } = require("./db.js");

const AGENT_ID = 260;
const logger = createAgentLogger(AGENT_ID);
const BROWSERLESS_URL =
	process.env.BROWSERLESS_URL || "ws://browserless-e44co4wws040gcokws8k0c00:3000";

const counts = { totalScraped: 0, totalSaved: 0, savedSales: 0, savedRentals: 0 };
const processedUrls = new Set();

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) return route.abort();
		return route.continue();
	});
}

async function handleListingPage({ page, request }) {
	const { pageNum, totalPages, isRental, label } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector(".properties.clear li.property", { timeout: 15000 });
	} catch (e) {
		logger.error(`Listing container not found on page ${pageNum}`, e);
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const cards = document.querySelectorAll(".properties.clear li.property");
		cards.forEach((card) => {
			const linkEl = card.querySelector('a[href*="/property/"]');
			if (!linkEl) return;
			results.push({
				link: linkEl.href,
				title: card.querySelector("h3")?.innerText?.trim() || "Property",
				priceRaw: card.querySelector(".price.fa247price")?.innerText?.trim() || "",
				bedText: card.querySelector("span.bedrooms")?.innerText?.trim() || "",
				statusText: card.querySelector(".flag")?.innerText?.trim() || "",
			});
		});
		return results;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

	for (const property of properties) {
		if (!property.link || isSoldProperty(property.statusText || "")) continue;
		if (processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.page(pageNum, label, `Skipping update (no price found): ${property.link}`, totalPages);
			continue;
		}

		let bedrooms = null;
		const bedMatch = property.bedText.match(/\d+/);
		if (bedMatch) bedrooms = parseInt(bedMatch[0], 10);

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let propertyAction = "UNCHANGED";
		if (result.updated) {
			counts.totalSaved++;
			propertyAction = "UPDATED";
		}

		if (!result.isExisting && !result.error) {
			const detail = await scrapePropertyDetail(page.context(), property.link);
			await processPropertyWithCoordinates(
				property.link,
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
				null,
				detail?.coords?.latitude,
				detail?.coords?.longitude,
			);

			counts.totalSaved++;
			counts.totalScraped++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;
			propertyAction = "CREATED";

			logger.property(
				pageNum,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				totalPages,
				propertyAction,
				detail?.coords?.latitude,
				detail?.coords?.longitude,
			);
		} else {
			if (result.error) propertyAction = "ERROR";
			logger.property(
				pageNum,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				totalPages,
				propertyAction,
			);
		}

		if (propertyAction !== "UNCHANGED") await sleep(500);
	}
}

async function scrapePropertyDetail(browserContext, url) {
	logger.page(null, null, `[Detail] Scraping coordinates: ${url}`);
	await sleep(700);
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
		await detailPage.waitForTimeout(800);
		const html = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(html);
		if (coords.latitude)
			logger.page(
				null,
				null,
				`[Detail] Found coordinates: ${coords.latitude}, ${coords.longitude}`,
			);
		else logger.page(null, null, `[Detail] No coordinates found`);
		return { coords };
	} catch (err) {
		logger.error(`Error scraping detail page ${url}: ${err.message}`);
		return { coords: { latitude: null, longitude: null } };
	} finally {
		await detailPage.close();
	}
}

async function run() {
	const args = process.argv.slice(2);
	const startPage = args[0] ? parseInt(args[0], 10) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Agent ${AGENT_ID} - FreeAgent247 (Agent 4 Style)`);
	logger.step(`Start Page: ${startPage}`);

	const PROPERTY_TYPES = [
		{
			baseUrl: "https://freeagent247.com/buy-property/?department=residential-sales&availability=4",
			totalPages: 10,
			isRental: false,
			label: "SALES",
		},
		{
			baseUrl:
				"https://freeagent247.com/rent-property/?department=residential-lettings&availability=4",
			totalPages: 8,
			isRental: true,
			label: "RENTALS",
		},
	];

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launchOptions: {
				browserWSEndpoint: BROWSERLESS_URL,
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
	});

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} (${type.totalPages} pages)`);
		for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
			// Insert /page/{pg}/ before the query string
			const urlParts = type.baseUrl.split("?");
			const pagedUrl = `${urlParts[0]}/page/${pg}/?${urlParts[1]}`;
			allRequests.push({
				url: pagedUrl,
				userData: {
					pageNum: pg,
					totalPages: type.totalPages,
					isRental: type.isRental,
					label: type.label,
				},
			});
		}
	}

	if (allRequests.length > 0) await crawler.run(allRequests);
	logger.step(
		`Completed Agent ${AGENT_ID} - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`,
	);

	if (startPage === 1) {
		logger.step("Updating remove status...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step("Partial run - skipping updateRemoveStatus");
	}

	logger.step(`Agent ${AGENT_ID} completed successfully`);
}

run().catch((err) => {
	logger.error(`Fatal: ${err.message}`);
	process.exit(1);
});
