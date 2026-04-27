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

async function autoScroll(page) {
	await page.evaluate(async () => {
		await new Promise((resolve) => {
			let totalHeight = 0;
			const distance = 800;

			const timer = setInterval(() => {
				const scrollHeight = document.body.scrollHeight;

				window.scrollBy(0, distance);
				totalHeight += distance;

				// stop when reached bottom
				if (totalHeight >= scrollHeight - window.innerHeight) {
					clearInterval(timer);
					resolve();
				}
			}, 400);
		});
	});
}

async function handleListingPage({ page, request }) {
	logger.page(1, "SALES", request.url);

	await page.waitForLoadState("domcontentloaded");

	// 🔥 IMPORTANT: wait for JS rendering
	await page.waitForTimeout(5000);

	// 🔥 Scroll to load all properties
	await autoScroll(page);

	// wait again after scroll
	await page.waitForTimeout(3000);

	const properties = await page.evaluate(() => {
		const results = [];

		const cards = document.querySelectorAll('a[href*="/property/"]');

		cards.forEach((card) => {
			const link = card.href;

			const title =
				card.querySelector("p")?.innerText?.trim() || "Property";

			const priceRaw =
				card.innerText.match(/£[\d,]+/)?.[0] || "";

			const bedText =
				card.innerText.match(/\d+\s*bed/i)?.[0] || "";

			results.push({
				link,
				title,
				priceRaw,
				bedText,
				statusText: "",
			});
		});

		return results;
	});

	console.log("🔥 Properties found:", properties.length);

	const { label, isRental } = request.userData;

for (const property of properties) {
	if (!property.link) continue;
	if (processedUrls.has(property.link)) continue;

	processedUrls.add(property.link);

	const price = parsePrice(property.priceRaw);
	if (!price) {
		logger.page(1, label, `Skipping (no price): ${property.link}`);
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
		isRental
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
			detail?.coords?.longitude
		);

		counts.totalSaved++;
		counts.totalScraped++;

		if (isRental) counts.savedRentals++;
		else counts.savedSales++;

		propertyAction = "CREATED";
	}

	logger.property(
		1,
		label,
		property.title.substring(0, 40),
		formatPriceDisplay(price, isRental),
		property.link,
		isRental,
		1,
		propertyAction
	);

	if (propertyAction !== "UNCHANGED") await sleep(300);
}
}

async function scrapePropertyDetail(browserContext, url) {
	logger.page(null, null, `[Detail] Scraping coordinates: ${url}`);
	await sleep(700);

	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(url, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// ✅ Wait for page to stabilize
		await detailPage.waitForTimeout(1500);

		// ✅ CLICK "More Details" button
		try {
			const moreDetailsBtn = await detailPage.locator("text=More Details");
			if (await moreDetailsBtn.isVisible({ timeout: 3000 })) {
				await moreDetailsBtn.click();
				logger.page(null, null, `[Detail] Clicked More Details`);

				// wait for dynamic content load
				await detailPage.waitForTimeout(1500);
			}
		} catch (e) {
			logger.page(null, null, `[Detail] More Details button not found`);
		}

		// ✅ Now get updated HTML
		const html = await detailPage.content();

		const coords = await extractCoordinatesFromHTML(html);

		if (coords.latitude) {
			logger.page(
				null,
				null,
				`[Detail] Found coordinates: ${coords.latitude}, ${coords.longitude}`
			);
		} else {
			logger.page(null, null, `[Detail] No coordinates found`);
		}

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
			baseUrl: "https://freeagent247.com/buy?status=For+Sale",
			isRental: false,
			label: "SALES",
		},
		// {
		// 	baseUrl: "https://freeagent247.com/rent?status=To+Rent",
		// 	isRental: true,
		// 	label: "RENTALS",
		// },
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
		for (const type of PROPERTY_TYPES) {
			logger.step(`Queueing ${type.label}`);

			allRequests.push({
				url: type.baseUrl,
				userData: {
					pageNum: 1,
					totalPages: 1,
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
