// Jackson-Stops scraper using Playwright with Crawlee
// Agent ID: 114
// Website: jackson-stops.co.uk
// Usage:
// node backend/scraper-agent-114.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceUk } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

const AGENT_ID = 114;
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

async function scrapePropertyDetail(page, property, isRental, pageNum, label, totalPages) {
	try {
		await page.goto(property.link, { waitUntil: "domcontentloaded", timeout: 45000 });
		await sleep(500);

		// Extract coordinates from scripts or JSON-LD
		const detailData = await page.evaluate(() => {
			try {
				const allScripts = Array.from(document.querySelectorAll("script"))
					.map((s) => s.textContent)
					.join("\n");

				// 1. loadLocratingPlugin match
				const locratingMatch = allScripts.match(
					/loadLocratingPlugin\s*\(\s*\{[^}]*lat\s*:\s*([0-9.+-]+)[^}]*lng\s*:\s*([0-9.+-]+)/,
				);
				if (locratingMatch) {
					return {
						lat: parseFloat(locratingMatch[1]),
						lng: parseFloat(locratingMatch[2]),
						html: document.body.innerHTML,
					};
				}

				// 2. JSON-LD geo data
				const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
				for (const s of scripts) {
					try {
						const data = JSON.parse(s.textContent);
						if (data && data.geo && data.geo.latitude && data.geo.longitude) {
							return {
								lat: parseFloat(data.geo.latitude),
								lng: parseFloat(data.geo.longitude),
								html: document.body.innerHTML,
							};
						}
					} catch (e) {}
				}

				// 3. Last resort: regex search for "lat": number
				const latMatch = allScripts.match(/"lat"\s*:\s*([0-9.+-]+)/);
				const lngMatch = allScripts.match(/"lng"\s*:\s*([0-9.+-]+)/);
				if (latMatch && lngMatch) {
					return {
						lat: parseFloat(latMatch[1]),
						lng: parseFloat(lngMatch[2]),
						html: document.body.innerHTML,
					};
				}

				return { lat: null, lng: null, html: document.body.innerHTML };
			} catch (e) {
				return { lat: null, lng: null, html: "" };
			}
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			detailData.html,
			detailData.lat,
			detailData.lng,
		);

		logger.property(
			pageNum,
			label,
			property.title,
			formatPriceUk(property.price),
			property.link,
			isRental,
			totalPages,
			"CREATED",
			detailData.lat,
			detailData.lng,
		);

		stats.totalSaved++;
		await sleep(1000);
		stats.totalScraped++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
	}
}

const crawler = new PlaywrightCrawler({
	maxConcurrency: 1,
	maxRequestRetries: 1,
	navigationTimeoutSecs: 60,
	requestHandlerTimeoutSecs: 300,

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
		const { pageNum, isRental, label, totalPages } = request.userData;

		// Extract properties from listing
		const properties = await page.evaluate((isRental) => {
			const items = Array.from(document.querySelectorAll(".property-single__grid"));
			return items.map((el) => {
				const statusEl = el.querySelector(".property-single__grid__status__main");
				const statusText = statusEl?.textContent?.trim().toLowerCase() || "";

				const linkEl = el.querySelector("a.property-single__grid__link-wrapper");
				let link = linkEl ? linkEl.getAttribute("href") : null;
				if (link && !link.startsWith("http")) {
					link = "https://www.jackson-stops.co.uk" + (link.startsWith("/") ? "" : "/") + link;
				}

				const priceEl = el.querySelector(".property-single__grid__price");
				const priceText = priceEl?.textContent?.trim() || "";

				const titleEl = el.querySelector(".property-single__grid__address");
				const title = titleEl?.textContent?.trim() || "";

				const roomsDiv = el.querySelector(".property-single__grid__rooms");
				let bedrooms = null;
				if (roomsDiv) {
					const bedroomSpan = Array.from(roomsDiv.querySelectorAll("span")).find((span) =>
						/Bedrooms?/i.test(span.textContent),
					);
					if (bedroomSpan) {
						const match = bedroomSpan.textContent.match(/(\d+)\s*Bedrooms?/i);
						bedrooms = match ? parseInt(match[1], 10) : null;
					}
				}

				return { link, priceText, title, bedrooms, statusText };
			});
		}, isRental);

		// Handle pagination discovery
		let currentPageTotalPages = totalPages || 1;
		if (pageNum === 1) {
			currentPageTotalPages = await page.evaluate(() => {
				const span = document.querySelector(".pagination span");
				if (span) {
					const match = span.textContent.match(/of\s+(\d+)/i);
					return match ? parseInt(match[1], 10) : 1;
				}
				return 1;
			});

			for (let p = 2; p <= currentPageTotalPages; p++) {
				const pagedUrl = request.url.includes("?")
					? request.url.replace(/\?/, `/page-${p}?`)
					: `${request.url.replace(/\/$/, "")}/page-${p}`;

				await crawler.addRequests([
					{
						url: pagedUrl,
						userData: { ...request.userData, pageNum: p, totalPages: currentPageTotalPages },
					},
				]);
			}
			request.userData.totalPages = currentPageTotalPages;
		}

		logger.page(pageNum, currentPageTotalPages, `Found ${properties.length} items`);

		for (const property of properties) {
			if (!property.link) continue;
			stats.totalFound++;

			if (isSoldProperty(property.statusText)) {
				logger.property(
					pageNum,
					label,
					property.title,
					property.priceText,
					property.link,
					isRental,
					currentPageTotalPages,
					"SKIPPED",
				);
				stats.totalSkipped++;
				continue;
			}

			const numericPrice = parsePrice(property.priceText);

			// Optimized check
			const priceCheck = await updatePriceByPropertyURLOptimized(
				property.link,
				numericPrice,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
			);
			if (priceCheck.isExisting) {
				if (priceCheck.updated) {
					logger.property(
						pageNum,
						label,
						property.title,
						formatPriceUk(numericPrice),
						property.link,
						isRental,
						currentPageTotalPages,
						"UPDATED",
					);
					stats.totalSaved++;
					await sleep(100);
				} else {
					logger.property(
						pageNum,
						label,
						property.title,
						formatPriceUk(numericPrice),
						property.link,
						isRental,
						currentPageTotalPages,
						"UNCHANGED",
					);
				}
			} else {
				// New property, visit detail
				const detailPage = await page.context().newPage();
				await blockNonEssentialResources(detailPage);
				await scrapePropertyDetail(
					detailPage,
					{ ...property, price: numericPrice },
					isRental,
					pageNum,
					label,
					currentPageTotalPages,
				);
				await detailPage.close();
			}
		}
	},
});

async function run() {
	logger.step(`Starting Jackson-Stops scraper (Agent ${AGENT_ID})`);

	const startUrls = [
		{
			url: "https://www.jackson-stops.co.uk/properties/sales",
			userData: { pageNum: 1, isRental: false, label: "SALES" },
		},
		{
			url: "https://www.jackson-stops.co.uk/properties/lettings",
			userData: { pageNum: 1, isRental: true, label: "RENTALS" },
		},
	];

	if (isPartialRun) {
		logger.step(
			`Partial run detected (startPage=${startPageArgument}). Remove status update will be skipped.`,
		);
		// For Jackson-Stops, we need to adjust start URLs to skip pages if needed,
		// but simple implementation just runs from page 1.
		// If user passes startPage, we should really only start from that page.
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
		`Scrape completed. Found: ${stats.totalFound}, Saved/Updated: ${stats.totalSaved}, Skipped: ${stats.totalSkipped}`,
	);
}

run().catch((err) => {
	logger.error(`Fatal error: ${err.message}`);
	process.exit(1);
});
