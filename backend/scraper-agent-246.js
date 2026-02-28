const { PlaywrightCrawler } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

const AGENT_ID = 246;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.simonblyth.co.uk/properties/",
		totalPages: 22,
		isRental: false,
		label: "SALES",
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

		const detailData = await detailPage.evaluate(() => {
			try {
				const data = {
					address: null,
					price: null,
					bedrooms: null,
					lat: null,
					lng: null,
				};

				const h1 = document.querySelector("#single-property h1") || document.querySelector("h1");
				if (h1) data.address = h1.textContent.trim();

				const priceEl =
					document.querySelector("#single-property .price") || document.querySelector(".price");
				if (priceEl) data.price = priceEl.textContent.trim();

				const specsEl = document.querySelector("div.row.sub-title p");
				if (specsEl) {
					const specs = specsEl.innerText || "";
					const bedMatch = specs.match(/Bedrooms:\s*(\d+)/i);
					if (bedMatch) data.bedrooms = bedMatch[1];
				}

				const latEl = document.getElementById("lat");
				const lngEl = document.getElementById("lng");
				if (latEl && lngEl) {
					data.lat = parseFloat(latEl.getAttribute("value"));
					data.lng = parseFloat(lngEl.getAttribute("value"));
				}

				return data;
			} catch (e) {
				return null;
			}
		});

		if (detailData) {
			const priceNum = parsePrice(detailData.price);
			const address = detailData.address || property.title;

			await processPropertyWithCoordinates(
				property.link.trim(),
				priceNum,
				address,
				detailData.bedrooms,
				AGENT_ID,
				isRental,
				html,
				detailData.lat,
				detailData.lng,
			);
			return true;
		}
		return false;
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
		await page.waitForSelector(".property", { timeout: 30000 }).catch(() => {
			logger.warn(`No properties found on page ${pageNum}`, pageNum, label);
		});

		const properties = await page.evaluate(() => {
			try {
				const items = Array.from(document.querySelectorAll(".property"));
				return items
					.map((el) => {
						const statusEl =
							el.querySelector(".status .darker-grey p") || el.querySelector(".status");
						const statusText = statusEl ? statusEl.innerText : "";

						const linkEl =
							el.querySelector("a.button.on-white[href]") ||
							el.querySelector("a[href*='/property/']");
						const link = linkEl ? linkEl.href : null;
						const title = el.querySelector("h3.property_title")?.textContent?.trim() || "";

						const priceEl = el.querySelector("span.price");
						const priceText = priceEl ? priceEl.innerText.trim() : null;

						return { link, title, statusText, priceText };
					})
					.filter((p) => p.link);
			} catch (e) {
				return [];
			}
		});

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

		for (const property of properties) {
			try {
				if (isSoldProperty(property.statusText)) {
					continue;
				}

				const priceNum = parsePrice(property.priceText);

				const result = await updatePriceByPropertyURLOptimized(
					property.link.trim(),
					priceNum,
					property.title,
					null, // Bedrooms unknown at listing level
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
					await scrapePropertyDetail(page.context(), property, isRental);
				}

				logger.property(
					pageNum,
					label,
					property.title,
					formatPriceDisplay(priceNum, isRental),
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
	} catch (err) {
		logger.error(`Error on page ${pageNum}: ${err.message}`, err, pageNum, label);
	}
}

async function run() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Simon Blyth scraper${isPartialRun ? ` from page ${startPage}` : ""}...`);

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
			const url = `${propertyType.urlBase}?radius=1&pagen=${pg}&sort=-created_at&type=buy`;
			allRequests.push({
				url,
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

run()
	.then(() => {
		process.exit(0);
	})
	.catch((err) => {
		logger.error(`Fatal error: ${err.message}`);
		process.exit(1);
	});
