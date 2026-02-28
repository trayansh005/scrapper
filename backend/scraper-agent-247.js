const { PlaywrightCrawler } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");

const AGENT_ID = 247;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		urlBase:
			"https://www.darlows.co.uk/search/?IsPurchase=True&Location=Wales%2C+UK&SearchDistance=50&Latitude=52.1306607&Longitude=-3.7837117&NumberOfResults=50",
		totalPages: 8,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase:
			"https://www.darlows.co.uk/search/?IsPurchase=False&Location=Wales%2C+UK&SearchDistance=50&Latitude=52.1306607&Longitude=-3.7837117&NumberOfResults=50",
		totalPages: 1,
		isRental: true,
		label: "LETTINGS",
	},
];

/**
 * Scrapes detail page coordinates and property information
 */
async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();
	try {
		await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 90000 });
		const html = await detailPage.content();

		const detailData = await detailPage.evaluate(() => {
			try {
				const data = {
					price: null,
					bedrooms: null,
					address: null,
					lat: null,
					lng: null,
				};

				const priceEl = document.querySelector("p.price");
				if (priceEl) data.price = priceEl.textContent.trim();

				const metaEl = document.querySelector("p.meta");
				if (metaEl) {
					const metaText = metaEl.textContent || "";
					const parts = metaText.split("●");
					if (parts.length >= 2) data.address = parts[1].trim();

					const bedMatch = metaText.match(/(\d+)\s+Bedroom/i);
					if (bedMatch) data.bedrooms = bedMatch[1];
				}

				if (!data.address) {
					const headingEl = document.querySelector("h3.property-heading a");
					if (headingEl) data.address = headingEl.textContent.trim();
				}

				if (!data.bedrooms) {
					const bedMatch = document.body.innerText.match(/\b(\d+)\s+Bedroom(?:s)?\b/i);
					if (bedMatch) data.bedrooms = bedMatch[1];
				}

				const onclickEls = Array.from(document.querySelectorAll("[onclick*='openStreetView']"));
				for (const el of onclickEls) {
					const onclick = el.getAttribute("onclick");
					const coordMatch = onclick.match(/openStreetView\([^,]*,\s*([-0-9.]+),\s*([-0-9.]+)/);
					if (coordMatch) {
						data.lat = parseFloat(coordMatch[1]);
						data.lng = parseFloat(coordMatch[2]);
						break;
					}
				}

				if (!data.lat) {
					const similarLink = document.querySelector("a[href*='Latitude='], a[href*='latitude=']");
					if (similarLink) {
						const href = similarLink.getAttribute("href");
						const latMatch = href.match(/Latitude=([-0-9.]+)/);
						const lngMatch = href.match(/Longitude=([-0-9.]+)/);
						if (latMatch && lngMatch) {
							data.lat = parseFloat(latMatch[1]);
							data.lng = parseFloat(lngMatch[1]);
						}
					}
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
		// Wait for either the properties or a "no results" message
		await page
			.waitForSelector("article.property-result, .no-results", { timeout: 30000 })
			.catch(() => {
				logger.warn(`Timeout waiting for properties on page ${pageNum}`, pageNum, label);
			});

		const { properties, debugInfo } = await page.evaluate(() => {
			try {
				const results = [];
				const debug = [];
				const articles = Array.from(document.querySelectorAll("article.property-result"));

				debug.push(`Found ${articles.length} articles`);

				for (const article of articles) {
					// Skip CTA/Valuation articles
					if (
						article.classList.contains("cta") ||
						article.querySelector("img[src*='valuing-homes']")
					) {
						debug.push(`Skipping CTA article`);
						continue;
					}

					const text = article.innerText;
					const isSold =
						text.match(/Sold STC|Subject to contract|Let Agreed|Sale Agreed|Under Offer/i) ||
						(typeof isSoldProperty === "function" && isSoldProperty(text));

					if (isSold) {
						debug.push(`Skipping sold property: ${text.substring(0, 30)}...`);
						continue;
					}

					const titleLink = article.querySelector("h3.property-heading a, a.no-decoration");
					if (!titleLink) {
						debug.push(`No title link found in article`);
						continue;
					}

					// Ensure absolute URL
					let href = titleLink.href;
					if (href && !href.startsWith("http")) {
						href = new URL(href, window.location.origin).href;
					}

					const metaEl = article.querySelector("p.meta");
					let bedrooms = null;
					let address = null;
					if (metaEl) {
						const metaText = metaEl.textContent || "";
						const parts = metaText.split("●");
						if (parts.length >= 2) address = parts[1].trim();

						const bedMatch = metaText.match(/(\d+)\s+Bedroom/i);
						if (bedMatch) bedrooms = bedMatch[1];
					}

					results.push({
						link: href,
						title: titleLink.innerText.trim(),
						priceText: article.querySelector("p.price")?.innerText.trim() || null,
						bedrooms,
						address,
					});
				}
				return { properties: results, debugInfo: debug };
			} catch (e) {
				return { properties: [], debugInfo: [`Error in evaluate: ${e.message}`] };
			}
		});

		if (debugInfo && debugInfo.length > 0) {
			logger.info(`Debug [Page ${pageNum}]: ${debugInfo.join(" | ")}`);
		}

		logger.page(pageNum, label, `Found ${properties.length} valid properties`, totalPages);

		for (const property of properties) {
			try {
				const priceNum = parsePrice(property.priceText);
				const address = property.address || property.title;

				const result = await updatePriceByPropertyURLOptimized(
					property.link.trim(),
					priceNum,
					address,
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

	logger.step(`Starting Darlows scraper${isPartialRun ? ` from page ${startPage}` : ""}...`);

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
				url: `${propertyType.urlBase}&Page=${pg}`,
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
