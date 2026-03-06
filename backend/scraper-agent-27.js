const { PlaywrightCrawler, Dataset, log } = require("crawlee");
const cheerio = require("cheerio");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers");
const { parsePrice } = require("./lib/property-helpers");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 27;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

const processedUrls = new Set();

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Scrapes coordinates and details from property page
 */
async function scrapePropertyDetail(browserContext, propertyUrl) {
	const page = await browserContext.newPage();
	await blockNonEssentialResources(page);
	
	try {
		await page.goto(propertyUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
		await page.waitForTimeout(1000);
		
		const data = await page.evaluate(() => {
			let lat = null;
			let lon = null;
			
			try {
				const jsonLDText = document.querySelector("script.yoast-schema-graph")?.textContent;
				if (jsonLDText) {
					const json = JSON.parse(jsonLDText);
					const graph = json["@graph"] || [];
					
					let geoObj = graph.find(obj => 
						obj["@type"] === "GeoCoordinates" || 
						(Array.isArray(obj["@type"]) && obj["@type"].includes("GeoCoordinates"))
					);
					
					if (!geoObj) {
						const parentObj = graph.find(obj => obj.geo);
						if (parentObj) geoObj = parentObj.geo;
					}
					
					if (geoObj) {
						lat = geoObj.latitude;
						lon = geoObj.longitude;
					}
				}
			} catch (e) {}
			
			return { lat, lon };
		});
		
		return data;
	} catch (e) {
		logger.error(`Error scraping detail page ${propertyUrl}: ${e.message}`);
		return null;
	} finally {
		await page.close();
	}
}

async function run() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Agent ${AGENT_ID}: Frank Harris (via Livin Estate Agents structure)...`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 600,

		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],

		async requestHandler({ page, request, enqueueLinks }) {
			const { pageNum = 1, totalPages = 10, label = "LISTING" } = request.userData;
			logger.page(pageNum, label, request.url, totalPages);

			await page.waitForSelector("ul.properties li", { timeout: 15000 }).catch(() => null);
			
			const content = await page.content();
			const $ = cheerio.load(content);

			const rawProperties = [];
			$("ul.properties li").each((i, el) => {
				const $li = $(el);
				const title = $li.find("h3.h4").text().trim() || $li.find("h3").text().trim();
				const priceText = $li.find(".price").text().trim();
				const price = parsePrice(priceText);
				const link = $li.find("h3.h4 a, h3 a").attr("href");
				const bedroomsText = $li.find(".room-bedrooms .room-count").text().trim();
				const bedrooms = parseInt(bedroomsText) || null;

				if (link && price > 0) {
					const absoluteLink = link.startsWith("http") ? link : `https://livinestateagents.co.uk${link}`;
					rawProperties.push({ title, price, url: absoluteLink, bedrooms });
				}
			});

			logger.page(pageNum, label, `Found ${rawProperties.length} properties`, totalPages);

			for (const prop of rawProperties) {
				try {
					if (processedUrls.has(prop.url)) continue;
					processedUrls.add(prop.url);

					// 1. Try optimized update
					const result = await updatePriceByPropertyURLOptimized(
						prop.url,
						prop.price,
						prop.title,
						prop.bedrooms,
						AGENT_ID,
						false // isRental
					);

					let propertyAction = "UNCHANGED";
					if (result.updated) {
						stats.totalSaved++;
						propertyAction = "UPDATED";
					}

					let lat = null;
					let lon = null;

					// 2. Scrape detail only if NEW
					if (!result.isExisting && !result.error) {
						const detail = await scrapePropertyDetail(page.context(), prop.url);
						if (detail) {
							lat = detail.lat;
							lon = detail.lon;
							
							await processPropertyWithCoordinates(
								prop.url,
								prop.price,
								prop.title,
								prop.bedrooms,
								AGENT_ID,
								false,
								null,
								lat,
								lon
							);
							
							stats.totalSaved++;
							stats.totalScraped++;
							propertyAction = "CREATED";
						}
					}

					logger.property(
						pageNum,
						label,
						prop.title,
						`£${prop.price.toLocaleString()}`,
						prop.url,
						false,
						totalPages,
						propertyAction,
						lat,
						lon
					);

					if (propertyAction !== "UNCHANGED") {
						await sleep(500);
					}
				} catch (err) {
					logger.error(`Error processing property ${prop.url}: ${err.message}`);
				}
			}

			// Pagination
			const nextLink = $("a.next.page-numbers").attr("href");
			if (nextLink) {
				await enqueueLinks({
					urls: [nextLink],
					userData: {
						pageNum: pageNum + 1,
						totalPages,
						label: "LISTING",
					},
				});
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Request ${request.url} failed.`);
		},
	});

	await crawler.run([
		{
			url: "https://livinestateagents.co.uk/property-search/",
			userData: { pageNum: 1, totalPages: 10, label: "LISTING" },
		},
	]);

	logger.step(`Agent ${AGENT_ID} completed. Total Saved: ${stats.totalSaved}`);
	logger.step(`Updating remove status...`);
	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

if (require.main === module) {
	run().catch((err) => {
		console.error("Agent 27 failed:", err);
		process.exit(1);
	});
}

module.exports = { run };

