const { PlaywrightCrawler, Dataset } = require("crawlee");
const cheerio = require("cheerio");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers");
const { parsePrice } = require("./lib/property-helpers");

async function run() {
	console.log("Starting Agent 27: Livin Estate Agents...");

	const crawler = new PlaywrightCrawler({
		maxRequestsPerCrawl: 500,
		requestHandlerTimeoutSecs: 60,

		async requestHandler({ page, request, enqueueLinks }) {
			const url = request.url;
			console.log(`Processing: ${url}`);

			if (url.includes("/property-search/")) {
				// Listing page
				await page.waitForSelector("ul.properties li", { timeout: 10000 }).catch(() => null);
				const content = await page.content();
				const $ = cheerio.load(content);

				const properties = [];
				$("ul.properties li").each((i, el) => {
					const $li = $(el);
					const title = $li.find("h3.h4").text().trim() || $li.find("h3").text().trim();
					const priceText = $li.find(".price").text().trim();
					const price = parsePrice(priceText);
					const link = $li.find("h3.h4 a, h3 a").attr("href");
					const bedrooms = $li.find(".room-bedrooms .room-count").text().trim();

					if (link && price > 0) {
						const absoluteLink = link.startsWith("http")
							? link
							: `https://livinestateagents.co.uk${link}`;
						properties.push({
							title,
							price,
							url: absoluteLink,
							bedrooms: parseInt(bedrooms) || null,
							agentId: 27,
						});
					}
				});

				console.log(`Found ${properties.length} valid properties on this page.`);

				for (const prop of properties) {
					const isNewOrPriceChanged = await updatePriceByPropertyURLOptimized(
						prop.url,
						prop.price,
						prop.title,
						prop.agentId,
					);
					if (isNewOrPriceChanged) {
						await enqueueLinks({
							urls: [prop.url],
							userData: { propertyData: prop },
						});
					}
				}

				// Pagination
				const nextLink = $("a.next.page-numbers").attr("href");
				if (nextLink) {
					await enqueueLinks({ urls: [nextLink] });
				}
			} else {
				// Detail page
				const propertyData = request.userData.propertyData;
				const content = await page.content();
				const $ = cheerio.load(content);

				let lat = null;
				let lon = null;

				// Extract coordinates from Yoast JSON LD
				try {
					const jsonLDText = $("script.yoast-schema-graph").text();
					if (jsonLDText) {
						const data = JSON.parse(jsonLDText);
						const graph = data["@graph"] || [];

						// Look for GeoCoordinates object directly or inside another object
						let geoObj = null;

						// Search for an object that IS GeoCoordinates
						geoObj = graph.find(
							(obj) =>
								obj["@type"] === "GeoCoordinates" ||
								(Array.isArray(obj["@type"]) && obj["@type"].includes("GeoCoordinates")),
						);

						// If not found, look for an object that HAS a geo property
						if (!geoObj) {
							const parentObj = graph.find(
								(obj) =>
									obj.geo &&
									(obj.geo["@type"] === "GeoCoordinates" ||
										(Array.isArray(obj.geo["@type"]) &&
											obj.geo["@type"].includes("GeoCoordinates"))),
							);
							if (parentObj) geoObj = parentObj.geo;
						}

						if (geoObj) {
							lat = geoObj.latitude;
							lon = geoObj.longitude;
						}
					}
				} catch (e) {
					console.error(`Error parsing JSON LD for ${url}:`, e.message);
				}

				console.log(`Extracted coords for ${url}: ${lat}, ${lon}`);

				await processPropertyWithCoordinates(
					propertyData.title,
					propertyData.price,
					url,
					propertyData.agentId,
					propertyData.bedrooms,
					null, // description
					null, // image
					lat,
					lon,
				);
			}
		},

		failedRequestHandler({ request }) {
			console.error(`Request ${request.url} failed.`);
		},
	});

	await crawler.run(["https://livinestateagents.co.uk/property-search/"]);
	console.log("Agent 27 completed.");
}

if (require.main === module) {
	run().catch((err) => {
		console.error("Agent 27 failed:", err);
		process.exit(1);
	});
}

module.exports = { run };
