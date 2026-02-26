// Bourne Estate Agents scraper using Playwright with Crawlee
// Agent ID: 211
// Usage:
// node backend/scraper-agent-211.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 211;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

// Configuration for Bourne Estate Agents
// 12 properties per page; sales 406 -> 34 pages, rent 117 -> 10 pages
const PROPERTY_TYPES = [
	// {
	// 	urlBase:
	// 		"https://bourneestateagents.com/search/?address_keyword&property_type&minimum_price&maximum_price&minimum_rent&maximum_rent&availability=2&minimum_bedrooms&department=residential-sales",
	// 	totalPages: 34,
	// 	recordsPerPage: 12,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlBase:
			"https://bourneestateagents.com/search/?address_keyword=&property_type=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&availability=6&minimum_bedrooms=&department=residential-lettings",
		totalPages: 10,
		recordsPerPage: 12,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeBourne() {
	console.log(`\n🚀 Starting Bourne Estate Agents scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Allow extra time for client-side rendering
			await page.waitForTimeout(1800);

			// Wait for listing container
			await page
				.waitForSelector(".archive-grid", { timeout: 20000 })
				.catch(() => console.log(`⚠️ No archive-grid found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const container = document.querySelector(".archive-grid");
					if (!container) return [];

					// Find anchors within the container that look like property links
					const anchors = Array.from(container.querySelectorAll("a[href]"));
					const seen = new Set();
					const results = [];

					for (const a of anchors) {
						const href = a.getAttribute("href");
						if (!href) continue;
						// property detail URLs typically contain '/property/' or '/property-for-sale/' or '/property-to-rent/'
						if (!/\/property\b|property-for-sale|property-to-rent|property-for-rent/i.test(href))
							continue;

						// normalize absolute/relative
						const link = href.startsWith("http") ? href : "https://bourneestateagents.com" + href;
						if (seen.has(link)) continue;
						seen.add(link);

						// try to find surrounding card element to get price/title/bedrooms
						const card =
							a.closest(".properties-block") ||
							a.closest(".grid-box") ||
							a.closest(".grid-box-card") ||
							a.closest("article") ||
							a.parentElement;

						const title =
							(card &&
								(card.querySelector(".property-archive-title h4")?.textContent ||
									card.querySelector("h4")?.textContent)) ||
							a.getAttribute("title") ||
							a.querySelector("img")?.alt ||
							"";
						const description =
							card && (card.querySelector(".property-single-description")?.textContent || "");
						const price =
							(card && card.querySelector(".property-archive-price")?.textContent) || "";

						let bedrooms = null;
						const bt = card && card.querySelector(".property-types li span");
						if (bt) bedrooms = bt.textContent.trim();

						results.push({
							link,
							price: price ? price.trim() : "",
							title: (title || description || "").trim(),
							bedrooms,
							lat: null,
							lng: null,
						});
					}

					return results;
				} catch (e) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						let coords = { latitude: property.lat || null, longitude: property.lng || null };

						// If no coords, visit detail page and try to extract from script.yoast-schema-graph or JSON-LD
						if (!coords.latitude || !coords.longitude) {
							const detailPage = await page.context().newPage();
							try {
								await detailPage.goto(property.link, {
									waitUntil: "domcontentloaded",
									timeout: 30000,
								});
								await detailPage.waitForTimeout(500);

								const detailCoords = await detailPage.evaluate(() => {
									try {
										// Yoast schema JSON-LD often lives in a script with class 'yoast-schema-graph'
										const yoast = document.querySelector("script.yoast-schema-graph");
										if (yoast) {
											try {
												const raw = JSON.parse(yoast.textContent || yoast.innerText);
												// raw can be an object with @graph array
												const graph = raw["@graph"] || (Array.isArray(raw) ? raw : null);
												const items = graph || (Array.isArray(raw) ? raw : [raw]);
												for (const node of items) {
													if (node && node.latitude && node.longitude) {
														return {
															lat: parseFloat(node.latitude),
															lng: parseFloat(node.longitude),
														};
													}
													if (
														node &&
														node.geo &&
														(node.geo.latitude || node.geo.lat || node.geo.latitude === 0)
													) {
														const lat =
															node.geo.latitude ||
															node.geo.lat ||
															(node.geo.latitude === 0 ? 0 : null);
														const lng = node.geo.longitude || node.geo.long || node.geo.lng || null;
														if (lat && lng) return { lat: parseFloat(lat), lng: parseFloat(lng) };
													}
												}
											} catch (e) {
												// fallthrough
											}
										}

										// Fallback: check application/ld+json scripts
										const scripts = Array.from(
											document.querySelectorAll('script[type="application/ld+json"]')
										);
										for (const s of scripts) {
											try {
												const data = JSON.parse(s.textContent);
												if (data && data.geo && (data.geo.latitude || data.geo.latitude === 0)) {
													return { lat: data.geo.latitude, lng: data.geo.longitude };
												}
												// handle nested @graph
												const graph = data["@graph"] || (Array.isArray(data) ? data : null);
												if (graph) {
													for (const node of graph) {
														if (
															node &&
															node.geo &&
															(node.geo.latitude || node.geo.latitude === 0)
														) {
															return { lat: node.geo.latitude, lng: node.geo.longitude };
														}
													}
												}
											} catch (e) {
												// continue
											}
										}

										// Last resort: regex search for latitude/longitude pairs in inline scripts
										const allScripts = Array.from(document.querySelectorAll("script"))
											.map((s) => s.textContent)
											.join("\n");
										const latMatch =
											allScripts.match(/"latitude"\s*:\s*([0-9.+-]+)/i) ||
											allScripts.match(/"lat"\s*:\s*([0-9.+-]+)/i);
										const lngMatch =
											allScripts.match(/"longitude"\s*:\s*([0-9.+-]+)/i) ||
											allScripts.match(/"lng"\s*:\s*([0-9.+-]+)/i);
										if (latMatch && lngMatch)
											return { lat: parseFloat(latMatch[1]), lng: parseFloat(lngMatch[1]) };

										return null;
									} catch (e) {
										return null;
									}
								});

								if (detailCoords) {
									coords.latitude = detailCoords.lat;
									coords.longitude = detailCoords.lng;
								}
							} catch (err) {
								// ignore detail page errors
							} finally {
								await detailPage.close();
							}
						}

						try {
							const priceClean = (property.price || "")
								.toString()
								.replace(/[£,\s]/g, "")
								.trim();

							await updatePriceByPropertyURL(
								property.link,
								priceClean,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								coords.latitude,
								coords.longitude
							);

							totalSaved++;
							totalScraped++;

							const coordsStr =
								coords.latitude && coords.longitude
									? `${coords.latitude}, ${coords.longitude}`
									: "No coords";
							console.log(`✅ ${property.title} - ${formatPrice(priceClean)} - ${coordsStr}`);
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
						}
					})
				);

				// Small delay between batches
				await new Promise((resolve) => setTimeout(resolve, 300));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			// Page 1 uses base; subsequent pages use /search/page/{pg}/?query
			let url = propertyType.urlBase;
			if (pg > 1) {
				// replace /search/ with /search/page/{pg}/
				url = propertyType.urlBase.replace("/search/", `/search/page/${pg}/`);
			}
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(`\n✅ Completed Bourne - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

(async () => {
	try {
		await scrapeBourne();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
