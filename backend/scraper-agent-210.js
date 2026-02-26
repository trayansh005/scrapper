// Jacksons scraper using Playwright with Crawlee
// Agent ID: 210
// Usage:
// node backend/scraper-agent-210.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 210;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

// Helper delay utilities to avoid site rate limits
function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function rand(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomDelay(minMs, maxMs) {
	return sleep(rand(minMs, maxMs));
}

// Configuration for Jacksons sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	// Sales
	// 	urlBase: "https://www.jacksonsestateagents.com/properties/sales/status-available",
	// 	totalPages: 33,
	// 	recordsPerPage: 12,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		// Lettings
		urlBase: "https://www.jacksonsestateagents.com/properties/lettings/status-available",
		totalPages: 8,
		recordsPerPage: 12,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeJacksons() {
	console.log(`\n🚀 Starting Jacksons scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(1200);
			// Prefer waiting for the actual property columns which are the real listing items
			await page
				.waitForSelector(".col-xs-12.col-sm-6.col-md-4.col-lg-3", { timeout: 20000 })
				.catch(() => {
					console.log(`⚠️ No property columns found on page ${pageNum}`);
				});

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(".col-xs-12.col-sm-6.col-md-4.col-lg-3")
					);
					if (!items || items.length === 0) return [];
					return items
						.map((el) => {
							const linkEl = el.querySelector(
								"article.property-card a.no-decoration, article.property-card a"
							);
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.jacksonsestateagents.com" + href
								: null;

							const title = el.querySelector("h1")?.textContent?.trim() || "";
							const location = el.querySelector("h2")?.textContent?.trim() || "";

							// price - prefer data.money value or text
							const money = el.querySelector("data.money");
							const price = money
								? money.getAttribute("value") || money.textContent.trim()
								: el.querySelector(".price")?.textContent?.trim() || "";

							const bedrooms =
								el.querySelector(".bed-bath-icons__number")?.textContent?.trim() || null;

							return {
								link,
								price,
								title: title + (location ? ", " + location : ""),
								bedrooms,
								lat: null,
								lng: null,
							};
						})
						.filter((p) => p.link);
				} catch (e) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				// Process sequentially to avoid rate-limits (site blocks aggressive parallel requests)
				for (const property of batch) {
					if (!property.link) continue;

					// Gentle random delay before visiting detail page
					await randomDelay(800, 2000);

					let coords = { latitude: property.lat || null, longitude: property.lng || null };

					if (!coords.latitude || !coords.longitude) {
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							// small pause for dynamic scripts to populate map container
							await randomDelay(500, 1200);

							const detailCoords = await detailPage.evaluate(() => {
								try {
									const map = document.querySelector("#propertyShowMap");
									if (map) {
										const lat = map.getAttribute("data-lat");
										const lng = map.getAttribute("data-lng");
										if (lat && lng) return { lat: parseFloat(lat), lng: parseFloat(lng) };
									}

									const scripts = Array.from(
										document.querySelectorAll('script[type="application/ld+json"]')
									);
									for (const s of scripts) {
										try {
											const data = JSON.parse(s.textContent);
											if (data && data.geo && data.geo.latitude && data.geo.longitude) {
												return { lat: data.geo.latitude, lng: data.geo.longitude };
											}
										} catch (e) {
											// continue
										}
									}

									const allScripts = Array.from(document.querySelectorAll("script"))
										.map((s) => s.textContent)
										.join("\n");
									const latMatch =
										allScripts.match(/data-lat\s*=\s*"?([0-9.+-]+)"?/i) ||
										allScripts.match(/"latitude"\s*:\s*"?([0-9.+-]+)"?/i);
									const lngMatch =
										allScripts.match(/data-lng\s*=\s*"?([0-9.+-]+)"?/i) ||
										allScripts.match(/"longitude"\s*:\s*"?([0-9.+-]+)"?/i);
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
							.replace(/[£,\spcm\(\)pw]/gi, "")
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

					// Short randomized pause between property processing
					await randomDelay(300, 900);
				}

				// Pause between batches
				await randomDelay(1000, 2500);
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			let url = propertyType.urlBase;
			if (pg > 1) url = `${propertyType.urlBase}/page-${pg}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();

		// Pause between property type runs to reduce chances of being rate-limited
		await randomDelay(2000, 6000);
	}

	console.log(
		`\n✅ Completed Jacksons - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeJacksons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
