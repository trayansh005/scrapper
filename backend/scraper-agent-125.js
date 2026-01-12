// Pattinson scraper using Playwright with Crawlee
// Agent ID: 125
// Website: pattinson.co.uk
// Usage:
// node backend/scraper-agent-125.js

const { PlaywrightCrawler, log } = require("crawlee");
const { launchOptions } = require("camoufox-js");
const { firefox } = require("playwright");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 125;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.pattinson.co.uk/buy/property-search",
		totalPages: 100, // 1986 properties / 20 per page = 100 pages (rounded up)
		recordsPerPage: 20,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.pattinson.co.uk/rent/property-search",
		totalPages: 13, // 244 properties / 20 per page = 13 pages (rounded up)
		recordsPerPage: 20,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapePattinson() {
	console.log(`\n🚀 Starting Pattinson scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 600,

		launchContext: {
			launcher: firefox,
			launchOptions: await launchOptions({
				headless: false,
				args: ["--start-maximized"],
			}),
		},

		browserPoolOptions: {
			useFingerprints: false, // Camoufox handles this
		},

		preNavigationHooks: [
			async ({ page }, gotoOptions) => {
				// Use a more common viewport
				await page.setViewportSize({ width: 1366, height: 768 });
				gotoOptions.waitUntil = "domcontentloaded";
				gotoOptions.timeout = 60000;
			},
		],

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			try {
				// --- 🛡️ CLOUDFLARE BYPASS LOGIC 🛡️ ---
				console.log("⏳ Waiting for Cloudflare/Page Load (Max 60s)...");

				// Wait for the title to change from the Cloudflare challenge
				// or for the characteristic listing selector to appear
				await page
					.waitForFunction(
						() => {
							const title = document.title;
							const hasListings = !!document.querySelector("a.row.m-0.bg-white");
							const isBlocked =
								title.includes("Just a moment") ||
								title.includes("Cloudflare") ||
								title.includes("Access Denied");
							return hasListings || !isBlocked;
						},
						{ timeout: 60000, polling: 1000 }
					)
					.catch(() => console.log("⚠️ Cloudflare wait timed out, attempting to proceed..."));

				// Small delay to let page settle after verification
				await page.waitForTimeout(5000);

				// Extract properties from the DOM
				const properties = await page.evaluate(() => {
					try {
						const cards = Array.from(document.querySelectorAll("a.row.m-0.bg-white"));
						return cards
							.map((card) => {
								const href = card.getAttribute("href");
								const link = href ? "https://www.pattinson.co.uk" + href : null;

								const priceEl = card.querySelector("dt.display-5.text-primary");
								const price = priceEl ? priceEl.textContent.trim() : "";

								const titleEl = card.querySelector("div.text-primary-dark.fw-medium");
								const title = titleEl ? titleEl.textContent.trim() : "";

								const specs = Array.from(card.querySelectorAll("div.d-flex.align-items-center"));
								const bedrooms = specs[0]
									? specs[0].querySelector("span.lh-1.fs-14")?.textContent.trim() || null
									: null;

								return { link, price, title, bedrooms };
							})
							.filter((p) => p.link);
					} catch (e) {
						return [];
					}
				});

				console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

				// Process properties
				for (const property of properties) {
					if (!property.link) continue;

					let coords = { latitude: null, longitude: null };

					// Visit detail page to extract coordinates from JSON-LD
					const detailPage = await page.context().newPage();
					try {
						await detailPage.goto(property.link, {
							waitUntil: "domcontentloaded",
							timeout: 30000,
						});

						// Brief Cloudflare check for detail page
						await detailPage
							.waitForFunction(
								() => {
									const title = document.title;
									return !title.includes("Just a moment") && !title.includes("Cloudflare");
								},
								{ timeout: 10000 }
							)
							.catch(() => {});

						await detailPage.waitForTimeout(1000);

						const detailCoords = await detailPage.evaluate(() => {
							try {
								const scripts = Array.from(
									document.querySelectorAll('script[type="application/ld+json"]')
								);
								for (const script of scripts) {
									try {
										const data = JSON.parse(script.textContent);
										if (data && data["@type"] === "GeoCoordinates") {
											return { lat: data.latitude, lng: data.longitude };
										}
										if (data && data.geo && data.geo.latitude) {
											return { lat: data.geo.latitude, lng: data.geo.longitude };
										}
									} catch (e) {}
								}
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
						// ignore detail errors
					} finally {
						await detailPage.close();
					}

					try {
						const priceClean = property.price ? property.price.replace(/[^0-9]/g, "").trim() : null;
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
						console.log(`✅ ${property.title} - ${formatPrice(priceClean)}`);
					} catch (dbErr) {
						console.error(`❌ DB error: ${dbErr.message}`);
					}

					// Delay between properties
					await new Promise((resolve) => setTimeout(resolve, 2000 + Math.random() * 2000));
				}
			} catch (err) {
				console.error(`❌ Request handler error: ${err.message}`);
			}
		},

		async failedRequestHandler({ request }) {
			console.error(`❌ Permanent failure for ${request.url}`);
		},
	});

	// Enqueue all listing pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}?p=${pg}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Pattinson - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapePattinson();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
