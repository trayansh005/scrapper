// Homesea scraper using Playwright with Crawlee
// Agent ID: 217
// Usage:
// node backend/scraper-agent-217.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 217;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Homesea: 167 properties, 9 per page => 19 pages
const PROPERTY_TYPES = [
	{
		urlBase: "https://homesea.co.uk/property-search/page", // page number will be appended like /{page}/?department=residential-sales&...
		totalRecords: 167,
		totalPages: 19,
		recordsPerPage: 9,
		isRental: false,
		label: "SALES",
	},
];

async function scrapeHomesea() {
	console.log(`\n🚀 Starting Homesea scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(2000);

			// Wait for property list items
			await page.waitForSelector("li.type-property, li.post-", { timeout: 15000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll("li.type-property"));

					return items
						.map((li) => {
							try {
								const anchor = li.querySelector("a[href]");
								const link = anchor ? anchor.href : null;

								const title = li.querySelector("h2 a")?.textContent?.trim() || "";

								const price = li.querySelector(".price")?.textContent?.trim() || "";

								// bedrooms: look for 'Bedrooms:' text in rooms list
								let bedrooms = null;
								const rooms = li.querySelectorAll("ul.rooms li");
								for (const r of rooms) {
									const txt = r.textContent || "";
									if (txt.toLowerCase().includes("bedrooms")) {
										const m = txt.match(/(\d+)/);
										if (m) bedrooms = m[1];
										break;
									}
								}

								if (link) return { link, title, price, bedrooms };
								return null;
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			for (const property of properties) {
				if (!property.link) continue;

				let coords = { latitude: null, longitude: null };

				// open detail page to extract JSON-LD
				const detailPage = await page.context().newPage();
				try {
					await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 30000 });
					await detailPage.waitForTimeout(500);

					// Find any inline script that initializes the google maps with LatLng
					const scriptText = await detailPage.evaluate(() => {
						const scripts = Array.from(document.querySelectorAll("script"));
						for (const s of scripts) {
							const t = s.textContent || "";
							if (t.includes("google.maps.LatLng") || t.includes("initialize_property_map"))
								return t;
						}
						return null;
					});

					if (scriptText) {
						const m = scriptText.match(
							/google\.maps\.LatLng\(\s*([\-0-9.]+)\s*,\s*([\-0-9.]+)\s*\)/
						);
						if (m) {
							coords = { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };
						}
					}
				} catch (err) {
					// ignore detail errors
				} finally {
					await detailPage.close();
				}

				try {
					const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;

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

					console.log(
						`✅ ${property.title} - ${formatPrice(priceClean)} - ${
							coords.latitude && coords.longitude
								? `${coords.latitude}, ${coords.longitude}`
								: "No coords"
						}`
					);
				} catch (dbErr) {
					console.error(`❌ DB error for ${property.link}: ${dbErr?.message || dbErr}`);
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(
			`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages, ${propertyType.recordsPerPage} per page)`
		);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}/${pg}/?department=residential-sales&address_keyword&radius&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&availability=2`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Homesea - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeHomesea();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
