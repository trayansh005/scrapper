// Connells scraper using Playwright with Crawlee
// Agent ID: 46
// Usage:
// node backend/scraper-agent-46.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 46;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(num) {
	if (!num || isNaN(num)) return "£0";
	return "£" + Number(num).toLocaleString("en-GB");
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.connells.co.uk/properties/sales",
		totalPages: 409,
		recordsPerPage: 18,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.connells.co.uk/properties/lettings",
		totalPages: 45,
		recordsPerPage: 18,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeConnells() {
	console.log(`\n🚀 Starting Connells scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for page content to populate
			await page.waitForTimeout(1500);
			await page.waitForSelector(".property", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No property container found on page ${pageNum}`);
			});

			// Extract properties from the DOM
			const properties = await page.evaluate(() => {
				const cards = Array.from(document.querySelectorAll("div.property"));
				return cards.map((card) => {
					const linkEl = card.querySelector("a[href*='/properties/']");
					const link = linkEl ? linkEl.href : null;

					// Extract title and bedrooms from summary
					const summary = card.querySelector(".property__summary")?.textContent?.trim() || "";
					const address = card.querySelector(".property__address")?.textContent?.trim() || "";

					// Extract price
					const priceText = card.querySelector(".property__price")?.textContent?.trim() || "";

					// Extract bedrooms from summary (e.g., "2 Bedroom Home")
					const bedroomsMatch = summary.match(/(\d+)\s+Bedroom/);
					const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[1]) : null;

					return { link, title: address, summary, price: priceText, bedrooms };
				});
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process properties in small batches
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						// Ensure absolute URL
						if (!property.link) return;

						let coords = { latitude: null, longitude: null };

						// Visit detail page to extract coordinates from HTML comments
						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(500);

							// Extract coordinates from HTML comments
							coords = await detailPage.evaluate(() => {
								try {
									const content = document.documentElement.outerHTML;

									// Extract latitude from comment: <!--property-latitude:"51.2791"-->
									const latMatch = content.match(/<!--property-latitude:"([0-9.-]+)"-->/);
									const lngMatch = content.match(/<!--property-longitude:"([0-9.-]+)"-->/);

									return {
										latitude: latMatch ? parseFloat(latMatch[1]) : null,
										longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
									};
								} catch (e) {
									return { latitude: null, longitude: null };
								}
							});
						} catch (err) {
							console.error(`⚠️ Failed to extract details for ${property.link}: ${err.message}`);
						} finally {
							await detailPage.close();
						}

						// Format price: keep only numeric and commas
						const priceClean = (property.price || "").replace(/[^0-9.]/g, "").trim();

						try {
							await updatePriceByPropertyURL(
								property.link.trim(),
								priceClean,
								property.title || "",
								property.bedrooms || null,
								AGENT_ID,
								isRental,
								coords.latitude,
								coords.longitude
							);

							console.log(`✅ ${property.title} - ${formatPrice(priceClean)} - ${property.link}`);

							totalSaved++;
							totalScraped++;
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
						}
					})
				);

				// Small delay between batches
				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue all listing pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 58; pg <= propertyType.totalPages; pg++) {
			// Construct page URL: page-2, page-3, etc. (page 1 is base URL)
			const url = pg === 1 ? `${propertyType.urlBase}` : `${propertyType.urlBase}/page-${pg}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Connells - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeConnells();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
