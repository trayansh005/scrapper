// Snellers scraper using Playwright with Crawlee
// Agent ID: 19
// Usage:
// node backend/scraper-agent-19.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 19;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.snellers.co.uk/properties/sales/status-available",
	// 	totalPages: 14,
	// 	recordsPerPage: 12,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlBase: "https://www.snellers.co.uk/properties/lettings/status-available",
		totalPages: 20, // Adjust based on the number of pages
		recordsPerPage: 12,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeSnellers() {
	console.log(`\n🚀 Starting Snellers scraper (Agent ${AGENT_ID})...\n`);

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
			await page.waitForSelector(".property-card", { timeout: 20000 }).catch(() => {
				console.log(`⚠️ No listing container found on page ${pageNum}`);
			});

			// Extract properties from the DOM
			const properties = await page.evaluate(() => {
				const cards = Array.from(document.querySelectorAll(".property-card"));
				return cards.map((card) => {
					const linkEl = card.querySelector("a.no-decoration");
					const link = linkEl ? linkEl.href : null;
					const title = linkEl ? linkEl.title : "";
					const priceText = card.querySelector(".price .money")?.textContent.trim() || "";
					const priceMatch = priceText.match(/£([\d,]+)/); // Extract the first numeric value after '£'
					const price = priceMatch ? priceMatch[1].replace(/,/g, "") : ""; // Remove commas
					const bedrooms =
						card.querySelector(".bed-baths li:nth-child(1)")?.textContent.trim() || "";
					const descriptionEl = card.querySelector(".property-card-description");
					const description = descriptionEl ? descriptionEl.textContent.trim() : ""; // Ensure description is defined

					return { link, title, price, bedrooms, description };
				});
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Replace the database insertion logic with `updatePriceByPropertyURL`
			for (const property of properties) {
				if (!property.link) continue;

				let coords = { latitude: null, longitude: null };

				// Visit detail page to extract coordinates
				const detailPage = await page.context().newPage();
				try {
					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});
					await detailPage.waitForTimeout(500);

					coords = await detailPage.evaluate(() => {
						const mapEl = document.querySelector("#propertyShowStreetview.map");
						if (mapEl) {
							const lat = mapEl.getAttribute("data-lat");
							const lng = mapEl.getAttribute("data-lng");
							return { latitude: parseFloat(lat), longitude: parseFloat(lng) };
						}
						return { latitude: null, longitude: null };
					});
				} catch (err) {
					console.error(`❌ Failed to extract details for ${property.link}: ${err.message}`);
				} finally {
					await detailPage.close();
				}

				// Save property to the database using `updatePriceByPropertyURL`
				try {
					const priceClean = (property.price || "").replace(/[£,\s]/g, "").trim();
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

				// Add a delay to handle rate limiting
				await new Promise((resolve) => setTimeout(resolve, 1000)); // 1-second delay
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue all listing pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
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
		`\n✅ Completed Snellers - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeSnellers();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
