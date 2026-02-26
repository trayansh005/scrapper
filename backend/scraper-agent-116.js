// Gascoigne Pees scraper using Playwright with Crawlee
// Agent ID: 116
//
// Usage:
// node backend/scraper-agent-116.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 116;
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
	// {
	// 	urlPath: "properties/sales/status-available/most-recent-first",
	// 	totalRecords: 512,
	// 	recordsPerPage: 10,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlPath: "properties/lettings/status-available/most-recent-first",
		totalRecords: 70,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeGascoignePees() {
	console.log(`\n🚀 Starting Gascoigne Pees scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1, // Process one page at a time
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

			// Processing listing page
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Wait for properties to load
			await page.waitForTimeout(3000);
			await page.waitForSelector(".hf-property-results .card", { timeout: 30000 }).catch(() => {
				console.log(`⚠️ No properties found on page ${pageNum}`);
			});

			// Extract all properties from the page
			const properties = await page.$$eval(".hf-property-results .card", (cards) => {
				const results = [];

				cards.forEach((card) => {
					try {
						// Extract link from anchor tag
						const linkEl = card.querySelector("a");
						let link = linkEl ? linkEl.getAttribute("href") : null;
						if (link && !link.startsWith("http")) {
							link = "https://www.gpees.co.uk" + link;
						}

						// Extract title from .card__text-content
						const titleEl = card.querySelector(".card__text-content");
						const title = titleEl ? titleEl.textContent.trim() : null;

						// Extract bedrooms from .card-content__spec-list-number (first occurrence)
						const bedroomsEl = card.querySelector(".card-content__spec-list-number");
						let bedrooms = null;
						if (bedroomsEl) {
							const bedroomsText = bedroomsEl.textContent.trim();
							const bedroomsMatch = bedroomsText.match(/\d+/);
							if (bedroomsMatch) {
								bedrooms = bedroomsMatch[0];
							}
						}

						// Extract price from .card__heading
						const priceEl = card.querySelector(".card__heading");
						let price = null;
						if (priceEl) {
							const priceText = priceEl.textContent.trim();
							const priceMatch = priceText.match(/£([\d,]+)/);
							if (priceMatch) {
								price = priceMatch[1].replace(/,/g, "");
							}
						}

						if (link && title && price) {
							results.push({
								link: link,
								title: title,
								price,
								bedrooms,
							});
						}
					} catch (err) {
						// Skip this card if error
					}
				});

				return results;
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process each property one by one
			for (let i = 0; i < properties.length; i++) {
				const property = properties[i];

				// Navigate to detail page directly
				try {
					await page.goto(property.link, { waitUntil: "domcontentloaded", timeout: 30000 });
					await page.waitForTimeout(1000);

					let coords = { latitude: null, longitude: null };

					// Extract coordinates from HTML comments
					const htmlContent = await page.content();
					const latMatch = htmlContent.match(/<!--property-latitude:"([0-9.-]+)"-->/);
					const lngMatch = htmlContent.match(/<!--property-longitude:"([0-9.-]+)"-->/);

					if (latMatch && lngMatch) {
						coords.latitude = parseFloat(latMatch[1]);
						coords.longitude = parseFloat(lngMatch[1]);
					}

					try {
						await updatePriceByPropertyURL(
							property.link,
							property.price,
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
						console.log(`✅ ${property.title} - ${formatPrice(property.price)} - ${coordsStr}`);
					} catch (dbErr) {
						console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
					}
				} catch (error) {
					console.error(`❌ Error processing ${property.link}: ${error.message}`);
				}

				// Delay between properties
				await new Promise((resolve) => setTimeout(resolve, 500));
			}

			// Random delay between pages (3-5 seconds)
			const delay = 3000 + Math.random() * 2000;
			console.log(`⏱️ Waiting ${Math.round(delay / 1000)}s before next page...`);
			await new Promise((resolve) => setTimeout(resolve, delay));
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Process property types one by one
	for (const propertyType of PROPERTY_TYPES) {
		const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
		console.log(
			`🏠 Processing ${propertyType.label} properties (${propertyType.totalRecords} total, ${totalPages} pages)\n`
		);

		// Add all pages to the queue
		const requests = [];
		for (let page = 1; page <= totalPages; page++) {
			requests.push({
				url: `https://www.gpees.co.uk/${propertyType.urlPath}/page-${page}#/`,
				userData: {
					pageNum: page,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Gascoigne Pees - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

// Main execution
(async () => {
	try {
		await scrapeGascoignePees();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
