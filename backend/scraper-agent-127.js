// BridgFords scraper using Playwright with Crawlee
// Agent ID: 127
//
// Usage:
// node backend/scraper-agent-127.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 127;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

// Configuration for both lettings and sales
const PROPERTY_TYPES = [
	// {
	// 	urlPath: "properties/sales/status-available/most-recent-first",
	// 	totalRecords: 1998,
	// 	recordsPerPage: 10,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		urlPath: "properties/lettings/status-available/most-recent-first",
		totalRecords: 409,
		recordsPerPage: 10,
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapeBridgFords() {
	console.log(`\n🚀 Starting BridgFords scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1, // Process one page at a time
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
				args: [
					"--no-sandbox",
					"--disable-setuid-sandbox",
					"--disable-blink-features=AutomationControlled",
				],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			// Processing listing page
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// No user agent setting (like agent 71)

			// Wait for properties to load
			await page.waitForTimeout(2000);
			await page.waitForSelector(".hf-property-results .card", { timeout: 30000 }).catch(() => {
				console.log(`⚠️ No properties found on page ${pageNum}`);
			});

			// Check for blocking/error messages (commented out for debugging)
			// const content = await page.content();
			// if (content.includes('Access Denied') || content.includes('blocked') || content.includes('captcha')) {
			//     console.error(`⚠️ Page ${pageNum} appears to be blocked or requires captcha`);
			//     return;
			// }

			// Extract all properties from the page
			const properties = await page.$$eval(".hf-property-results .card", (cards) => {
				const results = [];

				cards.forEach((card) => {
					try {
						// Extract link from anchor tag
						const linkEl = card.querySelector("a");
						let link = linkEl ? linkEl.getAttribute("href") : null;
						if (link && !link.startsWith("http")) {
							link = "https://www.bridgfords.co.uk" + link;
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

			// If no properties found, stop pagination
			if (properties.length === 0) {
				console.log(`⚠️ No properties found on page ${pageNum}, stopping pagination`);
				return;
			}

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
				url: `https://www.bridgfords.co.uk/${propertyType.urlPath}/page-${page}#/`,
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
		`\n✅ Completed BridgFords - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

// Main execution
(async () => {
	try {
		await scrapeBridgFords();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
