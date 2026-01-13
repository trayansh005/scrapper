const { PlaywrightCrawler, Dataset } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

const AGENT_ID = 4; // Marsh & Parsons

const PROPERTY_TYPES = [
	// {
	// 	name: "Lettings",
	// 	baseUrl:
	// 		"https://www.marshandparsons.co.uk/properties-to-rent/london/?filters=exclude_sold%2Cexclude_under_offer",
	// 	isRent: true,
	// 	totalPages: 18,
	// },
	{
		name: "Sales",
		baseUrl:
			"https://www.marshandparsons.co.uk/properties-for-sale/london/?filters=exclude_sold%2Cexclude_under_offer",
		isRent: false,
		totalPages: 30,
	},
];

async function scrapeMarshParsons() {
	console.log(`Starting Marsh & Parsons Scraper (Agent ${AGENT_ID})...`);

	const crawler = new PlaywrightCrawler({
		launchContext: {
			launchOptions: {
				headless: true,
			},
		},
		maxConcurrency: 1,
		requestHandlerTimeoutSecs: 300,
		requestHandler: async ({ page, request, log }) => {
			const { label, isRent } = request.userData;

			if (label === "LISTING") {
				log.info(`Processing listing page: ${request.url}`);

				// Extract property details from the listing page
				const properties = await page.evaluate((isRent) => {
					const propertyCards = document.querySelectorAll("div.my-4.shadow-md.rounded-xl");
					const results = [];

					propertyCards.forEach((card) => {
						const linkElement = card.querySelector('a[href*="/property/"]');
						const titleElement = card.querySelector("h3");
						const locationElement = card.querySelector("p");

						const textContent = card.innerText;
						const priceMatch = textContent.match(/£[0-9,]+(p\/w)?/);
						const priceRaw = priceMatch ? priceMatch[0] : null;

						const bedImg = card.querySelector('img[alt="bed"]');
						let bedrooms = null;
						if (bedImg && bedImg.parentElement) {
							bedrooms = parseInt(bedImg.parentElement.innerText.trim()) || null;
						}

						if (linkElement && priceRaw) {
							results.push({
								url: linkElement.href,
								title: titleElement ? titleElement.innerText.trim() : "",
								location: locationElement ? locationElement.innerText.trim() : "",
								priceRaw,
								bedrooms,
								isRent,
							});
						}
					});

					return results;
				}, isRent);

				log.info(`Found ${properties.length} properties on page.`);

				// Process properties in batches like Agent 50
				const batchSize = 5;
				for (let i = 0; i < properties.length; i += batchSize) {
					const batch = properties.slice(i, i + batchSize);

					await Promise.all(
						batch.map(async (property) => {
							const detailPage = await page.context().newPage();
							try {
								log.info(`Processing detail page: ${property.url}`);

								let priceClean = property.priceRaw.replace(/[£,]/g, "");
								if (property.isRent && priceClean.includes("p/w")) {
									priceClean = priceClean.replace("p/w", "").trim();
								}
								const price = parseFloat(priceClean);

								await detailPage.goto(property.url, {
									waitUntil: "networkidle",
									timeout: 60000,
								});

								const html = await detailPage.content();

								let latitude = null;
								let longitude = null;

								// Try multiple patterns for coordinates
								// Pattern 1: maps.google.com?ll=51.48265,-0.01465
								const mapsMatch = html.match(/ll=([\d.-]+),([\d.-]+)/);
								// Pattern 2: {lat: 51.50564, lng: -0.19597}
								const scriptMatch = html.match(/lat:\s*([\d.-]+),\s*lng:\s*([\d.-]+)/);
								// Pattern 3: "latitude":51.48265,"longitude":-0.01465
								const jsonMatch = html.match(/"latitude":\s*([\d.-]+),\s*"longitude":\s*([\d.-]+)/);

								if (mapsMatch) {
									latitude = parseFloat(mapsMatch[1]);
									longitude = parseFloat(mapsMatch[2]);
								} else if (scriptMatch) {
									latitude = parseFloat(scriptMatch[1]);
									longitude = parseFloat(scriptMatch[2]);
								} else if (jsonMatch) {
									latitude = parseFloat(jsonMatch[1]);
									longitude = parseFloat(jsonMatch[2]);
								}

								if (latitude !== null && longitude !== null) {
								} else {
									log.warning(`Coordinates not found for ${property.url}`);
								}

								const fullTitle = `${property.title}, ${property.location}`;

								await updatePriceByPropertyURL(
									property.url,
									price,
									fullTitle,
									property.bedrooms,
									AGENT_ID,
									isRent,
									latitude,
									longitude
								);
								log.info(`Updated DB for: ${fullTitle} (${price})`);
							} catch (error) {
								log.error(`Failed for ${property.url}: ${error.message}`);
							} finally {
								await detailPage.close();
							}
						})
					);

					// Small delay between batches
					await new Promise((resolve) => setTimeout(resolve, 500));
				}
			}
		},
	});

	const startUrls = [];
	for (const type of PROPERTY_TYPES) {
		for (let i = 1; i <= type.totalPages; i++) {
			startUrls.push({
				url: `${type.baseUrl}&page=${i}`,
				userData: {
					label: "LISTING",
					isRent: type.isRent,
				},
			});
		}
	}

	await crawler.run(startUrls);
	console.log("Scraping completed.");
	await updateRemoveStatus(AGENT_ID);
}

scrapeMarshParsons()
	.then(() => {
		console.log("✅ All done!");
		process.exit(0);
	})
	.catch((err) => {
		console.error("❌ Scraper encountered an error:", err);
		process.exit(1);
	});
