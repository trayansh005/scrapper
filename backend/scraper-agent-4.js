const { PlaywrightCrawler, Configuration } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

const AGENT_ID = 4; // Marsh & Parsons

const PROPERTY_TYPES = [
	// {
	//  name: "Lettings",
	//  baseUrl:
	//      "https://www.marshandparsons.co.uk/properties-to-rent/london/?filters=exclude_sold%2Cexclude_under_offer",
	//  isRent: true,
	//  totalPages: 18,
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

	// 1. Optimize Memory Config
	const config = Configuration.getGlobalConfig();
	config.set("availableMemoryRatio", 0.75);

	const crawler = new PlaywrightCrawler({
		launchContext: {
			launchOptions: {
				headless: true,
			},
		},
		// 2. Safe to increase concurrency slightly now that detail pages are lightweight
		maxConcurrency: 2,
		requestHandlerTimeoutSecs: 300,

		// 3. Block heavy resources (Images, CSS, Fonts) on the Listing Page
		preNavigationHooks: [
			async ({ page }, gotoOptions) => {
				await page.route("**/*", (route) => {
					const type = route.request().resourceType();
					if (["image", "media", "font", "stylesheet"].includes(type)) {
						return route.abort();
					}
					return route.continue();
				});
			},
		],

		requestHandler: async ({ page, request, log }) => {
			const { label, isRent } = request.userData;

			if (label === "LISTING") {
				log.info(`Processing listing page: ${request.url}`);

				// Extract property basic info from the listing page
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

				// 4. Process properties using fast HTTP requests (No new tabs)
				const batchSize = 10; // Can handle larger batches now
				for (let i = 0; i < properties.length; i += batchSize) {
					const batch = properties.slice(i, i + batchSize);

					await Promise.all(
						batch.map(async (property) => {
							try {
								// Use page.request (API) instead of page.goto (Browser)
								// This is ~10x faster and uses negligible memory
								const response = await page.request.get(property.url);

								if (!response.ok()) {
									throw new Error(`HTTP ${response.status()}`);
								}

								const html = await response.text();

								let priceClean = property.priceRaw.replace(/[£,]/g, "");
								if (property.isRent && priceClean.includes("p/w")) {
									priceClean = priceClean.replace("p/w", "").trim();
								}
								const price = parseFloat(priceClean);

								let latitude = null;
								let longitude = null;

								// Regex patterns
								const mapsMatch = html.match(/ll=([\d.-]+),([\d.-]+)/);
								const scriptMatch = html.match(/lat:\s*([\d.-]+),\s*lng:\s*([\d.-]+)/);
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
									log.info(`Updated: ${fullTitle} (£${price})`);
								} else {
									log.warning(`Coordinates not found for ${property.url}`);
								}
							} catch (error) {
								log.error(`Failed for ${property.url}: ${error.message}`);
							}
						})
					);

					// Tiny delay to be polite to the server
					await new Promise((resolve) => setTimeout(resolve, 200));
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
