const { PlaywrightCrawler } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { chromium } = require("playwright-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");

// Use stealth plugin to avoid bot detection
chromium.use(StealthPlugin());

const AGENT_ID = 4; // Marsh & Parsons

const PROPERTY_TYPES = [
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
		preNavigationHooks: [
			async ({ page, request }) => {
				// Block images and other unnecessary resources for ALL pages (Listing and Detail)
				await page.route("**/*", (route) => {
					const resourceType = route.request().resourceType();
					if (["image", "media", "font", "stylesheet"].includes(resourceType)) {
						route.abort();
					} else {
						route.continue();
					}
				});

				// Set realistic headers for every page
				await page.setExtraHTTPHeaders({
					"User-Agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.97 Safari/537.36",
					"Accept-Language": "en-GB,en;q=0.9",
				});
			},
		],
		requestHandler: async ({ page, request, log }) => {
			const { label, isRent } = request.userData;

			if (label === "LISTING") {
				log.info(`Processing listing page: ${request.url}`);

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

				const batchSize = 2; // smaller batch reduces memory usage
				for (let i = 0; i < properties.length; i += batchSize) {
					const batch = properties.slice(i, i + batchSize);

					await Promise.all(
						batch.map(async (property) => {
							const detailPage = await page.context().newPage();
							try {
								// Block images and other unnecessary resources on detail page
								await detailPage.route("**/*", (route) => {
									const resourceType = route.request().resourceType();
									if (["image", "media", "font", "stylesheet"].includes(resourceType)) {
										route.abort();
									} else {
										route.continue();
									}
								});

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
								// Small random delay to avoid blocking
								await new Promise((resolve) =>
									setTimeout(resolve, Math.floor(Math.random() * 1500) + 500)
								);
							}
						})
					);
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
