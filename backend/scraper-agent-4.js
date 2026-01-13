const { CheerioCrawler, PlaywrightCrawler } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { chromium } = require("playwright-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");

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

// Memory monitoring
function logMemoryUsage(label) {
	const used = process.memoryUsage();
	console.log(
		`[${label}] Memory: ${Math.round(used.heapUsed / 1024 / 1024)}MB / ${Math.round(
			used.heapTotal / 1024 / 1024
		)}MB`
	);
}

// Shared detail page processor
async function processDetailPages(properties, playwrightCrawler, log) {
	const detailUrls = properties.map((property) => ({
		url: property.url,
		userData: {
			label: "DETAIL",
			...property,
		},
	}));

	await playwrightCrawler.addRequests(detailUrls);
	await playwrightCrawler.run();
}

async function scrapeMarshParsons() {
	console.log(`Starting Marsh & Parsons Scraper (Agent ${AGENT_ID})...`);
	logMemoryUsage("START");

	// ============================================
	// PLAYWRIGHT CRAWLER - Persistent (reused for all detail pages)
	// ============================================
	const playwrightCrawler = new PlaywrightCrawler({
		launchContext: {
			launcher: chromium,
			launchOptions: {
				headless: true,
				args: [
					"--disable-dev-shm-usage",
					"--disable-accelerated-2d-canvas",
					"--no-first-run",
					"--no-zygote",
					"--disable-gpu",
					"--disable-software-rasterizer",
					"--disable-extensions",
					"--disable-background-networking",
					"--disable-background-timer-throttling",
					"--disable-backgrounding-occluded-windows",
					"--disable-breakpad",
					"--disable-component-extensions-with-background-pages",
					"--disable-features=TranslateUI,BlinkGenPropertyTrees",
					"--disable-ipc-flooding-protection",
					"--disable-renderer-backgrounding",
					"--enable-features=NetworkService,NetworkServiceInProcess",
					"--force-color-profile=srgb",
					"--hide-scrollbars",
					"--metrics-recording-only",
					"--mute-audio",
				],
			},
		},
		maxConcurrency: 2, // Increased from 1 for faster processing
		minConcurrency: 1,
		requestHandlerTimeoutSecs: 45,

		preNavigationHooks: [
			async ({ page }) => {
				await page.route("**/*", (route) => {
					const resourceType = route.request().resourceType();
					const url = route.request().url();

					if (["image", "media", "font", "stylesheet"].includes(resourceType)) {
						return route.abort();
					}

					if (
						url.includes("google-analytics") ||
						url.includes("googletagmanager") ||
						url.includes("facebook") ||
						url.includes("analytics") ||
						url.includes("doubleclick") ||
						url.includes("tracking")
					) {
						return route.abort();
					}

					route.continue();
				});

				await page.setExtraHTTPHeaders({
					"User-Agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
					"Accept-Language": "en-GB,en;q=0.9",
					Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
				});
			},
		],

		async requestHandler({ page, request, log }) {
			const { title, location, priceRaw, bedrooms, isRent } = request.userData;

			log.info(`Processing detail: ${request.url}`);

			let priceClean = priceRaw.replace(/[£,]/g, "");
			if (isRent && priceClean.includes("p/w")) {
				priceClean = priceClean.replace("p/w", "").trim();
			}
			const price = parseFloat(priceClean);

			try {
				await page.goto(request.url, {
					waitUntil: "domcontentloaded",
					timeout: 45000,
				});

				const html = await page.content();

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

				const fullTitle = `${title}, ${location}`;

				await updatePriceByPropertyURL(
					request.url,
					price,
					fullTitle,
					bedrooms,
					AGENT_ID,
					isRent,
					latitude,
					longitude
				);

				log.info(`✓ ${fullTitle} (£${price})`);
			} catch (error) {
				log.error(`✗ Failed ${request.url}: ${error.message}`);
			}

			// Reduced delay for faster processing
			await new Promise((resolve) => setTimeout(resolve, Math.floor(Math.random() * 500) + 200));
		},

		failedRequestHandler: async ({ request, log }) => {
			log.error(`Detail page failed: ${request.url}`);
		},
	});

	// ============================================
	// PROCESS PAGE BY PAGE
	// ============================================
	for (const type of PROPERTY_TYPES) {
		console.log(`\n📦 Processing ${type.name}...`);

		for (let pageNum = 1; pageNum <= type.totalPages; pageNum++) {
			const listingUrl = `${type.baseUrl}&page=${pageNum}`;
			console.log(`\n📋 Page ${pageNum}/${type.totalPages}: ${listingUrl}`);

			try {
				// Step 1: Fetch listing page with CheerioCrawler
				const cheerioCrawler = new CheerioCrawler({
					maxConcurrency: 1,
					requestHandlerTimeoutSecs: 30,

					async requestHandler({ $, request, log }) {
						log.info(`Scraping listing: ${request.url}`);

						const properties = [];
						$("div.my-4.shadow-md.rounded-xl").each((_, card) => {
							const $card = $(card);
							const linkElement = $card.find('a[href*="/property/"]');
							const titleElement = $card.find("h3");
							const locationElement = $card.find("p");

							const textContent = $card.text();
							const priceMatch = textContent.match(/£[0-9,]+(p\/w)?/);
							const priceRaw = priceMatch ? priceMatch[0] : null;

							const bedImg = $card.find('img[alt="bed"]');
							let bedrooms = null;
							if (bedImg.length > 0) {
								const bedroomText = bedImg.parent().text().trim();
								bedrooms = parseInt(bedroomText) || null;
							}

							const url = linkElement.attr("href");
							if (url && priceRaw) {
								properties.push({
									url: url.startsWith("http") ? url : `https://www.marshandparsons.co.uk${url}`,
									title: titleElement.text().trim(),
									location: locationElement.text().trim(),
									priceRaw,
									bedrooms,
									isRent: type.isRent,
								});
							}
						});

						log.info(`Found ${properties.length} properties`);

						// Step 2: Immediately process detail pages
						if (properties.length > 0) {
							console.log(`  🎭 Processing ${properties.length} detail pages...`);
							await processDetailPages(properties, playwrightCrawler, log);
						}
					},

					failedRequestHandler: async ({ request, log }) => {
						log.error(`Listing page failed: ${request.url}`);
					},
				});

				// Run Cheerio crawler for this single listing page
				await cheerioCrawler.run([
					{
						url: listingUrl,
						userData: {
							label: "LISTING",
							isRent: type.isRent,
						},
					},
				]);

				logMemoryUsage(`After page ${pageNum}`);

				// Small delay between listing pages
				await new Promise((resolve) => setTimeout(resolve, 500));
			} catch (error) {
				console.error(`Error on page ${pageNum}: ${error.message}`);
			}
		}
	}

	console.log("\n✅ Scraping completed.");
	await updateRemoveStatus(AGENT_ID);
	logMemoryUsage("END");
}

// Run scraper
scrapeMarshParsons()
	.then(() => {
		console.log("✅ All done!");
		process.exit(0);
	})
	.catch((err) => {
		console.error("❌ Scraper error:", err);
		process.exit(1);
	});
