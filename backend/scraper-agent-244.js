const { PlaywrightCrawler } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db");

const AGENT_ID = 244;
const BASE_URL = "https://www.mypropertybox.co.uk";

const PROPERTY_TYPES = [
	{
		type: "sales",
		urlBase:
			"https://www.mypropertybox.co.uk/results-gallery.php?location=&section=sales&minPrice=&maxPrice=&pppw_max=99999&minBedrooms=&maxBedrooms=",
		totalPages: 16, // 191 properties / 12 per page
		isRental: false,
		label: "SALES",
	},
	{
		type: "lettings",
		urlBase: "https://www.mypropertybox.co.uk/results-gallery.php?section=lets&ddm_order=2",
		totalPages: 26, // 302 properties / 12 per page
		isRental: true,
		label: "RENTALS",
	},
];

async function run() {
	console.log(`\n🚀 Starting My Property Box scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 300,
		navigationTimeoutSecs: 60,
		async requestHandler({ page, request, log }) {
			const { isRental, pageNum, label } = request.userData;

			log.info(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			// Wait for the gallery items to load
			await page
				.waitForSelector('a[href*="property-details.php"]', { timeout: 30000 })
				.catch(() => log.warn(`⚠️ Property links not found on page ${pageNum}`));

			const properties = await page.evaluate((baseUrl) => {
				const results = [];
				// Select links that go to details and have an ID
				const links = Array.from(document.querySelectorAll('a[href*="property-details.php"]'));

				// Use a Set to avoid duplicates (sometimes there's an image link and a text link)
				const seenIds = new Set();

				links.forEach((link) => {
					const href = link.getAttribute("href");
					const idMatch = href.match(/id=(\d+)/);
					if (!idMatch) return;

					const id = idMatch[1];
					if (seenIds.has(id)) return;
					seenIds.add(id);

					// Check for SSTC badge - skip if found
					const hasSSTCBadge = link.querySelector('img[src="/images/sstc-badge.png"]') !== null;
					if (hasSSTCBadge) return;

					// Extract data from the link's text content (usually contains address and price)
					const text = link.innerText || "";

					// Address is usually the first line or bold text
					const address = text.split("\n")[0].trim() || "Address not found";

					// Price extraction
					const priceMatch = text.replace(/,/g, "").match(/£(\d+)/);
					const price = priceMatch ? parseInt(priceMatch[1], 10) : 0;

					// Bedrooms extraction
					const bedMatch = text.match(/(\d+)\s*Bedroom/i);
					const bedrooms = bedMatch ? parseInt(bedMatch[1], 10) : null;

					const propertyUrl = href.startsWith("http")
						? href
						: baseUrl + (href.startsWith("/") ? "" : "/") + href;

					results.push({
						propertyUrl,
						price,
						address,
						bedrooms,
					});
				});
				return results;
			}, BASE_URL);

			log.info(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 4;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						const detailPage = await page.context().newPage();
						try {
							log.info(`Scraping details: ${property.propertyUrl}`);
							await detailPage.goto(property.propertyUrl, {
								waitUntil: "domcontentloaded",
								timeout: 45000,
							});
							await detailPage.waitForTimeout(500);

							const html = await detailPage.content();

							// Extract coordinates from Google Maps script
							const latLonMatch = html.match(
								/google\.maps\.LatLng\(\s*([-+]?\d*\.?\d+),\s*([-+]?\d*\.?\d+)\s*\)/
							);
							const lat = latLonMatch ? parseFloat(latLonMatch[1]) : null;
							const lon = latLonMatch ? parseFloat(latLonMatch[2]) : null;

							try {
								await updatePriceByPropertyURL(
									property.propertyUrl,
									property.price,
									property.address,
									property.bedrooms,
									AGENT_ID,
									isRental,
									lat,
									lon
								);
								log.info(`✅ Saved: ${property.address}`);
							} catch (err) {
								log.warn(`DB update failed: ${property.propertyUrl} (${err.message})`);
							}
						} catch (err) {
							log.warn(`Failed to process detail page: ${property.propertyUrl} (${err.message})`);
						} finally {
							await detailPage.close();
						}
					})
				);
			}
		},
	});

	for (const config of PROPERTY_TYPES) {
		const requests = [];
		for (let pg = 1; pg <= config.totalPages; pg++) {
			const url = new URL(config.urlBase);
			url.searchParams.set("page", pg);
			requests.push({
				url: url.toString(),
				uniqueKey: `${config.type}-page-${pg}`,
				userData: {
					isRental: config.isRental,
					pageNum: pg,
					label: config.label,
				},
			});
		}
		console.log(`🏠 Enqueuing ${config.label} (${config.totalPages} pages)`);
		await crawler.addRequests(requests);
	}

	await crawler.run();

	for (const config of PROPERTY_TYPES) {
		await updateRemoveStatus(AGENT_ID, config.isRental ? 1 : 0);
	}

	console.log("Crawl finished.");
}

run().catch(console.error);
