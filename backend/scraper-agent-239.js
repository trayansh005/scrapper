// HKL Home scraper using Playwright with Crawlee
// Agent ID: 239
// Usage:
// node backend/scraper-agent-239.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 239;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

// 124 records for sale, 10 per page => 13 pages
// 12 records for letting, 10 per page => 2 pages
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.hklhome.co.uk/search/",
	// 	suffix:
	// 		".html?showstc=off&showsold=off&instruction_type=Sale&ajax_polygon=&minprice=&maxprice=&property_type=",
	// 	totalRecords: 124,
	// 	recordsPerPage: 10,
	// 	totalPages: Math.ceil(124 / 10),
	// 	isRental: false,
	// 	label: "FOR SALE",
	// },
	{
		urlBase: "https://www.hklhome.co.uk/search/",
		suffix:
			".html?showstc=off&showsold=off&instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=",
		totalRecords: 12,
		recordsPerPage: 10,
		totalPages: Math.ceil(12 / 10),
		isRental: true,
		label: "FOR LETTING",
	},
];

async function scrapeHKLHome() {
	console.log(`\n🚀 Starting HKL Home scraper (Agent ${AGENT_ID})...\n`);

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

			await page.waitForTimeout(1200);

			// Wait for property cards
			await page
				.waitForSelector(".property", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll(".property"));
					return cards
						.map((card) => {
							try {
								// Extract property URL
								const linkEl = card.querySelector('a[href*="/property-details/"]');
								if (!linkEl) return null;

								const href = linkEl.getAttribute("href");
								if (!href) return null;
								const link = href.startsWith("/") ? `https://www.hklhome.co.uk${href}` : href;

								// Extract price and status from h4
								let price = "";
								let status = "";
								const priceEl = card.querySelector("h4");
								if (priceEl) {
									const priceText = priceEl.textContent.trim();
									// Check for "For Sale" or "To Let" status
									status = priceText;
									const m = priceText.match(/£([0-9,]+)/);
									if (m) {
										price = parseInt(m[1].replace(/,/g, "")).toLocaleString();
									}
								}

								// Skip if status contains sold/let indicators
								if (/sold/i.test(status) || /stc/i.test(status) || /let agreed/i.test(status)) {
									return null;
								}

								// Extract address/title from h3
								let title = "";
								const titleEl = card.querySelector("h3 a");
								if (titleEl) {
									title = titleEl.textContent.trim();
								}

								// Extract bedrooms
								let bedrooms = null;
								const bedroomIcon = card.querySelector(
									'img[src*="bed-purple"], img[alt="bedrooms"]'
								);
								if (bedroomIcon) {
									// Get the next sibling text node or parent's text
									const parent = bedroomIcon.parentElement;
									if (parent) {
										const text = parent.textContent.trim();
										const match = text.match(/(\d+)/);
										if (match) {
											bedrooms = match[1];
										}
									}
								}

								return { link, title, price, bedrooms };
							} catch (e) {
								console.error("Error parsing property card:", e);
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
					console.error("Error in page evaluation:", err);
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						let coords = { latitude: null, longitude: null };

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});
							await detailPage.waitForTimeout(800);

							// Wait for iframe to load
							await detailPage
								.waitForSelector('iframe[src*="google.com/maps"]', { timeout: 7000 })
								.catch(() => null);

							// Extract coordinates from Google Maps iframe
							const iframeCoords = await detailPage.evaluate(() => {
								try {
									const iframe = document.querySelector('iframe[src*="google.com/maps"]');
									if (!iframe) return null;

									const src = iframe.getAttribute("src");
									if (!src) return null;

									// Extract lat/lng from URL format: ?q=51.5359802246093750%2C-0.6448362469673157
									const match = src.match(/[?&]q=([-0-9.]+)%2C([-0-9.]+)/);
									if (match) {
										return {
											latitude: parseFloat(match[1]),
											longitude: parseFloat(match[2]),
										};
									}

									return null;
								} catch (e) {
									return null;
								}
							});

							if (iframeCoords && iframeCoords.latitude && iframeCoords.longitude) {
								coords.latitude = iframeCoords.latitude;
								coords.longitude = iframeCoords.longitude;
								console.log(`  📍 Found coords: ${coords.latitude}, ${coords.longitude}`);
							}
						} catch (err) {
							console.error(`  ⚠️ Error loading detail page: ${err.message}`);
						} finally {
							await detailPage.close();
						}

						try {
							const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;
							const priceNum = parseFloat(priceClean);

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

							const priceDisplay = isNaN(priceNum) ? "N/A" : formatPrice(priceNum);
							if (coords.latitude && coords.longitude) {
								console.log(
									`✅ ${property.title} - ${priceDisplay} - ${coords.latitude}, ${coords.longitude}`
								);
							} else {
								console.log(`✅ ${property.title} - ${priceDisplay} - No coords`);
							}
						} catch (dbErr) {
							console.error(`❌ DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					})
				);

				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = `${propertyType.urlBase}${pg}${propertyType.suffix}`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed HKL Home scraper - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeHKLHome();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
