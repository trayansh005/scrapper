// Avocado Property Agents scraper using Playwright with Crawlee
// Agent ID: 236
// Usage:
// node backend/scraper-agent-236.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 236;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://avocadopropertyagents.co.uk/property-for-sale?page=",
	// 	totalRecords: 172,
	// 	recordsPerPage: 22,
	// 	totalPages: Math.ceil(172 / 22),
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	suffix: "",
	// },
	{
		urlBase:
			"https://avocadopropertyagents.co.uk/property-to-rent/property/any-bed/all-location?exclude=1&page=",
		totalRecords: 5,
		recordsPerPage: 22,
		totalPages: Math.ceil(5 / 22) || 1,
		isRental: true,
		label: "TO LET",
		suffix: "",
	},
];

async function scrapeAvocado() {
	console.log(`\n🚀 Starting Avocado scraper (Agent ${AGENT_ID})...\n`);

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

			// Wait for card elements
			await page
				.waitForSelector(".card", { timeout: 15000 })
				.catch(() => console.log(`⚠️ No cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll(".card"));
					return cards
						.filter((card) => !card.classList.contains("card--property-worth"))
						.map((card) => {
							try {
								// Find anchor inside card (image or title link)
								const a = card.querySelector('a[href*="/property/"]');
								if (!a) return null;
								const href = a.getAttribute("href");
								const link = href.startsWith("/")
									? `https://avocadopropertyagents.co.uk${href}`
									: href;

								// Price
								const priceEl = card.querySelector(".price-value, .card-price, .price");
								const rawPrice = priceEl ? priceEl.textContent.trim() : "";
								let price = "";
								if (rawPrice) {
									const m = rawPrice.match(/[0-9,.]+/);
									if (m) price = parseInt(m[0].replace(/,/g, "")).toLocaleString();
								}

								// Title
								const title = a.textContent ? a.textContent.trim() : "";

								// Bedrooms: look for i.fa-bed then the following .number span
								let bedrooms = null;
								const bedIcon = card.querySelector("i.fa-bed, .icon-bedroom");
								if (bedIcon) {
									// Try immediate sibling(s)
									let el = bedIcon.nextElementSibling;
									while (el && !(el.classList && el.classList.contains("number")))
										el = el.nextElementSibling;
									if (el && el.textContent && el.textContent.trim()) {
										bedrooms = el.textContent.trim();
									} else {
										// Fallback: take the first .number inside the card detail (usually bedrooms)
										const nums = Array.from(
											card.querySelectorAll(".card-content__detail .number, .number")
										);
										if (nums.length) bedrooms = nums[0].textContent.trim();
									}
								} else {
									// If there's no bed icon, still try to find a .number that appears near bedroom text
									const nums = Array.from(
										card.querySelectorAll(".card-content__detail .number, .number")
									);
									if (nums.length) bedrooms = nums[0].textContent.trim();
								}

								return { link, title, price, bedrooms };
							} catch (e) {
								return null;
							}
						})
						.filter((p) => p !== null);
				} catch (err) {
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
							await detailPage.waitForTimeout(500);

							// Wait for maps iframe to appear (some pages render it after JS runs)
							await detailPage
								.waitForSelector(
									'.mapsEmbed iframe, iframe[src*="maps.google.com/maps"], iframe[src*="google.com/maps"]',
									{ timeout: 5000 }
								)
								.catch(() => null);

							// Look for iframe with Google Maps embed and parse q=lat,lng
							const iframeCoords = await detailPage.evaluate(() => {
								try {
									const ifr = document.querySelector(
										'iframe[src*="maps.google.com/maps"], iframe[src*="google.com/maps"]'
									);
									if (!ifr) return null;
									const src = ifr.getAttribute("src") || "";
									const m = src.match(/[?&]q=([0-9.-]+),([0-9.-]+)/);
									if (m) return { latitude: parseFloat(m[1]), longitude: parseFloat(m[2]) };

									// fallback: look for 'q=lat%2Clng' encoded
									const m2 = src.match(/[?&]q=([0-9.%,-]+)/);
									if (m2) {
										try {
											const decoded = decodeURIComponent(m2[1]);
											const parts = decoded.split(",");
											if (parts.length >= 2)
												return { latitude: parseFloat(parts[0]), longitude: parseFloat(parts[1]) };
										} catch (e) {
											// ignore
										}
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
							// ignore detail failures
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
		`\n✅ Completed Avocado scraper - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeAvocado();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
