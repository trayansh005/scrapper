// Ashtons scraper using Playwright with Crawlee
// Agent ID: 240
// Usage:
// node backend/scraper-agent-240.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 240;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();
	try {
		await detailPage.route("**/*", (route) => {
			if (["image", "font", "stylesheet", "media"].includes(route.request().resourceType())) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 30000,
		});

		const htmlContent = await detailPage.content();
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
		);

		stats.totalScraped++;
		if (isRental) {
			stats.savedRentals++;
		} else {
			stats.savedSales++;
		}
	} catch (err) {
		console.error(` Detail scrape error: ${err?.message || err}`);
	} finally {
		await detailPage.close();
	}
}

// Ashtons uses "Show more" button to load properties dynamically
const PROPERTY_TYPES = [
	// {
	// 	url: "https://www.ashtons.co.uk/buy?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
	// 	isRental: false,
	// 	label: "FOR SALE",
	// 	typeIndex: 0,
	// },
	{
		url: "https://www.ashtons.co.uk/rent?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
		isRental: true,
		label: "FOR LETTING",
		typeIndex: 0,
	},
];

async function scrapeAshtons() {
	console.log(`\n🚀 Starting Ashtons scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 2,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 600,

		launchContext: {
			launchOptions: {
				browserWSEndpoint,
			},
		},

		async requestHandler({ page, request }) {
			const { isRental, label, url } = request.userData;

			console.log(`📋 ${label} - ${url}`);

			await page.waitForTimeout(2000);

			// Wait for property cards to load
			await page
				.waitForSelector(".c-property-card", {
					timeout: 15000,
				})
				.catch(() => console.log(`⚠️ No property cards found`));

			// Keep clicking "Show more" button until all properties are loaded
			let clickCount = 0;
			const maxClicks = 50; // Safety limit

			while (clickCount < maxClicks) {
				try {
					// Check if "Show more" button exists and is enabled
					const showMoreButton = await page.$(
						".c-property-search__list-action button.c-button--tertiary",
					);

					if (!showMoreButton) {
						console.log(`  ℹ️ No more "Show more" button found after ${clickCount} clicks`);
						break;
					}

					const isDisabled = await showMoreButton.evaluate((el) => el.disabled);
					if (isDisabled) {
						console.log(`  ℹ️ "Show more" button is disabled after ${clickCount} clicks`);
						break;
					}

					console.log(`  🔄 Clicking "Show more" button (click ${clickCount + 1})...`);

					// Click using JavaScript to avoid timeout issues
					await page.evaluate(() => {
						const button = document.querySelector(
							".c-property-search__list-action button.c-button--tertiary",
						);
						if (button) button.click();
					});

					// Wait for new properties to load
					await page.waitForTimeout(2000);

					// Wait for loading state to finish
					await page
						.waitForFunction(
							() => {
								const button = document.querySelector(
									".c-property-search__list-action button.c-button--tertiary",
								);
								return button && !button.classList.contains("is-waiting");
							},
							{ timeout: 10000 },
						)
						.catch(() => {});

					await page.waitForTimeout(500);
					clickCount++;
				} catch (err) {
					console.log(`  ⚠️ Error clicking "Show more": ${err.message}`);
					break;
				}
			}

			console.log(`  ✅ Finished loading properties after ${clickCount} clicks`);

			const properties = await page.evaluate(() => {
				try {
					const cards = Array.from(document.querySelectorAll(".c-property-card"));
					return cards
						.map((card) => {
							try {
								// Extract property URL
								const linkEl = card.querySelector("a.c-property-card__anchor");
								if (!linkEl) return null;

								const href = linkEl.getAttribute("href");
								if (!href) return null;
								const link = href.startsWith("/") ? `https://www.ashtons.co.uk${href}` : href;

								// Extract price
								let price = "";
								const priceEl = card.querySelector(".c-property-price__value");
								if (priceEl) {
									const priceText = priceEl.textContent.trim();
									const m = priceText.match(/£([0-9,]+)/);
									if (m) {
										price = parseInt(m[1].replace(/,/g, "")).toLocaleString();
									}
								}

								// Extract address/title from h2
								let title = "";
								const titleEl = card.querySelector(".c-property-card__title");
								if (titleEl) {
									title = titleEl.textContent.trim().replace(/\s+/g, " ");
								}

								// Extract bedrooms
								let bedrooms = null;
								const bedroomsFeature = card.querySelector(
									".c-property-feature--bedrooms .c-property-feature__value",
								);
								if (bedroomsFeature) {
									const text = bedroomsFeature.textContent.trim();
									const match = text.match(/(\d+)/);
									if (match) {
										bedrooms = match[1];
									}
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

			console.log(`🔗 Found ${properties.length} properties total`);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						if (processedUrls.has(property.link)) {
							log.info(` Skipping duplicate: ${property.title}`);
							return;
						}
						processedUrls.add(property.link);

						try {
							const priceNum = parsePrice(property.price);

							if (priceNum === null) {
								log.warn(` No price found: ${property.title}`);
								return;
							}

							const result = await updatePriceByPropertyURLOptimized(
								property.link.trim(),
								priceNum,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
							);

							if (result.updated) {
								stats.totalSaved++;
							}

							if (!result.isExisting && !result.error) {
								await scrapePropertyDetail(
									page.context(),
									{
										...property,
										price: priceNum,
									},
									isRental,
								);
							}

							const priceDisplay = formatPriceUk(priceNum);
							console.log(`✅ ${property.title} - ${priceDisplay}`);
						} catch (dbErr) {
							console.error(` DB error for ${property.link}: ${dbErr?.message || dbErr}`);
						}
					}),
				);

				await new Promise((resolve) => setTimeout(resolve, 200));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label}`);

		await crawler.addRequests([
			{
				url: propertyType.url,
				userData: {
					isRental: propertyType.isRental,
					label: propertyType.label,
					url: propertyType.url,
				},
			},
		]);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Ashtons scraper - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

(async () => {
	try {
		await scrapeAshtons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
