// Romans scraper using Playwright with Crawlee
// Agent ID: 16
//
// Usage:
// node backend/scraper-agent-16.js [startPage]
// Example: node backend/scraper-agent-16.js 10 (starts from page 10)

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { logMemoryUsage } = require("./lib/scraper-utils.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { EventEmitter } = require("events");

// Increase max listeners to prevent memory leak warnings
EventEmitter.defaultMaxListeners = 100;

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 16;
let totalScraped = 0;
let totalSaved = 0;

// Small helper utilities to avoid rate-limiting
const userAgents = [
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
];

function sleep(ms) {
	return new Promise((res) => setTimeout(res, ms));
}

function randBetween(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

// Start page
const START_PAGE = 1;

// Configuration for Romans
const PROPERTY_TYPES = [
	// {
	// 	urlBase: "https://www.romans.co.uk/properties/for-sale",
	// 	isRental: false,
	// 	label: "SALES",
	// 	totalRecords: 876,
	// 	recordsPerPage: 8,
	// },
	{
		urlBase: "https://www.romans.co.uk/properties/to-rent",
		isRental: true,
		label: "LETTINGS",
		totalRecords: 537,
		recordsPerPage: 8,
	},
];

async function scrapeRomans() {
	console.log(`\n🚀 Starting Romans scraper (Agent ${AGENT_ID})...\n`);
	logMemoryUsage("START");

	// Browserless configuration
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;

	console.log(
		`🌐 Connecting to browserless for listing and detail pages: ${browserWSEndpoint.split("?")[0]}`,
	);

	// Create a unified Playwright crawler that handles both listing and detail pages
	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 120,

		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData || {};

			const scrapeDetailPage = async (browserContext, prop) => {
				await sleep(1000);
				const detailPage = await browserContext.newPage();
				try {
					try {
						const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
						await detailPage.setUserAgent(ua);
						await detailPage.setExtraHTTPHeaders({ "accept-language": "en-GB,en;q=0.9" });
					} catch (e) {}

					await sleep(randBetween(800, 1800));
					const resp = await detailPage.goto(prop.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});

					if (resp?.status?.() === 429) {
						console.warn(`⚠️ 429 on ${prop.link} — backing off`);
						await sleep(60000);
						throw new Error("429");
					}

					await detailPage.waitForTimeout(2000);

					// Dismiss cookie consent if present
					try {
						const acceptButton = await detailPage.locator('button:has-text("Accept all")').first();
						if (await acceptButton.isVisible({ timeout: 2000 }).catch(() => false)) {
							await acceptButton.click();
							await detailPage.waitForTimeout(1000);
						}
					} catch (e) {
						// No cookie banner or already dismissed
					}

					// Get HTML content first to check for coordinates in script tags
					const htmlContent = await detailPage.content();
					let coords = { latitude: null, longitude: null };

					// Try extracting coordinates from HTML/script tags first (faster)
					const htmlCoords = await extractCoordinatesFromHTML(htmlContent);
					if (htmlCoords.latitude && htmlCoords.longitude) {
						coords = htmlCoords;
						console.log(`✅ Found coords in HTML: ${coords.latitude}, ${coords.longitude}`);
					} else {
						// Only try Streetview if no coords found in HTML
						console.log(`🔍 No coords in HTML, trying Streetview button...`);
						try {
							// Find and click the Streetview button
							const streetviewBtn = await detailPage
								.locator('button:has-text("Streetview")')
								.first();
							const isVisible = await streetviewBtn.isVisible({ timeout: 5000 }).catch(() => false);

							if (isVisible) {
								await streetviewBtn.click();
								console.log(`🗺️ Clicked Streetview button, waiting for Google Maps to load...`);

								// Wait for Google Maps iframe to load
								await detailPage.waitForTimeout(8000);

								// Extract coordinates from Google Maps link with retries
								let googleMapsCoords = null;
								for (let retry = 0; retry < 5; retry++) {
									googleMapsCoords = await detailPage.evaluate(() => {
										const link = document.querySelector('a[href*="google.com/maps/@"]');
										if (link) {
											const href = link.getAttribute("href");
											const match = href.match(/@([\d.-]+),([\d.-]+)/);
											if (match) {
												return { lat: parseFloat(match[1]), lng: parseFloat(match[2]) };
											}
										}
										return null;
									});

									if (googleMapsCoords && googleMapsCoords.lat && googleMapsCoords.lng) {
										console.log(
											`✅ Found coords from Streetview: ${googleMapsCoords.lat}, ${googleMapsCoords.lng}`,
										);
										break;
									}

									if (retry < 4) {
										console.log(`⏳ Retry ${retry + 1}/5 - waiting for coords...`);
										await detailPage.waitForTimeout(3000);
									}
								}

								if (googleMapsCoords && googleMapsCoords.lat && googleMapsCoords.lng) {
									coords.latitude = googleMapsCoords.lat;
									coords.longitude = googleMapsCoords.lng;
								} else {
									console.log(`⚠️ No coords found after 5 retries`);
								}
							} else {
								console.log(`⚠️ Streetview button not visible`);
							}
						} catch (e) {
							console.log(`⚠️ Could not extract streetview coords: ${e.message}`);
						}
					}

					// Use helper to process property with coordinates
					await processPropertyWithCoordinates(
						prop.link,
						prop.price,
						prop.title,
						prop.bedrooms || null,
						AGENT_ID,
						prop.isRental,
						htmlContent,
					);

					totalScraped++;
					totalSaved++;

					const coordsStr =
						coords.latitude && coords.longitude
							? `${coords.latitude}, ${coords.longitude}`
							: "No coords";
					console.log(`✅ ${prop.title} - ${formatPrice(prop.price)} - ${coordsStr}`);
				} finally {
					await detailPage.close();
				}
			};

			// Handle listing pages
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			try {
				const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
				await page.setUserAgent(ua);
				await page.setExtraHTTPHeaders({ "accept-language": "en-GB,en;q=0.9" });
			} catch (e) {}

			await sleep(randBetween(500, 1200));
			const resp = await page.goto(request.url, {
				waitUntil: "domcontentloaded",
				timeout: 30000,
			});

			if (resp?.status?.() === 429) {
				console.warn(`⚠️ 429 on ${request.url} — backing off`);
				await sleep(60000);
				throw new Error("429");
			}

			// Wait for property cards to load
			await page.waitForTimeout(2000);
			await page.waitForSelector(".property-card-wrapper", { timeout: 30000 }).catch(() => {
				console.log(`⚠️ No properties found`);
			});

			// Extract properties using Playwright
			const htmlContent = await page.content();
			const $ = cheerio.load(htmlContent);

			const properties = [];

			$(".property-card-wrapper").each((index, element) => {
				try {
					const $card = $(element);

					// Get the link from the first <a> tag
					const linkEl = $card.find('a[href*="/properties"]').first();
					let href = linkEl.attr("href");
					if (!href) return;

					const link = href.startsWith("http") ? href : "https://www.romans.co.uk" + href;

					// Get the title from h2
					const title = $card.find(".property-title h2").text().trim() || "";

					// Get the price from h3.property-price and sanitize
					const priceText = $card.find(".property-price").text().trim() || "";
					const cardText = $card.text() || "";

					if (isSoldProperty(cardText) || isSoldProperty(priceText)) return;
					const priceMatch = priceText.match(/[0-9][0-9,\s]*/g);
					const priceClean = priceMatch ? priceMatch.join("").replace(/[^0-9]/g, "") : "";
					const price = priceClean ? parseInt(priceClean) : null;

					// Get bedrooms from the icon-bed list item
					let bedrooms = null;
					const bedEl = $card.find(".icon-bed");
					if (bedEl.length && bedEl.parent().length) {
						const bedText = bedEl.parent().text().trim();
						const bedMatch = bedText.match(/(\d+)/);
						bedrooms = bedMatch ? bedMatch[1] : null;
					}

					// Check status - exclude "Let Agreed"
					const status = $card.find(".property-status").text().trim() || "";

					// Skip if status is "Let Agreed"
					if (status === "Let Agreed") {
						return;
					}

					if (link && title && price) {
						properties.push({
							link,
							title,
							price,
							bedrooms,
						});
					}
				} catch (e) {
					// Skip this card
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process properties sequentially - update prices and scrape details for new ones
			for (const p of properties) {
				// First check if property exists using optimized method
				const result = await updatePriceByPropertyURLOptimized(
					p.link,
					p.price,
					p.title,
					p.bedrooms,
					AGENT_ID,
					isRental,
				);

				// If it's a new property, scrape detail page immediately
				if (!result.isExisting && !result.error) {
					console.log(`🆕 Scraping detail for new property: ${p.title}`);
					await scrapeDetailPage(page.context(), { ...p, isRental });
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed listing page: ${request.url}`);
		},
	});

	// Get starting page from command line argument (default to START_PAGE)
	const args = process.argv.slice(2);
	const startPageArg = args.length > 0 ? parseInt(args[0]) : START_PAGE;

	// Process pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		const totalPages =
			propertyType.totalRecords && propertyType.recordsPerPage
				? Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage)
				: 1;
		console.log(`🏠 Queueing ${propertyType.label} pages: ${totalPages} pages`);
		const startPage =
			!isNaN(startPageArg) && startPageArg > 0 && startPageArg <= totalPages
				? startPageArg
				: Math.max(1, START_PAGE);

		console.log(`📋 Starting from page ${startPage} to ${totalPages}`);

		// Queue all pages for this property type
		const requests = [];
		for (let page = startPage; page <= totalPages; page++) {
			// Romans uses /page-N/ format
			const url = `${propertyType.urlBase}/page-${page}/`;
			const uniqueKey = `${propertyType.label}_page_${page}`;

			requests.push({
				url,
				uniqueKey,
				userData: {
					pageNum: page,
					isRental: propertyType.isRental,
					label: propertyType.label,
				},
			});
		}

		// Add all pages and run - they'll process sequentially due to maxConcurrency: 1
		await crawler.addRequests(requests);
		await crawler.run();

		logMemoryUsage(`After ${propertyType.label}`);
	}

	console.log(`\n✅ Completed Romans - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
	logMemoryUsage("END");
}

// Main execution
(async () => {
	try {
		await scrapeRomans();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
