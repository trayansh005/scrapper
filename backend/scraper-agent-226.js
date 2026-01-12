// Palmer Partners scraper using Playwright with Crawlee
// Agent ID: 226
// Usage:
// node backend/scraper-agent-226.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 226;
let totalScraped = 0;
let totalSaved = 0;
let savedSales = 0;
let savedRentals = 0;
const processedUrls = new Set();

function formatPrice(num, isRental) {
	if (!num || isNaN(num)) return isRental ? "£0 pcm" : "£0";
	return "£" + Number(num).toLocaleString("en-GB") + (isRental ? " pcm" : "");
}

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.palmerpartners.com/buy/property-for-sale/",
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.palmerpartners.com/let/property-to-let/",
		isRental: true,
		label: "LETTINGS",
	},
];

async function scrapePalmerPartners() {
	console.log(`\n🚀 Starting Palmer Partners scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request, enqueueLinks }) {
			const { isRental, label, isDetail } = request.userData;

			if (!isDetail) {
				// LIST PAGE
				console.log(`📋 ${label} - List Page - ${request.url}`);

				// Wait for properties to load
				await page.waitForSelector('a[href*="/property/"]', { timeout: 15000 }).catch(() => {
					console.log(`⚠️ No properties found on current list page`);
				});

				// Extract links to detail pages
				const detailLinks = await page.evaluate((isRental) => {
					const links = Array.from(document.querySelectorAll('a[href*="/property/"]'));
					return links.map((a) => a.href).filter((href) => !href.includes("#"));
				}, isRental);

				for (const link of detailLinks) {
					if (!processedUrls.has(link)) {
						await enqueueLinks({
							urls: [link],
							userData: {
								isRental,
								label,
								isDetail: true,
							},
						});
					}
				}

				// Check for next page if we are on page 1 of the list
				// We can just enqueue pages 2, 3, 4 etc. if they exist
				if (!request.url.includes("page=")) {
					// Extract max page from pagination
					const maxPage = await page.evaluate(() => {
						const paginationLinks = Array.from(
							document.querySelectorAll("ul.pagination li a, .pagination a")
						);
						let highest = 1;
						paginationLinks.forEach((a) => {
							const val = parseInt(a.textContent.trim());
							if (!isNaN(val) && val > highest) highest = val;
						});
						return highest;
					});

					if (maxPage > 1) {
						console.log(`🔢 Detected ${maxPage} pages for ${label}`);
						for (let p = 2; p <= maxPage; p++) {
							const pageUrl = `${request.url}${request.url.includes("?") ? "&" : "?"}page=${p}`;
							await enqueueLinks({
								urls: [pageUrl],
								userData: { isRental, label, isDetail: false },
							});
						}
					}
				}
				return;
			}

			// DETAIL PAGE
			totalScraped++;
			const propertyUrl = request.url;
			processedUrls.add(propertyUrl);

			console.log(`🏠 Processing (${totalScraped}): ${propertyUrl}`);

			try {
				// Extract data from hidden inputs
				const propData = await page.evaluate(() => {
					const data = {
						price: null,
						address: null,
						lat: null,
						lng: null,
						bedrooms: null,
						propertyCode: null,
					};

					// Price
					const priceInput = document.querySelector('input[name="price"]');
					if (priceInput) data.price = priceInput.value;

					// Address
					const addressInput = document.querySelector('input[name="propertyAddress"]');
					if (addressInput) data.address = addressInput.value;

					// Property Code
					const codeInput = document.querySelector('input[name="propertyCode"]');
					if (codeInput) data.propertyCode = codeInput.value;

					// Bedrooms
					const bedsInput = document.querySelector('input[name="beds"]');
					if (bedsInput) data.bedrooms = bedsInput.value;

					// Fallback for bedrooms from UI
					if (!data.bedrooms) {
						const allLi = Array.from(document.querySelectorAll("li"));
						const bedsLi = allLi.find((li) => li.textContent.includes("Bedrooms"));
						if (bedsLi) {
							const match =
								bedsLi.textContent.match(/Bedrooms\s*(\d+)/i) ||
								bedsLi.textContent.match(/(\d+)\s*Bedrooms/i);
							if (match) {
								data.bedrooms = match[1];
							} else {
								// Try just finding the number in the text
								const numMatch = bedsLi.textContent.match(/(\d+)/);
								if (numMatch) data.bedrooms = numMatch[1];
							}
						}
					}

					// Coordinates from JSON hidden input
					const allHiddenInputs = Array.from(document.querySelectorAll('input[type="hidden"]'));
					for (const input of allHiddenInputs) {
						const val = input.value ? input.value.trim() : "";
						if (val.startsWith("[") && val.includes('"lat"') && val.includes('"lng"')) {
							try {
								const coords = JSON.parse(val);
								if (coords && coords.length > 0) {
									data.lat = coords[0].lat;
									data.lng = coords[0].lng;
									// Fallback for missing fields if they are in JSON
									if (!data.price && coords[0].price) data.price = coords[0].price;
									if (!data.address && coords[0].title) data.address = coords[0].title;
									if (!data.bedrooms && coords[0].beds) data.bedrooms = coords[0].beds;
								}
							} catch (e) {
								// ignore parse error
							}
						}
					}

					return data;
				});

				if (!propData.price || !propData.address) {
					console.log(`⚠️ Missing critical data for ${propertyUrl}`);
					return;
				}

				// Clean price
				let numericPrice = 0;
				if (propData.price) {
					// Remove currency symbols, commas, and pcm/pw
					const cleanPrice = propData.price
						.replace(/[£,]/g, "")
						.replace(/pcm|pw/gi, "")
						.trim();
					numericPrice = parseFloat(cleanPrice);
				}

				if (isNaN(numericPrice) || numericPrice === 0) {
					console.log(`⚠️ Invalid price "${propData.price}" for ${propertyUrl}`);
					return;
				}

				const formattedPrice = formatPrice(numericPrice, isRental);
				const bedrooms = propData.bedrooms ? parseInt(propData.bedrooms) : null;

				// Log what we found
				console.log(
					`   Price: ${formattedPrice} | Coords: ${propData.lat}, ${propData.lng} | Address: ${propData.address}`
				);

				// Update database
				const result = await updatePriceByPropertyURL(
					AGENT_ID,
					propertyUrl,
					formattedPrice,
					numericPrice,
					propData.address,
					propData.lat,
					propData.lng,
					bedrooms,
					isRental
				);

				if (result) {
					totalSaved++;
					if (isRental) savedRentals++;
					else savedSales++;
				}
			} catch (error) {
				console.error(`❌ Error processing ${propertyUrl}: ${error.message}`);
			}
		},

		async failedRequestHandler({ request }) {
			console.error(`❌ Request ${request.url} failed after multiple retries.`);
		},
	});

	// Initial requests
	const initialRequests = PROPERTY_TYPES.map((type) => ({
		url: type.urlBase,
		userData: {
			isRental: type.isRental,
			label: type.label,
			isDetail: false,
		},
	}));

	await crawler.run(initialRequests);

	console.log(`\n✅ Scraping finished!`);
	console.log(`📊 Total properties processed: ${totalScraped}`);
	console.log(`💾 Total properties saved/updated: ${totalSaved}`);
	console.log(`   💰 Sales: ${savedSales}`);
	console.log(`   🏠 Rentals: ${savedRentals}`);

	// Mark properties not found in this run as removed
	// Note: In some setups, we only do this if we are sure we scraped EVERY property.
	// if (totalScraped > 50) {
	// 	await updateRemoveStatus(AGENT_ID, Array.from(processedUrls));
	// }
}

scrapePalmerPartners().catch((err) => {
	console.error("Fatal error:", err);
	process.exit(1);
});
