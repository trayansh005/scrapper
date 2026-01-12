// Test script for Palmer Partners (Agent 226)
// Only processes a few properties to verify logic

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 226;
let totalScraped = 0;

function formatPrice(num, isRental) {
	if (!num || isNaN(num)) return isRental ? "£0 pcm" : "£0";
	return "£" + Number(num).toLocaleString("en-GB") + (isRental ? " pcm" : "");
}

async function testScraper() {
	console.log(`\n🧪 Testing Palmer Partners scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestsPerCrawl: 5, // Limit to 5 properties

		async requestHandler({ page, request, enqueueLinks }) {
			const { isRental, label, isDetail } = request.userData;

			if (!isDetail) {
				console.log(`📋 List Page: ${request.url}`);

				// Extract links but only take first 3 for testing
				const detailLinks = await page.evaluate(() => {
					const links = Array.from(document.querySelectorAll('a[href*="/property/"]'));
					return [...new Set(links.map((a) => a.href).filter((href) => !href.includes("#")))].slice(
						0,
						3
					);
				});

				console.log(`🔎 Found ${detailLinks.length} detail links for testing`);

				for (const link of detailLinks) {
					await enqueueLinks({
						urls: [link],
						userData: { isRental, label, isDetail: true },
					});
				}
				return;
			}

			// DETAIL PAGE
			totalScraped++;
			const propertyUrl = request.url;
			console.log(`🏠 Processing (${totalScraped}): ${propertyUrl}`);

			const propData = await page.evaluate(() => {
				const data = {
					price: null,
					address: null,
					lat: null,
					lng: null,
					bedrooms: null,
				};

				const priceInput = document.querySelector('input[name="price"]');
				if (priceInput) data.price = priceInput.value;

				const addressInput = document.querySelector('input[name="propertyAddress"]');
				if (addressInput) data.address = addressInput.value;

				const bedsInput = document.querySelector('input[name="beds"]');
				if (bedsInput) data.bedrooms = bedsInput.value;

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
							const numMatch = bedsLi.textContent.match(/(\d+)/);
							if (numMatch) data.bedrooms = numMatch[1];
						}
					}
				}

				const allHiddenInputs = Array.from(document.querySelectorAll('input[type="hidden"]'));
				for (const input of allHiddenInputs) {
					const val = input.value ? input.value.trim() : "";
					if (val.startsWith("[") && val.includes('"lat"') && val.includes('"lng"')) {
						try {
							const coords = JSON.parse(val);
							if (coords && coords.length > 0) {
								data.lat = coords[0].lat;
								data.lng = coords[0].lng;
								if (!data.price && coords[0].price) data.price = coords[0].price;
								if (!data.address && coords[0].title) data.address = coords[0].title;
								if (!data.bedrooms && coords[0].beds) data.bedrooms = coords[0].beds;
							}
						} catch (e) {}
					}
				}
				return data;
			});

			let numericPrice = 0;
			if (propData.price) {
				const cleanPrice = propData.price
					.replace(/[£,]/g, "")
					.replace(/pcm|pw/gi, "")
					.trim();
				numericPrice = parseFloat(cleanPrice);
			}

			const formattedPrice = formatPrice(numericPrice, isRental);
			console.log(`   Price: ${formattedPrice} (${numericPrice})`);
			console.log(`   Address: ${propData.address}`);
			console.log(`   Coords: ${propData.lat}, ${propData.lng}`);
			console.log(`   Beds: ${propData.bedrooms}`);

			// Only log, don't update DB in test unless wanted
			// await updatePriceByPropertyURL(AGENT_ID, propertyUrl, formattedPrice, numericPrice, propData.address, propData.lat, propData.lng, propData.bedrooms ? parseInt(propData.bedrooms) : null, isRental);
		},
	});

	await crawler.run([
		{
			url: "https://www.palmerpartners.com/buy/property-for-sale/",
			userData: { isRental: false, label: "SALES", isDetail: false },
		},
	]);

	console.log(`\n✅ Test complete.`);
}

testScraper().catch(console.error);
