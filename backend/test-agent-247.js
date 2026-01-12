// Test script for Agent 247 Darlows extraction
const { PlaywrightCrawler } = require("crawlee");

async function testExtraction() {
	const crawler = new PlaywrightCrawler({
		async requestHandler({ page }) {
			console.log(`Testing URL: ${page.url()}`);

			const detailData = await page.evaluate(() => {
				const data = {
					address: null,
					price: null,
					bedrooms: null,
					lat: null,
					lng: null,
				};

				const h1s = Array.from(document.querySelectorAll("h1"));
				const propertyH1 = h1s.find(
					(h) => h.textContent.includes("●") || !h.textContent.toLowerCase().includes("cookie")
				);

				if (propertyH1) {
					const parts = propertyH1.textContent.split("●");
					if (parts.length >= 2) {
						data.address = parts[1].trim();
					} else {
						data.address = propertyH1.textContent.trim();
					}
				}

				const priceMatch = (propertyH1?.textContent || document.body.innerText).match(/£([0-9,]+)/);
				if (priceMatch) {
					data.price = priceMatch[0];
				}

				const bedMatch = (propertyH1?.textContent || document.body.innerText).match(
					/(\d+)\s*Bedroom/i
				);
				if (bedMatch) {
					data.bedrooms = bedMatch[1].trim();
				}

				const mapLink = document.querySelector("#Map[onclick]");
				if (mapLink) {
					const onClickVal = mapLink.getAttribute("onclick");
					const coordsMatch = onClickVal.match(/([\d\.-]+),\s*([\d\.-]+)/);
					if (coordsMatch) {
						data.lat = coordsMatch[1];
						data.lng = coordsMatch[2];
					}
				}

				if (!data.lat) {
					const similarLink = document.querySelector("a[href*='Latitude=']");
					if (similarLink) {
						const href = similarLink.getAttribute("href");
						const latMatch = href.match(/Latitude=([\d\.-]+)/);
						const lngMatch = href.match(/Longitude=([\d\.-]+)/);
						if (latMatch) data.lat = latMatch[1];
						if (lngMatch) data.lng = lngMatch[2];
					}
				}

				return data;
			});

			console.log("Extracted Data:", JSON.stringify(detailData, null, 2));
		},
	});

	await crawler.run([
		"https://www.darlows.co.uk/buying/6-bedroom-house-for-sale/caerphilly-cf83/dar104220650/",
	]);
}

testExtraction();
