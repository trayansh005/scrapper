// Test script for Agent 246 coordinate fix
const { PlaywrightCrawler } = require("crawlee");

async function testExtraction() {
	const crawler = new PlaywrightCrawler({
		async requestHandler({ page }) {
			console.log(`Testing URL: ${page.url()}`);

			const detailData = await page.evaluate(async () => {
				const data = {
					address: null,
					price: null,
					lat: null,
					lng: null,
				};

				const h1 = document.querySelector("#single-property h1") || document.querySelector("h1");
				if (h1) data.address = h1.textContent.trim();

				const priceEl =
					document.querySelector("#single-property .price") || document.querySelector(".price");
				if (priceEl) data.price = priceEl.textContent.trim();

				const latEl = document.getElementById("lat");
				const lngEl = document.getElementById("lng");
				if (latEl && lngEl) {
					data.lat = latEl.getAttribute("value");
					data.lng = lngEl.getAttribute("value");
				}
				return data;
			});

			console.log("Extracted Data:", JSON.stringify(detailData, null, 2));
		},
	});

	await crawler.run(["https://www.simonblyth.co.uk/property/d9e32523-dec4-4a36-8c73-74b128dcbe2a"]);
}

testExtraction();
