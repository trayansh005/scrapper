const { chromium } = require("playwright");

(async () => {
	const browser = await chromium.connectOverCDP("http://localhost:9222");
	const context = browser.contexts()[0];
	const page = await context.newPage();

	page.on("response", async (response) => {
		const url = response.url();
		if (url.includes("_rsc=")) {
			console.log(`[RSC] ${url}`);
			try {
				const text = await response.text();
				console.log(`Body (first 500 chars): ${text.substring(0, 500)}`);
			} catch (e) {
				console.log("Could not get body");
			}
		}
	});

	await page.goto("https://www.connells.co.uk/properties/sales", { waitUntil: "networkidle" });

	// Trigger scroll or click to get more RSC payloads if needed
	await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
	await new Promise((r) => setTimeout(r, 5000));

	await browser.close();
})();
