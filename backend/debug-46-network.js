const { chromium } = require("playwright");
(async () => {
	try {
		const browser = await chromium.launch({ headless: true });
		const page = await browser.newPage();
		console.log("--- START DEBUG ---");
		page.on("request", (r) => {
			const u = r.url();
			if (
				u.includes("api") ||
				u.includes(".json") ||
				u.includes("graphql") ||
				u.includes("search")
			) {
				console.log("REQLINK: " + u);
			}
		});
		await page.goto("https://www.connells.co.uk/properties/sales", {
			waitUntil: "networkidle",
			timeout: 60000,
		});
		const data = await page.evaluate(() => ({
			next: !!window.__NEXT_DATA__,
			props: !!window.properties,
			html: document.body.innerText.substring(0, 100),
		}));
		console.log("FLAGS: " + JSON.stringify(data));
		await browser.close();
		console.log("--- END DEBUG ---");
	} catch (e) {
		console.log("ERR: " + e.message);
	}
})();
