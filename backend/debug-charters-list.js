const { chromium } = require("playwright");

(async () => {
	const browser = await chromium.launch({ headless: true });
	const page = await browser.newPage();
	const url =
		"https://www.chartersestateagents.co.uk/property/for-sale/in-hampshire-and-surrey/exclude-sale-agreed/";
	console.log("Go to listings:", url);
	await page.goto(url, { waitUntil: "networkidle", timeout: 120000 });
	// give hydration time
	await page.waitForTimeout(2000);

	// Try to trigger infinite load if any
	await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
	await page.waitForTimeout(1500);

	const links = await page.evaluate(() => {
		const out = [];
		// Common patterns: anchor within card with href containing 'property-for-sale'
		document.querySelectorAll("a[href*='property-for-sale']").forEach((a) => {
			const href = a.href;
			const title = a.textContent.trim();
			out.push({ href, title });
		});
		return out;
	});

	console.log("Found links:", links.slice(0, 10));
	console.log("Total links:", links.length);

	await browser.close();
})();
