const { chromium } = require("playwright");

(async () => {
	const browser = await chromium.launch({ headless: true });
	const page = await browser.newPage();
	const url =
		process.argv[2] ||
		"https://www.linleyandsimpson.co.uk/property-to-rent/2-bedroom-apartment-to-rent-in-st-marys-boothamm-york-yo30-69307cfb0b4bed823d23c5b0/";

	console.log(`Navigating to ${url}...`);
	await page.goto(url, { waitUntil: "domcontentloaded" });
	await page.waitForTimeout(1500);

	try {
		// Scroll into Location section to trigger lazy-loaded iframe
		const locationAnchor = page.locator("#map-holder, iframe#location-map").first();
		if ((await locationAnchor.count()) > 0) {
			await locationAnchor.scrollIntoViewIfNeeded({ timeout: 8000 });
		} else {
			await page.evaluate(() => {
				const scrollEl = document.scrollingElement || document.documentElement;
				if (scrollEl) window.scrollTo(0, scrollEl.scrollHeight);
			});
		}
		await page.waitForTimeout(2000);

		// Print location iframe src
		const iframe = await page.$("iframe#location-map");
		if (iframe) {
			const src = await iframe.evaluate((el) => el.getAttribute("src") || el.src);
			console.log("location-map src:", src);
			const latMatch = src && src.match(/[?&]lat=([0-9.-]+)/);
			const lngMatch = src && src.match(/[?&]lng=([0-9.-]+)/);
			console.log("Parsed lat/lng:", latMatch?.[1] || null, lngMatch?.[1] || null);
		} else {
			console.log("iframe#location-map not found");
		}

		// Extract price from detail page text
		const bodyText = await page.evaluate(
			() => `${document.body?.innerText || ""}\n${document.body?.textContent || ""}`
		);
		const priceMatch = bodyText.match(/£\s*([\d,]+)\s*p\.?c\.?m\.?/i);
		console.log("Price match:", priceMatch ? priceMatch[0] : null);
	} catch (e) {
		console.error(e);
	}

	await browser.close();
})();
