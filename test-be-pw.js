const { chromium } = require("playwright");

async function test() {
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;
	console.log("Connecting to", browserWSEndpoint.split("?")[0]);
	const browser = await chromium.connect(browserWSEndpoint);
	const context = await browser.newContext();
	const page = await context.newPage();

	try {
		console.log("Navigating to API...");
		await page.goto(
			"https://www.bairstoweves.co.uk/properties/sales/status-available/most-recent-first.ljson?page=1",
			{ waitUntil: "domcontentloaded" },
		);

		await page.waitForTimeout(3000); // give it time for cloudflare if needed

		const text = await page.evaluate(() => document.body.innerText);
		console.log("Text length:", text.length);
		console.log("Preview:", text.substring(0, 100));

		try {
			const data = JSON.parse(text);
			console.log(
				"Successfully parsed JSON. Property count:",
				data.properties ? data.properties.length : 0,
			);
		} catch (err) {
			console.log("Could not parse JSON. Received text instead of JSON?");
		}
	} catch (err) {
		console.error("Error:", err);
	} finally {
		await browser.close();
	}
}

test();
