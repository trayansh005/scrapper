const { chromium } = require("playwright");
const fs = require("fs");

async function test() {
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;
	const browser = await chromium.connect(browserWSEndpoint);
	const context = await browser.newContext();
	const page = await context.newPage();

	try {
		// Visit normal HTML page
		await page.goto(
			"https://www.bairstoweves.co.uk/properties/sales/status-available/most-recent-first/page-1/#/",
			{ waitUntil: "domcontentloaded" },
		);
		await page.waitForTimeout(5000);

		const html = await page.content();
		fs.writeFileSync("be-page.html", html);
		console.log("Saved be-page.html. Length:", html.length);
	} catch (err) {
		console.error("Error:", err);
	} finally {
		await browser.close();
	}
}

test();
