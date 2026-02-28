require("dotenv").config();
const { chromium } = require("playwright");

async function test() {
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;
	const browser = await chromium.connect(browserWSEndpoint);
	const context = await browser.newContext();
	const page = await context.newPage();

	try {
		console.log("Loading page...");
		await page.goto(
			"https://www.bairstoweves.co.uk/properties/sales/status-available/most-recent-first/page-1/#/",
			{ waitUntil: "domcontentloaded" },
		);
		await page.waitForTimeout(5000); // Wait for CF and load

		console.log("Checking for embedded property data...");
		const propertiesJSON = await page.evaluate(() => {
			// Look for script tags that might contain the data
			const scripts = Array.from(document.querySelectorAll("script"));
			for (const s of scripts) {
				if (s.innerText.includes("window.Homeflow") || s.innerText.includes('"properties":[')) {
					return s.innerText.substring(0, 500); // Return preview
				}
			}
			return null;
		});

		console.log("Script content found:", propertiesJSON ? "Yes" : "No");
		if (propertiesJSON) console.log(propertiesJSON);
	} catch (err) {
		console.error("Error:", err);
	} finally {
		await browser.close();
	}
}

test();
