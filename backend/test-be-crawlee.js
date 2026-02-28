const { PlaywrightCrawler, log } = require("crawlee");

log.setLevel(log.LEVELS.ERROR);

async function test() {
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;
	console.log("Connecting to browserless:", browserWSEndpoint.split("?")[0]);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 1,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: async ({ page, request }) => {
			console.log("Navigated to API...");

			// wait for network idle to ensure challenge passes
			await page.waitForTimeout(3000);

			const content = await page.evaluate(() => document.body.innerText);
			console.log("Content length:", content.length);
			console.log("Preview:", content.substring(0, 100));

			try {
				const data = JSON.parse(content);
				console.log(
					"Successfully parsed JSON. Properties array length:",
					data.properties ? data.properties.length : "undefined",
				);
			} catch (e) {
				console.log("Failed to parse JSON.");
			}
		},
	});

	await crawler.run([
		{
			url: "https://www.bairstoweves.co.uk/properties/sales/status-available/most-recent-first.ljson?page=1",
		},
	]);
}

test();
