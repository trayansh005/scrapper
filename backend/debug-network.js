const { PlaywrightCrawler } = require("crawlee");

async function run() {
	const crawler = new PlaywrightCrawler({
		maxRequestsPerCrawl: 1,
		requestHandler: async ({ page }) => {
			console.log("Capturing network requests for: " + page.url());
			page.on("request", (request) => {
				const url = request.url();
				if (url.includes("api") || url.includes("json") || url.includes("properties")) {
					console.log(`[NETWORK] ${request.method()} ${url}`);
				}
			});
			try {
				await page.goto(
					"https://www.newtonfallowell.co.uk/properties/for-sale/in-the-midlands/?orderby=price_desc&radius=3",
					{
						waitUntil: "networkidle",
						timeout: 60000,
					},
				);
				await page.waitForTimeout(10000);

				const hasNextData = await page.evaluate(() => !!window.__NEXT_DATA__);
				console.log("Has __NEXT_DATA__:", hasNextData);

				const scripts = await page.evaluate(() => {
					return Array.from(document.querySelectorAll("script"))
						.map((s) => s.src)
						.filter(
							(src) => src.includes("properties") || src.includes("api") || src.includes("json"),
						);
				});
				console.log("Script sources:", scripts);
			} catch (e) {
				console.error("Navigation error:", e.message);
			}
		},
	});

	await crawler.run([
		"https://www.newtonfallowell.co.uk/properties/for-sale/in-the-midlands/?orderby=price_desc&radius=3",
	]);
}

run();
