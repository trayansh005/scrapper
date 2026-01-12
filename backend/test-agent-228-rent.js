// Test script for Agent 228 Rentals
const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 228;
const processedUrls = new Set();

const RENTAL_URL_BASE = "https://www.starkingsandwatson.co.uk/letting/property-search/page/";
const RENTAL_SUFFIX = "/";

async function testRentals() {
	console.log(`\n🚀 Testing Starkings and Watson RENTALS (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 0,
		requestHandlerTimeoutSecs: 300,

		async requestHandler({ page, request }) {
			const { pageNum, label } = request.userData;
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(2000);

			// Wait for property cards
			await page
				.waitForSelector(".card.inview-trigger-animation-fade-in-up-sm", { timeout: 15000 })
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(
						document.querySelectorAll(".card.inview-trigger-animation-fade-in-up-sm")
					);

					return items.map((el) => {
						const statusLabel = el.querySelector(".card__label")?.textContent?.trim() || "";
						const imageFlash = el.querySelector(".image-flash")?.textContent?.trim() || "";

						const linkEl = el.querySelector("a[href*='/property/']");
						const link = linkEl ? linkEl.href : null;
						const title = el.querySelector(".card__title")?.textContent?.trim() || "";
						const rawPrice = el.querySelector(".card__text")?.textContent?.trim() || "";

						return { link, title, rawPrice, statusLabel, imageFlash };
					});
				} catch (err) {
					return [];
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}:`);
			properties.forEach((p) => {
				console.log(
					`  - ${p.title} | ${p.rawPrice} | ${p.statusLabel} | ${p.imageFlash} | ${p.link}`
				);
			});
		},
	});

	await crawler.addRequests([
		{
			url: `${RENTAL_URL_BASE}1${RENTAL_SUFFIX}`,
			userData: { pageNum: 1, label: "TO LET" },
		},
	]);

	await crawler.run();
}

testRentals().catch(console.error);
