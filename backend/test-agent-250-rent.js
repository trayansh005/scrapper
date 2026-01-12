// Test script for Agent 250 Rentals
const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 250;
const processedUrls = new Set();

const RENTAL_URL =
	"https://www.chartersestateagents.co.uk/property/to-rent/in-hampshire-and-surrey/exclude-let-agreed/";

function buildPagedUrl(urlBase, pageNum) {
	if (pageNum === 1) return urlBase;
	return `${urlBase.endsWith("/") ? urlBase : urlBase + "/"}page-${pageNum}/`;
}

async function testRentals() {
	console.log(`\n🚀 Testing Charters RENTALS (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 0,
		requestHandlerTimeoutSecs: 300,

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page
				.waitForSelector('a[href*="/property-to-rent/"]', { timeout: 20000 })
				.catch(() => console.log(`⚠️ No properties found on page ${pageNum}`));

			await page.waitForTimeout(1500);

			const properties = await page.evaluate(() => {
				const items = Array.from(document.querySelectorAll('a[href*="/property-to-rent/"]'));
				const results = [];
				const seen = new Set();
				for (const el of items) {
					const href = el.getAttribute("href");
					if (!href) continue;
					const link = new URL(href, window.location.origin).href;
					if (seen.has(link)) continue;
					seen.add(link);
					results.push({ link, title: el.textContent?.trim() });
				}
				return results;
			});

			console.log(`🔗 Found ${properties.length} properties`);
			if (properties.length > 0) {
				console.log(`Sample: ${properties[0].link}`);
			}
		},
	});

	await crawler.run([
		{
			url: buildPagedUrl(RENTAL_URL, 1),
			userData: { pageNum: 1, isRental: true, label: "RENTALS" },
		},
	]);

	console.log("\n✅ Test completed!");
}

testRentals().catch(console.error);
