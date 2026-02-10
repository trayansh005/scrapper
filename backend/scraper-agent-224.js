const { PlaywrightCrawler, sleep } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers");
const { parsePrice, isSoldProperty } = require("./lib/property-helpers");

const AGENT_ID = 224;
const stats = { totalScraped: 0, totalSaved: 0 };

/**
 * Get the Browserless WebSocket endpoint
 */
function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

/**
 * Scrape individual property details
 */
async function scrapePropertyDetail(browserContext, property, isRental) {
	const page = await browserContext.newPage();
	try {
		console.log(`    Detail: ${property.link}`);
		await page.goto(property.link, { waitUntil: "domcontentloaded", timeout: 60000 });

		const content = await page.content();
		const $ = cheerio.load(content);

		// Mistoria detail pages often have coordinates in a script or we can just try to find them
		// Usually Mistoria uses PropertyHive or similar. Let's look for JSON-LD.
		let lat = null,
			lng = null;

		$('script[type="application/ld+json"]').each((i, el) => {
			try {
				const json = JSON.parse($(el).html());
				const graph = json["@graph"] || [json];
				for (const item of graph) {
					if (item.geo && item.geo.latitude) {
						lat = item.geo.latitude;
						lng = item.geo.longitude;
					}
				}
			} catch (e) {}
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			lat,
			lng,
		);

		stats.totalSaved++;
	} catch (err) {
		console.error(`    Detail Error (${property.link}):`, err.message);
	} finally {
		await page.close();
	}
}

/**
 * Handle listing pages
 */
async function handleListingPage({ request, page, log }) {
	const { pageNum, isRental } = request.userData;
	log.info(`Processing ${request.userData.label} page ${pageNum}`);

	try {
		await page.waitForSelector("li.type-property", { timeout: 20000 });
	} catch (e) {
		log.warning(`No properties found on ${request.url}`);
		return;
	}

	const content = await page.content();
	const $ = cheerio.load(content);
	const $items = $("li.type-property");

	log.info(`Found ${$items.length} properties`);

	for (let i = 0; i < $items.length; i++) {
		const $item = $($items[i]);

		const statusText = $item.find(".flag, div.price, h3").text() || "";
		if (isSoldProperty(statusText)) continue;

		const link = $item.find("h3 a").attr("href");
		const title = $item.find("h3 a").text().trim();

		// Extract price, ensuring we remove the "Tenancy Info" popup text often found in rentals
		const $priceContainer = $item.find("div.price").clone();
		$priceContainer.find("span.lettings-fees, div.propertyhive_lettings_fees_popup").remove();
		const priceRaw = $priceContainer.text().trim();
		const price = parsePrice(priceRaw);

		const bedroomsMatch = $item.find(".room-bedrooms").text().match(/\d+/);
		const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

		if (!link || !price) continue;

		stats.totalScraped++;

		const result = await updatePriceByPropertyURLOptimized(
			link,
			price,
			title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		if (!result.isExisting || result.updated) {
			await scrapePropertyDetail(page.context(), { link, price, title, bedrooms }, isRental);
			await sleep(500);
		}
	}
}

// MAIN SCRAPER LOGIC
// ============================================================================

async function run() {
	console.log(` Starting Mistoria Refactored (Agent ${AGENT_ID})...`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const browserWSEndpoint = getBrowserlessEndpoint();

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 360,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(` Failed request: ${request.url}`);
		},
	});

	const initialRequests = [];

	// Build Sales requests (approx 5-10 pages)
	for (let pg = Math.max(1, startPage); pg <= 10; pg++) {
		const url = `https://mistoriaestateagents.co.uk/property-search/page/${pg}/?address_keyword&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&department=residential-sales&availability&maximum_bedrooms`;
		initialRequests.push({
			url,
			userData: { pageNum: pg, isRental: false, label: "SALES" },
		});
	}

	// Build Lettings requests (approx 10-15 pages)
	if (startPage === 1) {
		for (let pg = 1; pg <= 15; pg++) {
			const url = `https://mistoriaestateagents.co.uk/property-search/page/${pg}/?address_keyword=&department=residential-lettings&availability=&minimum_bedrooms=&maximum_bedrooms=`;
			initialRequests.push({
				url,
				userData: { pageNum: pg, isRental: true, label: "RENTALS" },
			});
		}
	}

	await crawler.run(initialRequests);
	await updateRemoveStatus(AGENT_ID);

	console.log(`\n Completed Agent ${AGENT_ID}`);
	console.log(`Total Scraped: ${stats.totalScraped}`);
	console.log(`Total Saved: ${stats.totalSaved}`);
}

if (require.main === module) {
	run().catch((err) => {
		console.error(` Fatal Error:`, err.message);
		process.exit(1);
	});
}

module.exports = { run };
