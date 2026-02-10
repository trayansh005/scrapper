// Remax scraper using Playwright with Crawlee
// Agent ID: 32
//
// Usage:
// node backend/scraper-agent-32.js

const { PlaywrightCrawler, sleep } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers");
const { parsePrice, isSoldProperty } = require("./lib/property-helpers");

const AGENT_ID = 32;
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

		// Use page.evaluate for robust extraction directly from the browser context
		const coords = await page.evaluate(() => {
			let lat = null,
				lng = null;

			// 1. Try JSON-LD
			const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
			for (const script of scripts) {
				try {
					const json = JSON.parse(script.innerText);
					const items = json["@graph"] || (Array.isArray(json) ? json : [json]);

					for (const item of items) {
						if (item.geo && item.geo.latitude != null) {
							lat = item.geo.latitude;
							lng = item.geo.longitude;
							break;
						}
						// Direct lat/lng check
						if (item.latitude != null && item.longitude != null) {
							lat = item.latitude;
							lng = item.longitude;
							break;
						}
					}
				} catch (e) {}
				if (lat && lng) break;
			}

			// 2. Fallback: Search script contents for coordinate patterns
			if (!lat || !lng) {
				const allScripts = Array.from(document.querySelectorAll("script"));
				for (const script of allScripts) {
					const content = script.innerText;

					// Match google.maps.LatLng pattern
					const gmapsMatch = content.match(
						/new\s+google\.maps\.LatLng\(\s*([\d.-]+)\s*,\s*([\d.-]+)\s*\)/i,
					);
					if (gmapsMatch) {
						lat = gmapsMatch[1];
						lng = gmapsMatch[2];
						break;
					}

					// Match lat: 53.3, lng: -2.1 pattern
					const coordMatch = content.match(
						/lat\s*[:=]\s*["']?([\d.-]+)["']?\s*,\s*lng\s*[:=]\s*["']?([\d.-]+)["']?/i,
					);
					if (coordMatch) {
						lat = coordMatch[1];
						lng = coordMatch[2];
						break;
					}
				}
			}
			return { lat, lng };
		});

		const { lat, lng } = coords;

		if (lat && lng) {
			console.log(`     Coords: ${lat}, ${lng}`);
		}

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			null, // HTML not needed if lat/lng provided
			lat,
			lng,
		);

		stats.totalSaved++;
	} catch (error) {
		console.error(`     Error detail ${property.link}:`, error.message);
	} finally {
		await page.close();
	}
}

/**
 * Handle listing pages
 */
async function handleListingPage({ request, page, crawler, log }) {
	const { pageNum, isRental, label } = request.userData;
	log.info(`[${label}] Page ${pageNum} - ${request.url}`);

	try {
		await page.waitForSelector(".property-item", { timeout: 30000 });
	} catch (e) {
		log.warning(`No properties found on ${request.url}`);
		return;
	}

	const content = await page.content();
	const $ = cheerio.load(content);
	const $items = $(".property-item");

	log.info(`Found ${$items.length} properties`);

	for (let i = 0; i < $items.length; i++) {
		const $item = $($items[i]);

		const statusText = $item.find(".f-price, .p-name").text() || "";
		if (isSoldProperty(statusText)) {
			console.log(`     Skipping sold: ${$item.find(".p-name").text().trim()}`);
			continue;
		}

		const linkEl = $item.find("a").first();
		let link = linkEl.attr("href");
		if (!link) continue;
		if (!link.startsWith("http")) link = `https://remax.co.uk${link}`;

		const title = $item.find(".p-name").text().trim() || "Remax Property";
		const priceRaw = $item.find(".f-price").text().trim();
		const price = parsePrice(priceRaw);

		const attrText = $item.find(".property-attr").text().trim();
		const bedroomsMatch = attrText.match(/(\d+)\s*Bed/);
		const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

		if (link && price) {
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

	// Pagination logic: keep incrementing until a page returns no items
	if ($items.length > 0) {
		const baseUrl = isRental
			? "https://remax.co.uk/properties-for-rent/"
			: "https://remax.co.uk/properties-for-sale/";
		const nextPage = pageNum + 1;
		const nextUrl = `${baseUrl}?page=${nextPage}`;

		await crawler.addRequests([
			{
				url: nextUrl,
				userData: { pageNum: nextPage, isRental, label },
			},
		]);
	}
}

async function run() {
	console.log(` Starting Remax Refactored (Agent ${AGENT_ID})...`);

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

	// Sales
	initialRequests.push({
		url: `https://remax.co.uk/properties-for-sale/?page=${startPage}`,
		userData: { pageNum: startPage, isRental: false, label: "SALES" },
	});

	// Lettings (only if startPage is 1 or user explicitly wants rentals)
	if (startPage === 1) {
		initialRequests.push({
			url: `https://remax.co.uk/properties-for-rent/?page=1`,
			userData: { pageNum: 1, isRental: true, label: "RENTALS" },
		});
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
