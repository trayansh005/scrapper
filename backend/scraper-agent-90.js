// OpenRent scraper using Playwright with Crawlee
// Agent ID: 90
// Website: openrent.co.uk
// Usage:
// node backend/scraper-agent-90.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 90;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

// BROWSERLESS SETUP

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// DETAIL PAGE SCRAPING FUNCTION

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();

	try {
		// Block unnecessary resources
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 60000,
		});

		// Small human-like delay
		await detailPage.waitForTimeout(1500);

		// Extract coordinates from map
		const detailData = await detailPage.evaluate(() => {
			let lat = null;
			let lng = null;

			const mapDiv =
				document.querySelector("#map[data-lat][data-lng]") ||
				document.querySelector("div[data-lat][data-lng]");

			if (mapDiv) {
				lat = parseFloat(mapDiv.getAttribute("data-lat"));
				lng = parseFloat(mapDiv.getAttribute("data-lng"));
			}

			return { lat, lng };
		});

		// Save to database
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			null,                    // html (optional)
			detailData.lat,
			detailData.lng,
		);

		stats.totalScraped++;
		stats.totalSaved++;

		console.log(`    ✅ Detail scraped: ${property.title}`);
	} catch (error) {
		console.error(`    ❌ Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}


// Auto-scroll to load lazy-loaded properties
async function autoScroll(page) {
	await page.evaluate(async () => {
		await new Promise((resolve) => {
			let totalHeight = 0;
			const distance = 700;
			const timer = setInterval(() => {
				window.scrollBy(0, distance);
				totalHeight += distance;
				if (totalHeight >= document.body.scrollHeight - 1200) {
					clearInterval(timer);
					resolve();
				}
			}, 380);
		});
	});
}


// REQUEST HANDLER

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNumber, area } = request.userData;
	console.log(`\n Loading [${label}] ${area} Page ${pageNumber}: ${request.url}`);

	try {
		await page.goto(request.url, {
			waitUntil: "networkidle",
			timeout: 90000
		});

		// Extra time for heavy JS
		await page.waitForTimeout(8000);

		// Multiple scrolls
		for (let i = 0; i < 3; i++) {
			await autoScroll(page);
			await page.waitForTimeout(2000);
		}

		// === DEBUG + EXTRACTION ===
		const result = await page.evaluate(() => {
			const links = Array.from(document.querySelectorAll('a[href*="/property-to-rent/"]'));

			const items = [];
			links.forEach(link => {
				const href = link.getAttribute('href');
				const fullText = (link.textContent || '').trim();

				if (!href || !fullText) return;

				let priceText = (fullText.match(/£[0-9,]+/) || [''])[0];
				let title = fullText.split('\n')[0] || "Property";
				let bedrooms = null;
				const bedMatch = fullText.match(/(\d+)\s*(bed|beds)/i);
				if (bedMatch) bedrooms = parseInt(bedMatch[1]);

				items.push({
					link: href.startsWith('http') ? href : 'https://www.openrent.co.uk' + href,
					title: title.substring(0, 150),
					priceText,
					bedrooms,
					statusText: fullText.toLowerCase()
				});
			});

			return {
				count: items.length,
				sample: items.slice(0, 2)
			};
		});

		console.log(`    🔍 Found ${result.count} property links on Page ${pageNumber}`);
		if (result.sample.length > 0) {
			console.log("    Sample:", result.sample);
		}

		// Use the extracted properties (same logic as above, but return full array)
		const properties = result.count > 0 ? await page.evaluate(() => {
			const items = [];
			document.querySelectorAll('a[href*="/property-to-rent/"]').forEach(linkEl => {
				const href = linkEl.getAttribute('href');
				const fullText = (linkEl.textContent || '').trim();

				if (!href || !fullText) return;

				const priceText = (fullText.match(/\u00a3[0-9,]+/) || [''])[0];
				const title = fullText.split('\n')[0] || "Property";

				let bedrooms = null;
				const bedMatch = fullText.match(/(\d+)\s*(bed|beds)/i);
				if (bedMatch) bedrooms = parseInt(bedMatch[1]);

				items.push({
					link: href.startsWith('http') ? href : 'https://www.openrent.co.uk' + href,
					title: title.substring(0, 150),
					priceText,
					bedrooms,
					statusText: fullText.toLowerCase(),
				});
			});

			return items;
		}) : [];

		if (properties.length === 0) {
			console.log("    ⚠️ Still no properties. Page may be blocked or heavily protected.");
			return;
		}

		// Deduplication + batch processing
		const seen = new Set();
		const deduped = [];
		for (const p of properties) {
			if (!p?.link) continue;
			const key = p.link.trim();
			if (seen.has(key)) continue;
			seen.add(key);
			deduped.push(p);
		}

		const batch = deduped.slice(0, 50);
		console.log(`    Processing ${batch.length} properties (deduped) on Page ${pageNumber}`);

		for (const property of batch) {
			const isSold = isSoldProperty(property.statusText || "");
			if (isSold) continue;

			const price = parsePrice(property.priceText);
			if (!price) {
				console.log(`    ⚠️ Skipping (bad price) ${property.link}`);
				continue;
			}

			// Detail scraping (coords from detail page)
			await scrapePropertyDetail(page.context(), property, isRental);
		}


	} catch (error) {
		console.error(` Error in handleListingPage: ${error.message}`);
	}
}

// CRAWLER SETUP

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1, // Stay at 1 for OpenRent to avoid immediate 429
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 600,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(` Failed listing page: ${request.url}`);
		},
	});
}

// MAIN SCRAPER LOGIC


async function scrapeOpenRent() {
	console.log(` Starting OpenRent Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	// Focus only on Greater London (6,000+ properties)
	const AREAS = [{ name: "Greater London", term: "Greater%20London", pages: 350 }];

	for (const area of AREAS) {
		const requests = [];
		const path = area.term.toLowerCase().replace(/%20/g, "-");
		const baseUrl = `https://www.openrent.co.uk/properties-to-rent/${path}?term=${area.term}&isLive=true`;

		for (let p = 0; p < area.pages; p++) {
			const skip = p * 20;
			const url = skip === 0 ? baseUrl : `${baseUrl}&skip=${skip}`;
			requests.push({
				url,
				userData: {
					pageNumber: p + 1,
					isRental: true,
					label: "RENTALS",
					area: area.name,
				},
			});
		}
		await crawler.addRequests(requests);
	}

	await crawler.run();

	console.log(
		`\n Finished OpenRent - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
}

// MAIN EXECUTION

(async () => {
	try {
		// Start from a random starting area to distribute load if multiple agents run
		await scrapeOpenRent();
		await updateRemoveStatus(AGENT_ID);
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
