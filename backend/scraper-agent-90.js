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
			timeout: 60000 
		});

		const title = await page.title();
		if (title.includes("Access Denied") || title.includes("Too Many Requests")) {
			console.error(` BLOCKED...`);
			await page.waitForTimeout(60000);
			return;
		}

		await page.waitForTimeout(5000);
		await autoScroll(page);
		await page.waitForTimeout(3000);

		// === DEBUG: Get all possible property links ===
		const debugInfo = await page.evaluate(() => {
			const allLinks = Array.from(document.querySelectorAll('a[href*="property-to-rent"]'));
			const items = [];

			allLinks.forEach(link => {
				const href = link.getAttribute('href');
				const text = (link.textContent || '').substring(0, 100);
				items.push({ href, text: text.replace(/\s+/g, ' ') });
			});

			return {
				totalLinks: allLinks.length,
				sample: items.slice(0, 3)
			};
		});

		console.log(`    🔍 Debug - Found ${debugInfo.totalLinks} potential property links`);
		if (debugInfo.sample.length > 0) {
			console.log(`    Sample:`, debugInfo.sample);
		}

		// Main extraction
		const properties = await page.evaluate(() => {
			const items = [];
			const cards = Array.from(document.querySelectorAll('a[href*="/property-to-rent/"]'));

			for (const card of cards) {
				let link = card.href || card.getAttribute('href');
				if (!link) continue;

				const fullText = card.textContent || '';

				let priceText = '';
				const priceMatch = fullText.match(/£[0-9,]+/);
				if (priceMatch) priceText = priceMatch[0];

				let title = fullText.split('\n')[0] || "OpenRent Property";

				let bedrooms = null;
				const bedMatch = fullText.match(/(\d+)\s*(?:bed|beds|bedroom)/i);
				if (bedMatch) bedrooms = parseInt(bedMatch[1]);

				if (link) {
					items.push({
						link: link.startsWith('http') ? link : 'https://www.openrent.co.uk' + link,
						title: title.trim().substring(0, 150),
						priceText,
						bedrooms,
						statusText: fullText.toLowerCase()
					});
				}
			}
			return items;
		});

		console.log(`    Found ${properties.length} properties on [${area}] Page ${pageNumber}`);

		// ... rest of your deduplication and batch processing code remains the same ...

		const uniqueProperties = [];
		const seenLinks = new Set();
		for (const p of properties) {
			if (!seenLinks.has(p.link)) {
				seenLinks.add(p.link);
				uniqueProperties.push(p);
			}
		}

		// (Keep your existing batch processing from here onward)
		const batchSize = 8;
		for (let i = 0; i < uniqueProperties.length; i += batchSize) {
			const batch = uniqueProperties.slice(i, i + batchSize);
			console.log(`    🚀 Processing batch ${Math.floor(i / batchSize) + 1}...`);

			await Promise.all(
				batch.map(async (property) => {
					if (isSoldProperty(property.statusText || "") || 
						property.statusText.includes("let agreed")) return;

					let price = null;
					if (property.priceText) {
						price = parsePrice(property.priceText);
					}
					if (!price) return;

					const updateResult = await updatePriceByPropertyURLOptimized(
						property.link, price, property.title, property.bedrooms, AGENT_ID, isRental
					);

					if (updateResult.updated) stats.totalSaved++;

					if (!updateResult.isExisting && !updateResult.error) {
						console.log(`    🆕 New: ${property.title} - £${price}`);
						await new Promise(r => setTimeout(r, Math.random() * 1500));
						await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
					}
				})
			);
			await new Promise(r => setTimeout(r, 4000));
		}

		await new Promise(r => setTimeout(r, 6000));

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
