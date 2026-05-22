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

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function autoScroll(page) {
	// Scrolls the page incrementally to trigger lazy-loaded property cards.
	await page.evaluate(async () => {
		await new Promise((resolve) => {
			let totalHeight = 0;
			const distance = 1000;
			const timer = setInterval(() => {
				const scrollHeight = document.body.scrollHeight;
				window.scrollBy(0, distance);
				totalHeight += distance;
				if (totalHeight >= scrollHeight) {
					clearInterval(timer);
					resolve();
				}
			}, 500);
		});
	});
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
			const distance = 1000;
			const timer = setInterval(() => {
				const scrollHeight = document.body.scrollHeight;
				window.scrollBy(0, distance);
				totalHeight += distance;
				if (totalHeight >= scrollHeight) {
					clearInterval(timer);
					resolve();
				}
			}, 500);
		});
	});
}


// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNumber, area } = request.userData;
	console.log(`\n Loading [${label}] ${area} Page ${pageNumber}: ${request.url}`);

	try {
		await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });

		// Look for 429
		const title = await page.title();
		if (title.includes("Access Denied") || title.includes("Too Many Requests")) {
			console.error(` BLOCKED by OpenRent on ${area} Page ${pageNumber}. Cooling down...`);
			await page.waitForTimeout(60000);
			return;
		}

		await page.waitForTimeout(2000);

		// Wait for property cards
		await page.waitForSelector("a.pli.search-property-card", { timeout: 30000 }).catch(() => {
			console.log(`    No properties found on [${area}] Page ${pageNumber}`);
		});

		// Extract properties old html struture 
		// const properties = await page.evaluate(() => {
		// 	const containers = Array.from(document.querySelectorAll("a.pli.search-property-card"));
		// 	const items = [];

		// 	for (const container of containers) {
		// 		const link = container.href;
		// 		const priceMonthlyEl = container.querySelector(".pim .fs-4");
		// 		const priceWeeklyEl = container.querySelector(".piw .fs-4");

		// 		let priceText = "";
		// 		if (priceMonthlyEl) {
		// 			priceText = priceMonthlyEl.textContent.trim();
		// 		} else if (priceWeeklyEl) {
		// 			priceText = priceWeeklyEl.textContent.trim() + " pw";
		// 		}

		// 		const title = container.querySelector(".fs-3")?.textContent?.trim() || "OpenRent Property";
		// 		const statusText = container.innerText || "";

		// 		let bedrooms = null;
		// 		const featuresEl = container.querySelector("ul.inline-list-divide");
		// 		if (featuresEl) {
		// 			const text = featuresEl.textContent;
		// 			const bedMatch = text.match(/(\d+)\s*(beds?|bedrooms?)/i);
		// 			const roomMatch = text.match(/(\d+)\s*(rooms?)/i);
		// 			if (bedMatch) bedrooms = parseInt(bedMatch[1]);
		// 			else if (roomMatch) bedrooms = parseInt(roomMatch[1]);
		// 		}

		// 		if (link && priceText) {
		// 			items.push({ link, title, priceText, bedrooms, statusText });
		// 		}
		// 	}
		// 	return items;
		// });


		// new html struture 
		const properties = await page.evaluate(() => {

			const cards = Array.from(document.querySelectorAll('a[href^="/properties-to-rent/"]'));

			const items = [];

			for (const card of cards) {
				const link = card.href;
				if (!link || !link.includes('/properties-to-rent/')) continue;

				// Price - multiple possible locations
				let priceText = '';
				const priceEls = card.querySelectorAll('text, strong, div, span');
				for (const el of priceEls) {
					const text = el.textContent.trim();
					if (text.includes('£') && (text.includes('month') || text.includes('week') || text.match(/per\s*(month|week)/i))) {
						priceText = text;
						break;
					}
				}

				const titleEl = card.querySelector('h2, h3, .title, [class*="title"]') ||
					Array.from(card.querySelectorAll('*')).find(el =>
						el.textContent && el.textContent.includes('Bed') && el.textContent.length < 100
					);

				const title = titleEl ? titleEl.textContent.trim() : "OpenRent Property";

				// Bedrooms
				let bedrooms = null;
				const bedText = card.textContent;
				const bedMatch = bedText.match(/(\d+)\s*(?:bed|beds|bedroom|bedrooms)/i);
				if (bedMatch) bedrooms = parseInt(bedMatch[1]);

				const statusText = card.textContent.toLowerCase();

				if (link && (priceText || title)) {
					items.push({
						link,
						title,
						priceText,
						bedrooms,
						statusText
					});
				}
			}

			return items;
		});

		// quick debug
		// === REPLACE your existing properties = await page.evaluate(() => { ... }) block with this ===

		// De-duplicate properties on the same page
		const uniqueProperties = [];
		const seenLinks = new Set();
		for (const p of properties) {
			if (!seenLinks.has(p.link)) {
				seenLinks.add(p.link);
				uniqueProperties.push(p);
			}
		}

		console.log(
			`    Found ${uniqueProperties.length} unique properties on [${area}] Page ${pageNumber}`,
		);

		const batchSize = 10;
		for (let i = 0; i < uniqueProperties.length; i += batchSize) {
			const batch = uniqueProperties.slice(i, i + batchSize);
			console.log(
				`    🚀 Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(uniqueProperties.length / batchSize)} for [${area}] p${pageNumber}...`,
			);

			await Promise.all(
				batch.map(async (property) => {
					if (
						isSoldProperty(property.statusText || "") ||
						property.statusText.toLowerCase().includes("let agreed")
					) {
						// console.log(`    ⏭️ Skipping let agreed: ${property.title}`);
						return;
					}

					// OpenRent specific price parsing for weekly
					let price = null;
					if (property.priceText.includes("pw")) {
						const weeklyPrice = parsePrice(property.priceText.replace("pw", ""));
						if (weeklyPrice) price = Math.round((weeklyPrice * 52) / 12);
					} else {
						price = parsePrice(property.priceText);
					}

					if (!price) {
						return;
					}

					const updateResult = await updatePriceByPropertyURLOptimized(
						property.link,
						price,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
					);

					if (updateResult.updated) {
						stats.totalSaved++;
					}

					if (!updateResult.isExisting && !updateResult.error) {
						console.log(`    🆕 New property: ${property.title} - £${price}`);
						// Small jittered delay to avoid hitting detail pages at the exact same millisecond
						await new Promise((r) => setTimeout(r, Math.random() * 2000));
						await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
					}
				}),
			);

			// Increased delay after each batch of 10 to cool down
			await new Promise((r) => setTimeout(r, 5000));
		}

		// Wait between listing pages
		await new Promise((r) => setTimeout(r, 6000));
	} catch (error) {
		console.error(` Error in handleListingPage: ${error.message}`);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

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

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

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

// ============================================================================
// MAIN EXECUTION
// ============================================================================

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
