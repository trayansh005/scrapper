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
			waitUntil: "networkidle2", 
			timeout: 60000 
		});

		// Anti-block check
		const title = await page.title();
		if (title.includes("Access Denied") || title.includes("Too Many Requests")) {
			console.error(` BLOCKED by OpenRent on ${area} Page ${pageNumber}. Cooling down...`);
			await page.waitForTimeout(60000);
			return;
		}

		await page.waitForTimeout(4000);
		await autoScroll(page);
		await page.waitForTimeout(2500);

		// === MAIN PROPERTY EXTRACTION ===
		const properties = await page.evaluate(() => {
			const items = [];
			let cards = Array.from(document.querySelectorAll('a[href^="/property-to-rent/"]'));

			if (cards.length < 5) {
				cards = Array.from(document.querySelectorAll('a[href*="/properties-to-rent/"]'));
			}

			for (const card of cards) {
				let link = card.href || card.getAttribute('href');
				if (!link || !link.includes('/property-to-rent/')) continue;

				const fullText = card.textContent || '';

				// Price extraction
				let priceText = '';
				const priceMatch = fullText.match(/£[0-9,]+(\s*(?:pcm|p\/w|pw|per month|week))?/i);
				if (priceMatch) {
					priceText = priceMatch[0];
				} else {
					const priceEl = Array.from(card.querySelectorAll('*')).find(el => 
						/£[0-9,]+/.test(el.textContent)
					);
					if (priceEl) priceText = priceEl.textContent.trim();
				}

				// Title
				let title = "OpenRent Property";
				const titleEl = card.querySelector('h2, h3, strong, [class*="title"], [class*="address"]');
				if (titleEl) {
					title = titleEl.textContent.trim().split('\n')[0];
				}

				// Bedrooms
				let bedrooms = null;
				const bedMatch = fullText.match(/(\d+)\s*(?:bed|beds|bedroom|bedrooms)/i);
				if (bedMatch) bedrooms = parseInt(bedMatch[1]);

				const statusText = fullText.toLowerCase();

				if (link) {
					items.push({
						link: link.startsWith('http') ? link : 'https://www.openrent.co.uk' + link,
						title: title.substring(0, 150),
						priceText,
						bedrooms,
						statusText
					});
				}
			}
			return items;
		});

		console.log(`    Found ${properties.length} properties on [${area}] Page ${pageNumber}`);

		// De-duplicate
		const uniqueProperties = [];
		const seenLinks = new Set();
		for (const p of properties) {
			if (!seenLinks.has(p.link)) {
				seenLinks.add(p.link);
				uniqueProperties.push(p);
			}
		}

		const batchSize = 8;
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
						return;
					}

					let price = null;
					if (property.priceText.includes("pw") || property.priceText.toLowerCase().includes("week")) {
						const weeklyPrice = parsePrice(property.priceText.replace(/pw|week/i, ""));
						if (weeklyPrice) price = Math.round((weeklyPrice * 52) / 12);
					} else {
						price = parsePrice(property.priceText);
					}

					if (!price) return;

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
						await new Promise((r) => setTimeout(r, Math.random() * 2000));
						await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
					}
				}),
			);

			await new Promise((r) => setTimeout(r, 5000));
		}

		await new Promise((r) => setTimeout(r, 7000));
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
