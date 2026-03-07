// Ashtons scraper using Playwright with Crawlee
// Agent ID: 240
// Usage: node backend/scraper-agent-240.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Inline sleep
function sleep(ms) {
	return new Promise(resolve => setTimeout(resolve, ms));
}

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 240;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		"ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
	);
}

// ------------------------------------------------------------------
// DETAIL PAGE
// ------------------------------------------------------------------

async function scrapePropertyDetail(context, property, isRental) {
	await sleep(1200 + Math.random() * 800);

	const detailPage = await context.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		logger.step(`[Detail] ${property.title || 'Property'}`);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 45000,
		});

		const htmlContent = await detailPage.content();

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent
		);

		stats.totalScraped++;
		if (isRental) stats.savedRentals++;
		else stats.savedSales++;

		logger.step(`[Detail] Saved`);
	} catch (err) {
		logger.error(`Detail failed → ${property.link}`, err.message || err);
	} finally {
		await detailPage.close().catch(() => { });
	}
}

// ------------------------------------------------------------------
// PROPERTY TYPES
// ------------------------------------------------------------------

const PROPERTY_TYPES = [
	{
		url: "https://www.ashtons.co.uk/buy?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
		isRental: false,
		label: "FOR SALE",
	},
	{
		url: "https://www.ashtons.co.uk/rent?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
		isRental: true,
		label: "FOR LETTING",
	},
];

// ------------------------------------------------------------------
// MAIN SCRAPER
// ------------------------------------------------------------------

async function scrapeAshtons() {
	const scrapeStartTime = new Date();
	logger.step(`Starting Ashtons (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Browserless connected`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 60,
		requestHandlerTimeoutSecs: 600,

		launchContext: { launchOptions: { browserWSEndpoint } },

		preNavigationHooks: [
			async ({ page }) => await blockNonEssentialResources(page),
		],

		async requestHandler({ page, request }) {
			const { isRental, label } = request.userData;
			logger.step(`Processing ${label}`);

			await sleep(2500 + Math.random() * 1500);

			// Scroll to load everything
			logger.step("Infinite scroll loading...");
			let prevHeight = 0;
			let attempts = 0;
			const maxAttempts = 60;

			while (attempts < maxAttempts) {
				await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
				await sleep(3500 + Math.random() * 2000);

				const height = await page.evaluate(() => document.body.scrollHeight);
				if (height === prevHeight) {
					logger.step(`Scroll stopped - no more content (attempt ${attempts})`);
					break;
				}
				prevHeight = height;
				attempts++;
				if (attempts % 5 === 0) logger.step(`Scroll ${attempts}/${maxAttempts}`);
			}

			// Debug: screenshot + broad card count
			const safeLabel = label.toLowerCase().replace(/\s+/g, '-');
			const shotPath = `ashtons-${safeLabel}-final.png`;
			await page.screenshot({ path: shotPath, fullPage: true });
			logger.step(`Screenshot: ${shotPath}`);

			const potentialCards = await page.$$eval(
				'article, li, div[class*="card"], div[class*="property"], div[class*="listing"], div[data-testid*="property"], .result, .item',
				els => els.length
			);
			logger.step(`Detected ${potentialCards} potential cards`);

			// Extract – loose text-based fallback
			const properties = await page.evaluate(() => {
				const items = [];
				const cardElements = document.querySelectorAll(
					'article, li, div[class*="card"], div[class*="property"], div[class*="listing"], div[data-testid*="property"], .result, .item'
				);

				Array.from(cardElements).slice(0, 300).forEach((card) => {
					try {
						// Link
						const linkEl = card.querySelector('a[href*="/property/"], a[href*="/details/"], a[href*="property-"], a');
						if (!linkEl) return;
						let href = linkEl.getAttribute('href');
						if (!href) return;
						const link = href.startsWith('http') ? href : `https://www.ashtons.co.uk${href.startsWith('/') ? '' : '/'}${href}`;

						const cardText = card.innerText.trim().replace(/\s+/g, ' ');

						// Price - look for £ followed by digits, possibly with pcm
						const priceMatch = cardText.match(/£[\d,]+(?:\s*(?:pcm|per\s*calendar\s*month|monthly))?/i);
						let price = priceMatch ? priceMatch[0].replace(/[£,]/g, '').trim() : null;
						if (!price) return;

						// Title
						let title = '';
						const heading = card.querySelector('h1,h2,h3,h4,h5,strong,.address,.title,.location,.property-address');
						if (heading) {
							title = heading.innerText.trim();
						} else {
							title = cardText.split(/[\n•]/)[0]?.trim() || '';
						}
						if (!title || title.length < 8) return;

						// IMPROVED BEDROOMS EXTRACTION
						let bedrooms = null;

						// Try to find number right before/after "bed" keywords
						const bedPatterns = [
							/(\d+)\s*(?:bed|bedroom|bedrooms|bed(s)?)/i,
							/(?:bed|bedroom|bedrooms|bed(s)?)\s*(\d+)/i,           // number after word
							/(\d+)\s*bed/i                                         // shorter variants
						];

						for (const pattern of bedPatterns) {
							const match = cardText.match(pattern);
							if (match) {
								const num = match[1] || match[2];
								const parsed = parseInt(num, 10);
								if (parsed >= 0 && parsed <= 20) {  // reasonable range for bedrooms
									bedrooms = parsed.toString();     // keep as string for DB consistency
									break;
								}
							}
						}

						// Skip sold/let agreed
						if (/sold|stc|let\s*agreed|under offer|reserved/i.test(cardText)) return;

						items.push({ link, title, price, bedrooms });
					} catch { }
				});

				// Debug: show sample of first card
				if (cardElements.length > 0) {
					const sample = cardElements[0].innerText.substring(0, 400).replace(/\n/g, ' ');
					console.log(`Sample card text (first): ${sample}...`);
				}

				return items;
			});

			logger.step(`Extracted ${properties.length} valid properties`);

			// Process in batches
			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(batch.map(async (property) => {
					if (processedUrls.has(property.link)) return;
					processedUrls.add(property.link);

					try {
						let actionTaken = "UNCHANGED";
						const priceNum = parseInt(property.price.replace(/[^0-9]/g, ''), 10);

						if (isNaN(priceNum)) {
							logger.warn(`Invalid price → ${property.link}`);
							return;
						}

						const result = await updatePriceByPropertyURLOptimized(
							property.link.trim(),
							priceNum,
							property.title,
							property.bedrooms || null,
							AGENT_ID,
							isRental
						);

						if (result.updated) {
							actionTaken = "UPDATED";
							stats.totalSaved++;
						}

						if (!result.isExisting && !result.error) {
							await scrapePropertyDetail(page.context(), { ...property, price: priceNum }, isRental);
							actionTaken = "CREATED";
						}

						logger.property(
							null,
							label,
							property.title,
							formatPriceUk(priceNum),
							property.link,
							isRental,
							null,
							actionTaken
						);

						if (actionTaken === "CREATED") await sleep(1800 + Math.random() * 1200);
					} catch (err) {
						logger.error(`Failed → ${property.link}`, err.message || err);
					}
				}));

				await sleep(1000 + Math.random() * 800);
			}
		},

		failedRequestHandler({ request }) {
			logger.error(`Failed request: ${request.url}`);
		},
	});

	const initialRequests = PROPERTY_TYPES.map(t => ({
		url: t.url,
		userData: { isRental: t.isRental, label: t.label },
	}));

	await crawler.run(initialRequests);

	logger.step(`Completed - Scraped: ${stats.totalScraped} | Saved: ${stats.totalSaved}`);
	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
	try {
		await scrapeAshtons();
		logger.step("Done");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error:", err?.message || err);
		process.exit(1);
	}
})();