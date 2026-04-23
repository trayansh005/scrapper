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
			// === IMPROVED INFINITE SCROLL + CLICK "SHOW MORE" FOR ASHTONS ===
			logger.step("Loading all properties with Scroll + Show More...");

			let prevPropertyCount = 0;
			let attempts = 0;
			const maxAttempts = 40;

			while (attempts < maxAttempts) {
				// Scroll to bottom
				await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
				await sleep(3000 + Math.random() * 1500);

				// Try to click "Show More" button if present
				const showMoreClicked = await page.evaluate(() => {
					const buttons = Array.from(document.querySelectorAll('button, a'));
					const showMoreBtn = buttons.find(btn => {
						const text = (btn.textContent || '').toLowerCase().trim();
						return text.includes('show more') ||
							text.includes('load more') ||
							text.includes('more properties') ||
							btn.id?.includes('more') ||
							btn.className?.includes('more');
					});

					if (showMoreBtn) {
						showMoreBtn.scrollIntoView({ behavior: 'smooth' });
						showMoreBtn.click();
						return true;
					}
					return false;
				});

				if (showMoreClicked) {
					logger.step("Clicked 'Show More' button");
					await sleep(4000 + Math.random() * 2000);
				}

				// Count current property cards
				const currentCount = await page.$$eval('.c-property-card, .o-card.c-property-card', els => els.length);

				logger.step(`Properties loaded so far: ${currentCount}`);

				if (currentCount === prevPropertyCount && !showMoreClicked) {
					logger.step(`No more properties to load (Total: ${currentCount})`);
					break;
				}

				prevPropertyCount = currentCount;
				attempts++;

				if (attempts % 5 === 0) {
					logger.step(`Loading progress: ${currentCount} properties (${attempts}/${maxAttempts})`);
				}
			}

			// Final wait
			await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
			await sleep(3000);

			// Debug: screenshot + broad card count
			const safeLabel = label.toLowerCase().replace(/\s+/g, '-');
			const shotPath = `ashtons-${safeLabel}-final.png`;
			await page.screenshot({ path: shotPath, fullPage: true });
			logger.step(`Screenshot: ${shotPath}`);

			const potentialCards = await page.$$eval(
				'.c-property-card, .o-card.c-property-card',
				els => els.length
			);
			logger.step(`Detected ${potentialCards} potential cards`);

			// Extract – loose text-based fallback
						// Extract properties
			const properties = await page.evaluate(() => {
				const items = [];
				const cardElements = document.querySelectorAll('.c-property-card, .o-card.c-property-card');

				console.log(`Total cards found in DOM: ${cardElements.length}`);

				Array.from(cardElements).forEach((card, index) => {   // Removed .slice(0, 300)
					try {
						// Link
						const linkEl = card.querySelector('a[href*="/property/"], a.c-property-card__anchor');
						if (!linkEl) return;
						let href = linkEl.getAttribute('href');
						if (!href) return;
						const link = href.startsWith('http') ? href : `https://www.ashtons.co.uk${href.startsWith('/') ? '' : '/'}${href}`;

						const cardText = card.innerText.trim().replace(/\s+/g, ' ');

						// Price
						const priceMatch = cardText.match(/£[\d,]+/);
						let price = priceMatch ? priceMatch[0].replace(/[£,]/g, '').trim() : null;
						if (!price) return;

						// Title
						let title = '';
						const heading = card.querySelector('h1, h2, h3, h4, h5, .c-property-card__title, strong, .address');
						if (heading) {
							title = heading.innerText.trim();
						} else {
							title = cardText.split(/[\n•]/)[0]?.trim() || `Property ${index}`;
						}
						if (!title || title.length < 5) return;

						// === FIXED BEDROOMS EXTRACTION ===
						let bedrooms = null;

						const bedroomDl = card.querySelector('dl.c-property-feature--bedrooms');
						if (bedroomDl) {
							const valueEl = bedroomDl.querySelector('dd.c-property-feature__value');
							if (valueEl) {
								const text = valueEl.textContent.trim();
								const match = text.match(/(\d+)/);
								if (match) {
									const num = parseInt(match[1], 10);
									if (num >= 1 && num <= 20) {
										bedrooms = num;
									}
								}
							}
						}

						// Fallback
						if (!bedrooms) {
							const bedMatch = cardText.match(/(\d+)\s*(?:bed|bedroom|bedrooms)/i);
							if (bedMatch) {
								const num = parseInt(bedMatch[1], 10);
								if (num >= 1 && num <= 20) bedrooms = num;
							}
						}

						// Skip sold / let agreed
						if (/sold|stc|let\s*agreed|under offer|reserved/i.test(cardText)) return;

						items.push({ link, title, price, bedrooms });
					} catch (e) {
						// silent fail per card
					}
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