// Belle Property Scraper
// Agent ID: 281
// Created: 22 July 2026

'use strict';

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env'), override: true });

const { PlaywrightCrawler, log } = require('crawlee');
const { updateRemoveStatus } = require('./db.js');
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require('./lib/db-helpers.js');
const { parsePrice, formatPriceUk } = require('./lib/property-helpers.js');
const { createAgentLogger } = require('./lib/logger-helpers.js');

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 281;
const CURRENCY = '$';
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
	totalSkipped: 0,
};

const scrapeStartTime = new Date();
const processedUrls = new Set();

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const val = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(val) || val < 1) return 1;
	return Math.floor(val);
}

const startPage = getStartPage();

function getPageUrl(isRental, pageNum) {
	const status = isRental ? 'rent' : 'buy';
	return `https://www.belleproperty.com/listings?propertyType=residential&sort=newold&searchStatus=${status}&surr=1&state=all&page=${pageNum}`;
}

const PROPERTY_TYPES = [
	{
		label: 'SALES',
		isRental: false,
		baseUrl: getPageUrl(false, startPage),
	},
	{
		label: 'RENTALS',
		isRental: true,
		baseUrl: getPageUrl(true, startPage),
	},
];

function formatBellePrice(priceRaw) {
	if (!priceRaw || typeof priceRaw !== 'string') return 'POA';
	const clean = priceRaw.trim();

	if (
		/contact|poa|auction|offers presented|by negotiation|express|for sale|under offer/i.test(clean) &&
		!/\$\s*[\d,]+/.test(clean)
	) {
		return 'POA';
	}

	const dollarMatch = clean.match(/\$\s*([\d,]+)/);
	if (dollarMatch) {
		const rawNum = dollarMatch[1].replace(/,/g, '');
		const num = parseInt(rawNum, 10);
		if (!isNaN(num) && num > 0) {
			return formatPriceUk(num) || 'POA';
		}
	}

	const parsed = parsePrice(clean);
	if (parsed === null || isNaN(parsed) || parsed < 50) return 'POA';
	return formatPriceUk(parsed) || 'POA';
}

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, label, isRental } = request.userData;

	logger.page(pageNum, label, `Processing ${request.url}`);

	await page.waitForTimeout(3000);

	let totalPages = request.userData.totalPages || 1;

	// Extract Next.js React Query state
	const nextData = await page.evaluate(() => {
		const el = document.getElementById('__NEXT_DATA__');
		return el ? JSON.parse(el.innerHTML) : null;
	});

	const itemsToProcess = [];

	if (nextData) {
		const dehydratedState = nextData.props?.pageProps?.dehydratedState;
		let data = null;
		if (Array.isArray(dehydratedState?.queries)) {
			for (const q of dehydratedState.queries) {
				if (q.state?.data && Array.isArray(q.state.data.results)) {
					data = q.state.data;
					break;
				}
			}
		}

		if (data) {
			if (data.pages && pageNum === startPage) {
				totalPages = data.pages;
				logger.page(
					pageNum,
					label,
					`Found total ${data.total} properties across ~${totalPages} pages.`,
				);
			}

			if (Array.isArray(data.results)) {
				for (const item of data.results) {
					if (!item.slug && !item.id) continue;
					const link = `https://www.belleproperty.com/listings/${item.slug || item.id}`;
					const address = item.address?.displayAddress || '';
					const heading = item.heading || '';
					const fullTitle =
						heading && address && heading !== address
							? `${heading}, ${address}`
							: address || heading || 'Property';

					const price = formatBellePrice(item.priceDisplay);
					const bedrooms = item.bedrooms ? parseInt(item.bedrooms, 10) : null;
					let lat = item.address?.location?.lat ? parseFloat(item.address.location.lat) : null;
					let lng = item.address?.location?.lon ? parseFloat(item.address.location.lon) : null;
					if (lat === 0 && lng === 0) {
						lat = null;
						lng = null;
					}

					itemsToProcess.push({
						link,
						title: fullTitle,
						price,
						bedrooms,
						lat,
						lng,
					});
				}
			}
		}
	}

	// Fallback to DOM card parsing if Next.js data was not found
	if (itemsToProcess.length === 0) {
		const domCards = await page.evaluate(() => {
			const cards = Array.from(
				document.querySelectorAll('article, [class*="ListingCard"], [class*="property-card"]'),
			);
			return cards.map((c) => {
				const linkEl = c.querySelector('a[href]');
				const href = linkEl ? linkEl.getAttribute('href') : '';
				const text = c.innerText.replace(/\s+/g, ' ').trim();
				return { href, text };
			});
		});

		for (const card of domCards) {
			if (!card.href) continue;
			const link = card.href.startsWith('http')
				? card.href
				: `https://www.belleproperty.com${card.href}`;

			const price = formatBellePrice(card.text);
			const bedMatch = card.text.match(/(\d+)\s*(?:bed|br)/i);
			const bedrooms = bedMatch ? parseInt(bedMatch[1], 10) : null;

			itemsToProcess.push({
				link,
				title: card.text.substring(0, 120),
				price,
				bedrooms,
				lat: null,
				lng: null,
			});
		}
	}

	if (itemsToProcess.length === 0) {
		logger.page(pageNum, label, `No properties found on page ${pageNum}. Ending pagination.`);
		return;
	}

	logger.page(pageNum, label, `Found ${itemsToProcess.length} properties on page ${pageNum}`);

	for (const prop of itemsToProcess) {
		if (processedUrls.has(prop.link)) {
			counts.totalSkipped++;
			continue;
		}
		processedUrls.add(prop.link);
		counts.totalScraped++;

		try {
			const result = await updatePriceByPropertyURLOptimized(
				prop.link,
				prop.price,
				prop.title,
				prop.bedrooms,
				AGENT_ID,
				isRental,
				CURRENCY,
			);

			if (result.isExisting && !result.missingData) {
				if (result.updated) {
					logger.property(prop.link, prop.price, prop.bedrooms, 'UPDATED', isRental);
					counts.totalSaved++;
					if (isRental) counts.savedRentals++;
					else counts.savedSales++;
				} else {
					logger.property(prop.link, prop.price, prop.bedrooms, 'UNCHANGED', isRental);
				}
			} else {
				let lat = prop.lat;
				let lng = prop.lng;
				let detailHtml = null;

				if (lat === null || lng === null) {
					try {
						const detailPage = await page.context().newPage();
						await detailPage.goto(prop.link, { waitUntil: 'domcontentloaded', timeout: 30000 });
						await detailPage.waitForTimeout(1000);
						detailHtml = await detailPage.content();
						await detailPage.close();
					} catch (e) {}
				}

				const coordsResult = await processPropertyWithCoordinates(
					prop.link,
					prop.price,
					prop.title,
					prop.bedrooms,
					AGENT_ID,
					isRental,
					detailHtml,
					lat,
					lng,
					CURRENCY,
				);

				logger.property(prop.link, prop.price, prop.bedrooms, 'CREATED', isRental, {
					latitude: coordsResult.latitude || lat,
					longitude: coordsResult.longitude || lng,
				});

				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;

				await sleep(200); // Politeness delay ONLY on CREATED
			}
		} catch (err) {
			logger.error(`Error processing property ${prop.link}: ${err.message}`);
		}
	}

	// Enqueue next page
	const nextPageNum = pageNum + 1;
	if (nextPageNum <= totalPages && itemsToProcess.length > 0) {
		const nextUrl = getPageUrl(isRental, nextPageNum);
		await crawler.addRequests([
			{
				url: nextUrl,
				userData: {
					pageNum: nextPageNum,
					totalPages,
					isRental,
					label,
				},
			},
		]);
	}
}

async function run() {
	logger.step(`Starting Belle Property scraper (Agent ${AGENT_ID}) from page ${startPage}`);

	const crawler = new PlaywrightCrawler({
		launchContext: {
			launchOptions: {
				headless: true,
				args: [
					'--no-sandbox',
					'--disable-setuid-sandbox',
					'--disable-blink-features=AutomationControlled',
					'--disable-infobars',
					'--window-size=1920,1080',
				],
			},
		},
		browserPoolOptions: {
			useFingerprints: true,
		},
		sessionPoolOptions: {
			blockedStatusCodes: [], // Do not block initial 403 response before page JS runs
		},
		preNavigationHooks: [
			async ({ page }) => {
				await page.addInitScript(() => {
					Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
					window.navigator.chrome = { runtime: {} };
					Object.defineProperty(navigator, 'languages', { get: () => ['en-AU', 'en-US', 'en'] });
					Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
				});
			},
		],
		maxConcurrency: 1, // Single concurrency to safely bypass Cloudflare
		requestHandlerTimeoutSecs: 90,
		navigationTimeoutSecs: 45,
		maxRequestRetries: 3,
		requestHandler: handleListingPage,
		failedRequestHandler: ({ request }, error) => {
			logger.error(`Request failed for ${request.url}: ${error.message}`);
		},
	});

	const initialRequests = PROPERTY_TYPES.map((type) => ({
		url: type.baseUrl,
		userData: {
			pageNum: startPage,
			isRental: type.isRental,
			label: type.label,
		},
	}));

	await crawler.run(initialRequests);

	if (startPage === 1) {
		logger.step(`Cleaning up removed properties for Agent ${AGENT_ID}`);
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.step(`Partial run from page ${startPage} - skipping updateRemoveStatus for safety.`);
	}

	logger.step(`
==================================================
🏁 SCRAPER COMPLETED (Agent ${AGENT_ID})
--------------------------------------------------
Total Scraped: ${counts.totalScraped}
Total Saved:   ${counts.totalSaved}
Saved Sales:   ${counts.savedSales}
Saved Rentals: ${counts.savedRentals}
Skipped:       ${counts.totalSkipped}
==================================================
	`);
}

run().catch((err) => {
	logger.error(`Fatal error in Agent ${AGENT_ID}: ${err.stack || err.message}`);
	process.exit(1);
});
