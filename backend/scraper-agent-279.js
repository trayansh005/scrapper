// RayWhite Scraper
// Agent ID: 279
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

const AGENT_ID = 279;
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
	const baseUrl = isRental
		? 'https://www.raywhite.com/listing?address=Sydney%2C+NSW+2000&location=130d0d1006080907120f0b0f100c0f120a0b0a12706d69120c0e0e0e126d475a505b47&type=rent&category=ACR%2COTH%2CHSE%2CRTM'
		: 'https://www.raywhite.com/listing?address=Sydney%2C+NSW+2000&location=130d0d1006080907120f0b0f100c0f120a0b0a12706d69120c0e0e0e126d475a505b47&type=buy&category=ACR%2COTH%2CHSE%2CRTM%2CAPT';

	return pageNum > 1 ? `${baseUrl}&page=${pageNum}` : baseUrl;
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

function formatRayWhitePrice(priceRaw) {
	if (!priceRaw || typeof priceRaw !== 'string') return 'POA';
	const clean = priceRaw.trim();

	// Check million format e.g. $9 million or $1.5 million
	const millionMatch = clean.match(/\$\s*([\d.]+)\s*million/i);
	if (millionMatch) {
		const num = Math.round(parseFloat(millionMatch[1]) * 1000000);
		return formatPriceUk(num) || 'POA';
	}

	// Standard dollar match e.g. $595,000 or $1,400 per week
	const dollarMatch = clean.match(/\$\s*([\d,]+)/);
	if (dollarMatch) {
		const rawNum = dollarMatch[1].replace(/,/g, '');
		const num = parseInt(rawNum, 10);
		if (!isNaN(num) && num > 0) {
			return formatPriceUk(num) || 'POA';
		}
	}

	if (
		/contact|poa|auction|offers|by negotiation|express|inspection/i.test(clean) &&
		!/\$\s*[\d,]+/.test(clean)
	) {
		return 'POA';
	}

	const parsed = parsePrice(clean);
	if (parsed === null || isNaN(parsed) || parsed < 50) return 'POA';
	return formatPriceUk(parsed) || 'POA';
}

function parseRayWhiteBedrooms(cardText) {
	if (!cardText || typeof cardText !== 'string') return null;

	if (/\bSTUDIO\b/i.test(cardText)) {
		return 1;
	}

	const explicitBed = cardText.match(/(\d+)\s*(?:bed|br|brm|bedroom)/i);
	if (explicitBed) {
		return parseInt(explicitBed[1], 10);
	}

	const patternMatch = cardText.match(
		/(?:STUDIO|APARTMENT|HOUSE|TOWNHOUSE|VILLA|UNIT|LAND|OTHER|COMMERCIAL)(?:\s+\d+\s*m2)?\s+(\d+)/i,
	);
	if (patternMatch) {
		const beds = parseInt(patternMatch[1], 10);
		if (beds > 0 && beds < 30) return beds;
	}

	return null;
}

function parseRayWhiteTitle(cardText, headingText = '') {
	if (headingText && !/^\$\s*[\d,]+/.test(headingText) && !/buyer|price|guide|inspection/i.test(headingText)) {
		const cleanHeading = headingText.replace(/\s*Australia\b/gi, '').trim();
		if (cleanHeading.length > 5) return cleanHeading.substring(0, 150);
	}

	if (!cardText || typeof cardText !== 'string') return 'Property';

	let clean = cardText.replace(/\s*Australia\b/gi, '').trim();

	const addrMatch =
		clean.match(/(\d+[\w\s/-]+,\s*[\w\s]+,\s*[\w\s]+\s*\d{4})/i) ||
		clean.match(/([\w\s/-]+\d+[\w\s/-]+,\s*[\w\s]+,\s*[\w\s]+\s*\d{4})/i) ||
		clean.match(/([\w\s/-]+,\s*[\w\s]+,\s*(?:New South Wales|NSW|VIC|QLD|SA|WA|TAS|ACT)\s*\d{4})/i);

	if (addrMatch) {
		return addrMatch[1].replace(/\s+/g, ' ').trim().substring(0, 150);
	}

	let fallback = clean
		.replace(
			/^(?:STUDIO|APARTMENT|HOUSE|TOWNHOUSE|VILLA|UNIT|LAND|OTHER|COMMERCIAL)\s*(?:\d+\s*m2)?\s*(?:\d+\s+){1,3}/i,
			'',
		)
		.replace(
			/(?:\$\s*[\d,.]+(?:\s*million)?|\$\s*[\d,]+\s*per\s*week|buyer\s*guide:[^\s]+|price\s*guide[^\s]*|inspection[^\s]*)/gi,
			'',
		)
		.replace(/\s+/g, ' ')
		.trim();

	return fallback ? fallback.substring(0, 150) : 'Property';
}

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, label, isRental } = request.userData;

	logger.page(pageNum, label, `Processing ${request.url}`);

	await page.waitForTimeout(3500);

	const itemsToProcess = await page.evaluate(() => {
		const links = Array.from(document.querySelectorAll('a[href]')).filter((a) => {
			const href = a.getAttribute('href');
			return href && /\/(?:nsw|qld|vic|sa|wa|act|nt|tas)\/[a-z0-9-]+\/\d+/i.test(href);
		});

		const seenHrefs = new Set();
		const results = [];

		for (const a of links) {
			const href = a.getAttribute('href');
			if (seenHrefs.has(href)) continue;
			seenHrefs.add(href);

			let container =
				a.closest("article, [class*='card'], [class*='Card'], [class*='listing']") || a.parentElement;
			while (
				container &&
				container.innerText &&
				container.innerText.length < 50 &&
				container.parentElement
			) {
				container = container.parentElement;
			}

			const fullText = container ? container.innerText.replace(/\s+/g, ' ').trim() : '';

			const headingEl = container
				? container.querySelector("h1, h2, h3, h4, h5, [class*='address'], [class*='title']")
				: null;
			const headingText = headingEl ? headingEl.innerText.trim() : '';

			results.push({
				href,
				fullText,
				headingText,
			});
		}

		return results;
	});

	if (itemsToProcess.length === 0) {
		logger.page(pageNum, label, `No properties found on page ${pageNum}. Ending pagination.`);
		return;
	}

	logger.page(pageNum, label, `Found ${itemsToProcess.length} properties on page ${pageNum}`);

	for (const rawProp of itemsToProcess) {
		const link = rawProp.href.startsWith('http')
			? rawProp.href
			: `https://www.raywhite.com${rawProp.href}`;

		if (processedUrls.has(link)) {
			counts.totalSkipped++;
			continue;
		}
		processedUrls.add(link);
		counts.totalScraped++;

		const price = formatRayWhitePrice(rawProp.fullText);
		const bedrooms = parseRayWhiteBedrooms(rawProp.fullText);
		const title = parseRayWhiteTitle(rawProp.fullText, rawProp.headingText);

		try {
			const result = await updatePriceByPropertyURLOptimized(
				link,
				price,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
				CURRENCY,
			);

			if (result.isExisting && !result.missingData) {
				if (result.updated) {
					logger.property(link, price, bedrooms, 'UPDATED', isRental);
					counts.totalSaved++;
					if (isRental) counts.savedRentals++;
					else counts.savedSales++;
				} else {
					logger.property(link, price, bedrooms, 'UNCHANGED', isRental);
				}
			} else {
				let lat = null;
				let lng = null;
				let detailHtml = null;

				try {
					const detailPage = await page.context().newPage();
					await detailPage.goto(link, { waitUntil: 'domcontentloaded', timeout: 30000 });
					await detailPage.waitForTimeout(1000);
					detailHtml = await detailPage.content();
					await detailPage.close();
				} catch (e) {}

				const coordsResult = await processPropertyWithCoordinates(
					link,
					price,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					detailHtml,
					lat,
					lng,
					CURRENCY,
				);

				logger.property(link, price, bedrooms, 'CREATED', isRental, {
					latitude: coordsResult.latitude,
					longitude: coordsResult.longitude,
				});

				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;

				await sleep(200); // Politeness delay ONLY on CREATED
			}
		} catch (err) {
			logger.error(`Error processing property ${link}: ${err.message}`);
		}
	}

	// Pagination detection: check if properties exist and try next page
	const nextPageNum = pageNum + 1;
	const maxPages = isRental ? 60 : 30; // Safety threshold

	if (nextPageNum <= maxPages && itemsToProcess.length > 0) {
		const nextUrl = getPageUrl(isRental, nextPageNum);
		await crawler.addRequests([
			{
				url: nextUrl,
				userData: {
					pageNum: nextPageNum,
					isRental,
					label,
				},
			},
		]);
	}
}

async function run() {
	logger.step(`Starting RayWhite scraper (Agent ${AGENT_ID}) from page ${startPage}`);

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
			blockedStatusCodes: [],
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
		maxConcurrency: 1,
		requestHandlerTimeoutSecs: 120,
		navigationTimeoutSecs: 60,
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
