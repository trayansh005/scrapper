// Harcourts Australia Scraper
// Agent ID: 280
// Created: 22 July 2026

'use strict';

const { CheerioCrawler, log } = require('crawlee');
const { updateRemoveStatus } = require('./db.js');
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require('./lib/db-helpers.js');
const { parsePrice, formatPriceUk, extractCoordinatesFromHTML } = require('./lib/property-helpers.js');
const { createAgentLogger } = require('./lib/logger-helpers.js');

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 280;
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
	const type = isRental ? 'rent' : 'buy';
	return `https://harcourts.net/au/listings/${type}?include-suburb=1&category=residential&listing-category=residential&showSurroundMsg=1&page=${pageNum}`;
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

function formatHarcourtsPrice(priceRaw) {
	if (!priceRaw || typeof priceRaw !== 'string') return 'POA';
	const clean = priceRaw.trim();

	if (/contact|poa|auction|offers presented|by negotiation|express/i.test(clean) && !/\$\s*[\d,]+/.test(clean)) {
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

async function fetchDetailHTML(url) {
	try {
		const res = await fetch(url, {
			headers: {
				'User-Agent':
					'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
				Accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
				'Accept-Language': 'en-AU,en-US;q=0.9,en;q=0.8',
			},
		});
		if (res.ok) {
			return await res.text();
		}
	} catch (err) {
		logger.error(`Failed to fetch detail page ${url}: ${err.message}`);
	}
	return '';
}

async function handleListingPage({ $, request, crawler }) {
	const { pageNum, label, isRental } = request.userData;

	logger.page(pageNum, label, `Processing ${request.url}`);

	let totalPages = request.userData.totalPages || 1;

	// Extract total results or pagination on page 1
	if (pageNum === startPage) {
		const resultText = $('.property-search-result-found, [class*="result-found"]').text().trim();
		const countMatch = resultText.match(/([\d,]+)\s+properties/i);
		if (countMatch) {
			const totalCount = parseInt(countMatch[1].replace(/,/g, ''), 10);
			totalPages = Math.ceil(totalCount / 12);
			logger.page(pageNum, label, `Found total ${totalCount} properties across ~${totalPages} pages.`);
		}
	}

	// Try extracting from embedded mapItemSearchResultsJSON script
	let jsonListings = null;
	$('script').each((i, el) => {
		const text = $(el).html() || '';
		if (text.includes('mapItemSearchResultsJSON')) {
			const match = text.match(/var\s+mapItemSearchResultsJSON\s*=\s*(\[[\s\S]*?\]);/);
			if (match) {
				try {
					jsonListings = JSON.parse(match[1]);
				} catch (e) {}
			}
		}
	});

	const itemsToProcess = [];

	if (Array.isArray(jsonListings) && jsonListings.length > 0) {
		for (const item of jsonListings) {
			if (!item.url) continue;
			const link = item.url.startsWith('http') ? item.url : `https://harcourts.net${item.url}`;
			const address = item.address ? item.address.trim() : '';
			const title = item.title ? item.title.trim() : '';
			const fullTitle = title && address && title !== address ? `${title}, ${address}` : (address || title || 'Property');
			const price = formatHarcourtsPrice(item.price);
			const bedrooms = item.bedrooms ? parseInt(item.bedrooms, 10) : null;
			let lat = item.lat ? parseFloat(item.lat) : null;
			let lng = item.lng ? parseFloat(item.lng) : null;
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
	} else {
		// Fallback to DOM parsing
		$('.property-item.card').each((i, card) => {
			const relLink = $(card).find('a.card-link-url, a[href]').attr('href');
			if (!relLink) return;
			const link = relLink.startsWith('http') ? relLink : `https://harcourts.net${relLink}`;
			const title = $(card).find('.property-title').text().trim();
			const address = $(card).find('.address').text().trim();
			const fullTitle = title && address ? `${title}, ${address}` : (address || title || 'Property');
			const priceText = $(card).find('.price.hi-price, .price, .card-text').text().trim();
			const price = formatHarcourtsPrice(priceText);
			const bedsText = $(card).find('.summary li:first-child span').text().trim();
			const bedrooms = bedsText ? parseInt(bedsText, 10) : null;

			itemsToProcess.push({
				link,
				title: fullTitle,
				price,
				bedrooms,
				lat: null,
				lng: null,
			});
		});
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
						logger.property(pageNum, label, prop.title, prop.price, prop.link, isRental, null, 'UPDATED');
						counts.totalSaved++;
						if (isRental) counts.savedRentals++;
						else counts.savedSales++;
					} else {
						logger.property(pageNum, label, prop.title, prop.price, prop.link, isRental, null, 'UNCHANGED');
					}
				} else {
					let lat = prop.lat;
					let lng = prop.lng;
					let htmlForCoords = null;

					if (lat === null || lng === null) {
						htmlForCoords = await fetchDetailHTML(prop.link);
					}

					const coordsResult = await processPropertyWithCoordinates(
						prop.link,
						prop.price,
						prop.title,
						prop.bedrooms,
						AGENT_ID,
						isRental,
						htmlForCoords,
						lat,
						lng,
						CURRENCY,
					);

					const finalLat = coordsResult.latitude || lat;
					const finalLng = coordsResult.longitude || lng;

					logger.property(
						pageNum,
						label,
						prop.title,
						prop.price,
						prop.link,
						isRental,
						null,
						'CREATED',
						finalLat,
						finalLng,
					);

					counts.totalSaved++;
					if (isRental) counts.savedRentals++;
					else counts.savedSales++;

					await sleep(200); // Politeness delay ONLY on CREATED
				}
		} catch (err) {
			logger.error(`Error processing property ${prop.link}: ${err.message}`);
		}
	}

	// Enqueue next page if more items exist
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
	logger.step(`Starting Harcourts Australia scraper (Agent ${AGENT_ID}) from page ${startPage}`);

	const crawler = new CheerioCrawler({
		maxConcurrency: 3,
		requestHandlerTimeoutSecs: 60,
		navigationTimeoutSecs: 30,
		maxRequestRetries: 3,
		additionalMimeTypes: ['text/html', 'application/xhtml+xml'],
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
