// RayWhite API Scraper
// Agent ID: 279
// Optimized with Pre-Fetched Existing Records & Parallel Batch DB Updates/Inserts.

'use strict';

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env'), override: true });

const axios = require('axios');
const { promisePool, updateRemoveStatus } = require('./db.js');
const { parsePrice, formatPriceUk } = require('./lib/property-helpers.js');
const { createAgentLogger } = require('./lib/logger-helpers.js');

const AGENT_ID = 279;
const CURRENCY = '$';
const API_URL =
	'https://raywhiteapi.ep.dynamics.net/v1/listings?apiKey=6625c417-067a-4a8e-8c1d-85c812d0fb25';
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

function formatRayWhitePrice(priceRaw, fallbackNumeric = null) {
	if (priceRaw && typeof priceRaw === 'string') {
		const clean = priceRaw.trim();

		const millionMatch = clean.match(/\$\s*([\d.]+)\s*million/i);
		if (millionMatch) {
			const num = Math.round(parseFloat(millionMatch[1]) * 1000000);
			return formatPriceUk(num) || 'POA';
		}

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
		if (parsed !== null && !isNaN(parsed) && parsed >= 50) {
			return formatPriceUk(parsed) || 'POA';
		}
	}

	if (fallbackNumeric !== null && fallbackNumeric !== undefined && !isNaN(fallbackNumeric)) {
		const num = parseFloat(fallbackNumeric);
		if (num >= 50) return formatPriceUk(num) || 'POA';
	}

	return 'POA';
}

function buildTitle(addr) {
	if (!addr) return 'Property';
	if (addr.formatted) {
		const parts = addr.formatted
			.split('\n')
			.map((p) => p.trim())
			.filter((p) => p && p !== 'Australia');
		if (parts.length > 0) {
			return parts.join(', ').replace(/\s+/g, ' ').substring(0, 150);
		}
	}

	const unit = addr.unitNumber ? `${addr.unitNumber}/` : '';
	const number = addr.streetNumber || '';
	const name = addr.streetName || '';
	const type = addr.streetType || '';
	const suburb = addr.suburb || '';
	const state = addr.stateCode || addr.state || '';
	const postcode = addr.postCode || '';

	const streetPart = `${unit}${number} ${name} ${type}`.trim();
	const locationPart = `${suburb} ${state} ${postcode}`.trim();

	const title = [streetPart, locationPart].filter(Boolean).join(', ').trim();
	return title || 'Property';
}

async function fetchApiBatch(isRental, fromOffset = 0, batchSize = 50) {
	const typeCodes = isRental ? ['REN', 'LSD', 'RNR'] : ['RUR', 'SAL'];

	const payload = {
		size: batchSize,
		from: fromOffset,
		sort: [{ field: 'location', lat: -33.8679, lon: 151.21, order: 'asc' }],
		location: { lat: -33.8679, lon: 151.21 },
		stateCode: 'NSW',
		categoryCode: {
			in: [
				'ACR',
				'OTH',
				'HSE',
				'RTM',
				'APT',
				'SDC',
				'TCE',
				'ASR',
				'THS',
				'VIL',
				'BOU',
				'SAP',
				'STD',
				'FLT',
				'UNT',
				'ASR',
				'LAN',
			],
		},
		countryCode: ['AU', 'NZ'],
		typeCode: { in: typeCodes },
		statusCode: { in: ['CUR'] },
	};

	const response = await axios.post(API_URL, payload, {
		headers: {
			'Content-Type': 'application/json',
			'User-Agent':
				'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
			Origin: 'https://www.raywhite.com',
			Referer: 'https://www.raywhite.com/',
		},
		timeout: 30000,
	});

	return response.data?.data || [];
}

async function scrapeCategory(label, isRental) {
	logger.step(`Processing ${label} listings via REST API...`);

	const tableName = isRental ? 'property_for_rent' : 'property_for_sale';
	const existingMap = new Map();

	try {
		const [rows] = await promisePool.query(
			`SELECT property_url, price FROM ${tableName} WHERE agent_id = ?`,
			[AGENT_ID],
		);
		for (const r of rows) {
			if (r.property_url) existingMap.set(r.property_url.trim(), r.price);
		}
		logger.step(`Pre-fetched ${existingMap.size} existing ${label} records for Agent ${AGENT_ID}`);
	} catch (e) {
		logger.error(`Pre-fetch error for ${tableName}: ${e.message}`);
	}

	let fromOffset = 0;
	const batchSize = 50;
	let pageNum = 1;
	let hasMore = true;

	while (hasMore) {
		let items = [];
		try {
			items = await fetchApiBatch(isRental, fromOffset, batchSize);
		} catch (err) {
			logger.error(`API Fetch failed at offset ${fromOffset} for ${label}: ${err.message}`);
			break;
		}

		if (!items || items.length === 0) {
			logger.page(pageNum, label, `No more listings found at offset ${fromOffset}. Ending ${label}.`);
			hasMore = false;
			break;
		}

		logger.page(pageNum, label, `Received ${items.length} listings from API (offset ${fromOffset})`);

		// Process batch concurrently in parallel
		await Promise.all(
			items.map(async (itemObj) => {
				const item = itemObj.value || itemObj;
				if (!item || !item.id) return;

				const suburbSlug = item.address?.suburb ? item.address.suburb.toLowerCase().replace(/\s+/g, '-') : 'property';
				const stateSlug = item.address?.stateCode ? item.address.stateCode.toLowerCase() : 'nsw';
				const link = item.url || `https://www.raywhite.com/${stateSlug}/${suburbSlug}/${item.id}`;

				if (processedUrls.has(link)) {
					counts.totalSkipped++;
					return;
				}
				processedUrls.add(link);
				counts.totalScraped++;

				const title = buildTitle(item.address);
				const price = formatRayWhitePrice(item.displayPrice, item.price);
				const bedrooms = item.bedrooms !== undefined && item.bedrooms !== null ? parseInt(item.bedrooms, 10) : null;
				const lat = item.address?.location?.lat ? parseFloat(item.address.location.lat) : null;
				const lng = item.address?.location?.lon ? parseFloat(item.address.location.lon) : null;

				const isExisting = existingMap.has(link);
				const oldPrice = isExisting ? existingMap.get(link) : null;

				try {
					if (isExisting) {
						const priceChanged = oldPrice !== price;

						await promisePool.query(
							`UPDATE ${tableName}
							SET property_name = ?, price = ?, currency = ?, bedrooms = ?, latitude = ?, longitude = ?, remove_status = 0, updated_at = NOW()
							WHERE property_url = ? AND agent_id = ?`,
							[title, price, CURRENCY, bedrooms, lat, lng, link, AGENT_ID],
						);

						if (priceChanged) {
							logger.property(pageNum, label, title, price, link, isRental, null, 'UPDATED', lat, lng);
							counts.totalSaved++;
							if (isRental) counts.savedRentals++;
							else counts.savedSales++;
						} else {
							logger.property(pageNum, label, title, price, link, isRental, null, 'UNCHANGED', lat, lng);
						}
					} else {
						const insertQuery = `INSERT INTO ${tableName} (
							property_id, agent_id, property_name, price, currency, bedrooms, property_url, logo, latitude, longitude, remove_status, created_at, updated_at
						) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NOW(), NOW())`;

						const propId = String(item.id);
						const logo = 'property_for_sale/logo.png';

						await promisePool.query(insertQuery, [
							propId,
							AGENT_ID,
							title,
							price,
							CURRENCY,
							bedrooms,
							link,
							logo,
							lat,
							lng,
						]);

						logger.property(
							pageNum,
							label,
							title,
							price,
							link,
							isRental,
							null,
							'CREATED',
							lat,
							lng,
						);

						counts.totalSaved++;
						if (isRental) counts.savedRentals++;
						else counts.savedSales++;

						existingMap.set(link, price);
					}
				} catch (err) {
					logger.error(`Error saving property ${link}: ${err.message}`);
				}
			}),
		);

		if (items.length < batchSize) {
			hasMore = false;
		} else {
			fromOffset += batchSize;
			pageNum++;
		}
	}
}

async function run() {
	logger.step(`Starting RayWhite REST API scraper (Agent ${AGENT_ID})`);

	await scrapeCategory('SALES', false);
	await scrapeCategory('RENTALS', true);

	logger.step(`Cleaning up removed properties for Agent ${AGENT_ID}`);
	await updateRemoveStatus(AGENT_ID, scrapeStartTime);

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
