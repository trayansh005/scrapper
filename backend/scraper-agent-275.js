// Haybrook (HAY) Scraper
// Agent ID: 275
// Agent Name: Haybrook
// Created: 06 May 2026

'use strict';

const { PlaywrightCrawler, log } = require('crawlee');
const { updateRemoveStatus } = require('./db.js');
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require('./lib/db-helpers.js');
const {
	parsePrice,
	formatPriceDisplay,
	extractCoordinatesFromHTML,
} = require('./lib/property-helpers.js');
const { createAgentLogger } = require('./lib/logger-helpers.js');

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 275;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		// Provided base: already a search listing page
		baseUrl: 'https://www.haybrook.com/search/?Location=Sheffield%2C+UK&Latitude=53.38112899999999&Longitude=-1.470085&SearchDistance=50&MinPrice=&MaxPrice=&MinimumBeds=&Stc=False&MinEfficiency=&CreatedWithinDays=&IsPurchase=True&CustomFacet=&SortBy=price%7Cdesc&NumberOfResults=20&NumberOfResults=20',
		isRental: false,
		label: 'SALES',
		totalPages: 50,
	},
	{
		baseUrl: 'https://www.haybrook.com/search/?IsPurchase=False&Location=Sheffield%2C+UK&SearchDistance=50&Latitude=53.38112899999999&Longitude=-1.470085&lettingCalc=monthly&MinPrice=&MaxPrice=&MinimumBeds=&Furnished=1&Furnished=0&SortBy=&Page=1&NumberOfResults=0',
		isRental: true,
		label: 'RENTALS',
		totalPages: 50,
	},
];

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockNonEssentialResources(page) {
	return page.route('**/*', (route) => {
		const resourceType = route.request().resourceType();
		if (['image', 'font', 'stylesheet', 'media'].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
}

async function scrapePropertyDetail(browserContext, property) {
	const detailPage = await browserContext.newPage();
	try {
		await blockNonEssentialResources(detailPage);
		await detailPage.goto(property.link, {
			waitUntil: 'domcontentloaded',
			timeout: 60000,
		});
		await detailPage.waitForTimeout(1500);

const detailHtml = await detailPage.evaluate(() => document.documentElement.innerHTML);

		// Haybrook embeds a JS object in the page:
		// var propertyResults = [{ ..., Location: {Lat: 53..., Lon: -1...} }];
		// extractCoordinatesFromHTML() may not match this exact casing.
		const coordsFromHaybrookPayload = await detailPage.evaluate(() => {
			try {
				const match = document.documentElement.innerHTML.match(
					/var\s+propertyResults\s*=\s*([\s\S]*?);/i,
				);
				if (!match) return { latitude: null, longitude: null };
				const payload = match[1];
				const latMatch = payload.match(/"Lat"\s*:\s*([-0-9.]+)/i) || payload.match(/Lat\s*:\s*([-0-9.]+)/i);
				const lonMatch = payload.match(/"Lon"\s*:\s*([-0-9.]+)/i) || payload.match(/Lon\s*:\s*([-0-9.]+)/i);
				return {
					latitude: latMatch ? parseFloat(latMatch[1]) : null,
					longitude: lonMatch ? parseFloat(lonMatch[1]) : null,
				};
			} catch (e) {
				return { latitude: null, longitude: null };
			}
		});

		const fallbackCoords = await extractCoordinatesFromHTML(detailHtml);
		const latitude = coordsFromHaybrookPayload.latitude ?? fallbackCoords.latitude;
		const longitude = coordsFromHaybrookPayload.longitude ?? fallbackCoords.longitude;

		return { bedrooms: null, latitude, longitude };
	} catch (error) {
		logger.error(`Detail page error ${property.link}: ${error.message}`);
		return { bedrooms: null, latitude: null, longitude: null };
	} finally {
		await detailPage.close();
	}
}

async function handleListingPage({ page, request }) {
	const { isRental, label, pageNum, totalPages } = request.userData;

	logger.step(`Processing ${label} page ${pageNum}/${totalPages}: ${request.url}`);

	try {
		await page.waitForLoadState('networkidle', { timeout: 45000 }).catch(() => {});
		await page.waitForTimeout(2000);
	} catch (e) {
		logger.error(`Load timeout or network idle failure on ${label}`);
	}

	const properties = await page.evaluate(() => {
		// Haybrook listing cards include anchors to property detail pages.
		const anchors = Array.from(document.querySelectorAll('a[href]'));
		const seen = new Map();

		for (const a of anchors) {
			const href = a.getAttribute('href') || '';
			if (!href) continue;
			// Detail pages look like:
			// /buying/<...>/hayXXXX/
			// /renting/<...>/hayXXXX/
			if (!href.includes('/hay') || !href.includes('/buying/') && !href.includes('/renting/')) continue;
			if (!href.startsWith('/buying/') && !href.startsWith('/renting/')) continue;

			const link = new URL(href, window.location.origin).href;

			const cardRoot = a.closest('div, li, article') || a.parentElement;
			const text = (cardRoot ? cardRoot.innerText : a.textContent) || '';

			const titleEl = cardRoot?.querySelector('h1, h2, h3, h4') || a;
			const title = (titleEl?.innerText || titleEl?.textContent || a.getAttribute('title') || a.textContent || 'Property')
				.trim()
				.replace(/\s{2,}/g, ' ');

			const priceMatch = text.match(/\u00a3\s*[\d,]+\s*(pm)?/i);
			const priceRaw = priceMatch ? priceMatch[0] : '';

			let bedrooms = null;
			const bedMatch = text.match(/(\d+)\s*(?:bed|bedroom|bedrooms|beds)\b/i);
			if (bedMatch) bedrooms = parseInt(bedMatch[1], 10);

			seen.set(link, { link, title: title || 'Property', priceRaw, bedrooms });
		}

		return Array.from(seen.values());
	});

	logger.step(`Found ${properties.length} ${label} properties on page ${pageNum}.`);

	for (const property of properties) {
		if (!property.link || processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.property(
				1,
				label,
				property.title.substring(0, 40),
				'N/A',
				property.link,
				isRental,
				1,
				'SKIPPED (No Price)',
			);
			continue;
		}

		const bedrooms = property.bedrooms;

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			price,
			property.title,
			bedrooms,
			AGENT_ID,
			isRental,
		);

		let action = 'UNCHANGED';
		let lat = null;
		let lng = null;

		if (result.updated) action = 'UPDATED';

		if (!result.isExisting && !result.error) {
			const details = await scrapePropertyDetail(page.context(), property);
			lat = details.latitude;
			lng = details.longitude;

			await processPropertyWithCoordinates(
				property.link,
				price,
				property.title,
				details.bedrooms ?? bedrooms ?? null,
				AGENT_ID,
				isRental,
				null,
				lat,
				lng,
			);

			counts.totalScraped++;
			counts.totalSaved++;
			if (isRental) counts.savedRentals++; else counts.savedSales++;
			action = 'CREATED';
		}

		logger.property(
			1,
			label,
			property.title.substring(0, 40),
			formatPriceDisplay(price, isRental),
			property.link,
			isRental,
			1,
			action,
			lat,
			lng,
		);

		if (action !== 'UNCHANGED') await sleep(400);
	}
}

async function run() {
	const args = process.argv.slice(2);
	const startPage = args[0] ? parseInt(args[0], 10) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Agent ${AGENT_ID} - Haybrook`);
	logger.step(`Start Page: ${startPage}`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 120,
		requestHandlerTimeoutSecs: 1200,
		launchContext: {
			launchOptions: {
				browserWSEndpoint:
					process.env.BROWSERLESS_WS_ENDPOINT ||
						`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`,
				args: ['--no-sandbox', '--disable-setuid-sandbox'],
				viewport: { width: 1920, height: 1080 },
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		requestHandler: handleListingPage,
	});

	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		for (let p = startPage; p <= type.totalPages; p++) {
			// Fix pagination by rewriting/adding Page=<p>.
			const url = new URL(type.baseUrl);
			url.searchParams.set('Page', String(p));
			allRequests.push({
				url: url.toString(),
				userData: {
					isRental: type.isRental,
					label: type.label,
					pageNum: p,
					totalPages: type.totalPages,
				},
			});
		}
	}

	await crawler.run(allRequests);

	logger.step(`Completed - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`);
	logger.step(`Breakdown - SALES: ${counts.savedSales}, RENTALS: ${counts.savedRentals}`);

	await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	logger.step(`Remove-status update done${isPartialRun ? ' (partial run)' : ''}.`);
}

(async () => {
	try {
		await run();
		logger.step('All done!');
		process.exit(0);
	} catch (err) {
		logger.error(`Fatal error: ${err?.message || err}`);
		process.exit(1);
	}
})();

