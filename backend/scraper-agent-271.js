// Templeton Robinson (NI) Scraper
// Agent ID: 271
// Agent Name: Templeton Robinson (NI)
// Updated: 06 May 2026

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
} = require('./lib/property-helpers.js');
const { createAgentLogger } = require('./lib/logger-helpers.js');

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 271;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: 'https://www.templetonrobinson.com/search/3578986/',
		isRental: false,
		label: 'SALES',
		// Total pages provided by user
		totalPages: 17,
	},
	{
		baseUrl: 'https://www.templetonrobinson.com/search/3578989/',
		isRental: true,
		label: 'RENTALS',
		// Total pages provided by user
		totalPages: 4,
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

		const detailInfo = await detailPage.evaluate(() => {
			let bedrooms = null;
			let latitude = null;
			let longitude = null;

			const html = document.documentElement.innerHTML;

			// Bedrooms: infer from the icon area or generic match.
			const bedIconMatch = html.match(/fa-bed[^<]*?<\/span>\s*(\d+)\s*bedrooms?/i);
			if (bedIconMatch) {
				bedrooms = parseInt(bedIconMatch[1], 10);
			}

			if (!bedrooms) {
				const allMatches = [...html.matchAll(/(\d+)\s*bedrooms?/gi)];
				for (const m of allMatches) {
					const num = parseInt(m[1], 10);
					if (num >= 1 && num <= 10) {
						bedrooms = num;
						break;
					}
				}
			}

			// Coordinates: match common snippet used by the provided templates.
			// Sale example: var map or myLatLng = { lat: 54..., lng: -5... }
			const coordMatch = html.match(
				/myLatLng\s*=\s*\{\s*lat:\s*([\d.-]+)\s*,\s*lng:\s*([\d.-]+)/i,
			);
			if (coordMatch) {
				latitude = parseFloat(coordMatch[1]);
				longitude = parseFloat(coordMatch[2]);
			}

			// Fallback for rent template: latlng = new google.maps.LatLng(-54..., -5...)
			if (latitude === null || longitude === null) {
				const latlngMatch = html.match(
					/LatLng\(\s*([\d.-]+)\s*,\s*([\d.-]+)\s*\)/i,
				);
				if (latlngMatch) {
					latitude = parseFloat(latlngMatch[1]);
					longitude = parseFloat(latlngMatch[2]);
				}
			}

			// Another fallback: address geocode initial values sometimes appear as lat: / lng:
			if (latitude === null || longitude === null) {
				const latMatch = html.match(/\blat\b\s*[:=]\s*([\d.-]+)/i);
				const lngMatch = html.match(/\blng\b\s*[:=]\s*([\d.-]+)/i);
				if (latMatch && lngMatch) {
					latitude = parseFloat(latMatch[1]);
					longitude = parseFloat(lngMatch[1]);
				}
			}

			return { bedrooms, latitude, longitude };
		});

		return detailInfo;
	} catch (error) {
		logger.error(`Detail page error ${property.link}: ${error.message}`);
		return { bedrooms: null, latitude: null, longitude: null };
	} finally {
		await detailPage.close();
	}
}

async function handleListingPage({ page, request, crawler }) {
	const { isRental, label, pageNum, totalPages } = request.userData;

	logger.step(`Processing ${label} page ${pageNum}/${totalPages}: ${request.url}`);

	try {
		await page.waitForLoadState('networkidle', { timeout: 45000 }).catch(() => {});
		await page.waitForTimeout(2500);
	} catch (e) {
		logger.error(`Load timeout or network idle failure on ${label}`);
	}

	const properties = await page.evaluate(() => {
		const results = [];

		// Listing cards generally contain anchors pointing to /property/...
		const cards = Array.from(document.querySelectorAll('a[href*="/property/"]'));

		for (const a of cards) {
			const href = a.getAttribute('href');
			if (!href) continue;

			const link = new URL(href, window.location.origin).href;

			// Robust title extraction: try to find a heading within the closest property card.
			const cardRoot = a.closest('div, li, article') || a.parentElement;
			const titleEl =
				cardRoot?.querySelector('h1, h2, h3, h4') ||
				cardRoot?.querySelector('[class*="name"], [class*="title"], [id*="name"], [id*="title"]') ||
				a;
			const title = (titleEl?.innerText || titleEl?.textContent || a.textContent || a.getAttribute('title') || '')
					.trim()
					.replace(/\s{2,}/g, ' ')
					|| 'Property';

			// Price and beds often appear near the card; use the nearest parent text.
			const cardText = cardRoot ? cardRoot.innerText : '';


			const priceMatch = cardText.match(/£\s*[\d,]+\s*(pm)?/i);
			const priceRaw = priceMatch ? priceMatch[0] : '';

			let bedrooms = null;
			const bedMatch = cardText.match(/(\d+)\s*(?:bed|bedroom|bedrooms|beds)/i);
			if (bedMatch) bedrooms = parseInt(bedMatch[1], 10);

			results.push({ link, title, priceRaw, bedrooms });
		}

		const map = new Map();
		for (const r of results) {
			if (!map.has(r.link)) map.set(r.link, r);
		}
		return Array.from(map.values());
	});

	logger.step(`Found ${properties.length} ${label} properties on page ${pageNum}.`);

	for (const property of properties) {
		if (!property.link || processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.property(1, label, property.title.substring(0, 40), 'N/A', property.link, isRental, 1, 'SKIPPED (No Price)');
			continue;
		}

		let bedrooms = property.bedrooms;

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

		if (result.updated) {
			action = 'UPDATED';
			counts.totalSaved++;
		}

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

			counts.totalSaved++;
			counts.totalScraped++;
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

		if (action !== 'UNCHANGED') await sleep(500);
	}
}

async function run() {
	const args = process.argv.slice(2);
	const startPage = args[0] ? parseInt(args[0], 10) : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Agent ${AGENT_ID} - Templeton Robinson (NI)`);
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
			allRequests.push({
				url: `${type.baseUrl}${p}/`,
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

	// If run is partial, updateRemoveStatus must still be called (project baseline).
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

