// Ulster Property Sales (NI) Scraper
// Agent ID: 270
// Agent Name: Ulster Property Sales (NI)
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

const AGENT_ID = 270;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: 'https://www.ulsterpropertysales.co.uk/search/1419833/',
		isRental: false,
		label: 'SALES',
	},
	{
		
		baseUrl: 'https://www.ulsterpropertysales.co.uk/search/1419835/',
		isRental: true,
		label: 'RENTALS',
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
            const iconMatch = html.match(/fa-bed[^<]*?<\/span>\s*(\d+)\s*bedrooms?/i);
            if (iconMatch) {
                bedrooms = parseInt(iconMatch[1], 10);
            }

            if (!bedrooms) {
                const iconsBlock = document.querySelector('.icons2')?.innerText || '';
                const match = iconsBlock.match(/(\d+)\s*bedrooms?/i);
                if (match) bedrooms = parseInt(match[1], 10);
            }

            if (!bedrooms) {
                const topSection = document.querySelector('.prop-det-top-right')?.innerText || 
                                  document.querySelector('.prop-det-price-outer')?.parentElement?.innerText || '';
                const match = topSection.match(/(\d+)\s*bedrooms?/i);
                if (match) bedrooms = parseInt(match[1], 10);
            }

          
            if (!bedrooms) {
                const allMatches = [...html.matchAll(/(\d+)\s*bedrooms?/gi)];
                for (const m of allMatches) {
                    const num = parseInt(m[1], 10);
                    if (num >= 1 && num <= 10) {   // realistic bedroom count
                        bedrooms = num;
                        break;
                    }
                }
            }

            // Coordinates
            const coordMatch = html.match(/myLatLng\s*=\s*\{\s*lat:\s*([\d.-]+)\s*,\s*lng:\s*([\d.-]+)/i);
            if (coordMatch) {
                latitude = parseFloat(coordMatch[1]);
                longitude = parseFloat(coordMatch[2]);
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
	const { isRental, label, pageNum, totalPages, isFirstPage } = request.userData;

	logger.step(`Processing ${label} page ${pageNum ?? ''}/${totalPages ?? ''}: ${request.url}`);

	try {
		await page.waitForLoadState('networkidle', { timeout: 45000 }).catch(() => { });
		await page.waitForTimeout(3000);
	} catch (e) {
		logger.error(`Load timeout or network idle failure on ${label}`);
	}

	if (isFirstPage) {
		const discoveredTotalPages = await page.evaluate(() => {
		
			const anchors = Array.from(document.querySelectorAll('a'));
			let maxPage = 1;
			for (const a of anchors) {
				const href = a.getAttribute('href') || '';
			
				const m = href.match(/(?:\/|=)(page|p|\b)(\d{1,4})(?:\/)?$/i);
				if (m && m[2]) {
					const p = parseInt(m[2], 10);
					if (!isNaN(p)) maxPage = Math.max(maxPage, p);
				}
				
				const nums = href.match(/\/(\d{1,4})\/?$/);
				if (nums && nums[1]) {
					const p = parseInt(nums[1], 10);
					if (!isNaN(p)) maxPage = Math.max(maxPage, p);
				}
			}
			return maxPage;
		});

		if (discoveredTotalPages && discoveredTotalPages > 1) {
			logger.step(`Discovered ${discoveredTotalPages} pages for ${label}. Queuing...`);

			
			const base = request.url.replace(/\/+\d+\/+$/, '/');

			for (let p = 2; p <= discoveredTotalPages; p++) {
				const nextUrl = `${base}${p}/`;
				await crawler.addRequests([
					{
						url: nextUrl,
						userData: { isRental, label, pageNum: p, totalPages: discoveredTotalPages, isFirstPage: false },
					},
				]);
			}
		}
	}

	const properties = await page.evaluate(() => {
		const results = [];
		const cards = Array.from(document.querySelectorAll('a[href*="/property/"]'));

		for (const a of cards) {
			const href = a.getAttribute('href');
			if (!href) continue;

			const link = new URL(href, window.location.origin).href;
			const title = (a.getAttribute('title') || a.textContent || '').trim() || 'Property';
			let card = a.closest('div, li, article') || a.parentElement;
			let cardText = card ? card.innerText : '';

			let bedrooms = null;

			const bedMatch1 = cardText.match(/\*\s*(\d+)\s*(?=\n|\*|$)/);
			if (bedMatch1) bedrooms = parseInt(bedMatch1[1], 10);
			if (!bedrooms) {
				const bedMatch2 = cardText.match(/(\d+)\s*(?:bed|bedroom|beds)/i);
				if (bedMatch2) bedrooms = parseInt(bedMatch2[1], 10);
			}
			if (!bedrooms) {
				const postcodeMatch = cardText.match(/BT\d{1,2}\s*\d{1,2}[A-Z]?\s*\*?\s*(\d+)/i);
				if (postcodeMatch) bedrooms = parseInt(postcodeMatch[1], 10);
			}

			if (!bedrooms) {
				const addressLineMatch = cardText.match(/BT[\dA-Z\s]+\*?\s*(\d+)/i);
				if (addressLineMatch) bedrooms = parseInt(addressLineMatch[1], 10);
			}

			const priceMatch = cardText.match(/£\s*[\d,]+(\s*pm)?/i);
			const priceRaw = priceMatch ? priceMatch[0] : '';

			results.push({ link, title, priceRaw, bedrooms });
		}

		// Dedupe
		const map = new Map();
		for (const r of results) {
			if (!map.has(r.link)) map.set(r.link, r);
		}

		return Array.from(map.values());
	});

	logger.step(`Found ${properties.length} ${label} properties on this page.`);

	for (const property of properties) {
		if (!property.link || processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		const price = parsePrice(property.priceRaw);
		if (!price) {
			logger.property(1, label, property.title.substring(0, 40), 'N/A', property.link, isRental, 1, 'SKIPPED (No Price)');
			continue;
		}

		let bedrooms = property.bedrooms;
		if (!bedrooms) {
			const bedFallbackMatch = (property.title || '').match(/(\d+)\s*bed/i) || (property.priceRaw || '').match(/(\d+)\s*bed/i);
			if (bedFallbackMatch && bedFallbackMatch[1]) {
				bedrooms = parseInt(bedFallbackMatch[1], 10);
			}
		}


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

	logger.step(`Starting Agent ${AGENT_ID} - Ulster Property Sales (NI)`);
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

	const allRequests = PROPERTY_TYPES.map((type) => ({
		url:
			type.baseUrl.endsWith('/') ? `${type.baseUrl}${startPage}/` : `${type.baseUrl}/${startPage}/`,
		userData: {
			isRental: type.isRental,
			label: type.label,
			pageNum: startPage,
			totalPages: null,
			isFirstPage: startPage === 1,
		},
	}));

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

