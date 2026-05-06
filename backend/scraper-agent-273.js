// CPS Property (NI) Scraper
// Agent ID: 273
// Agent Name: CPS Property (NI)
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
	extractBedroomsFromHTML,
} = require('./lib/property-helpers.js');
const { createAgentLogger } = require('./lib/logger-helpers.js');

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 273;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: 'https://cps-property.com/search/510678/?includeagreed=1',
		isRental: false,
		label: 'SALES',
		totalPages: 14,
	},
	{
		baseUrl: 'https://cps-property.com/search/510680/?includeagreed=1',
		isRental: true,
		label: 'RENTALS',
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
			const html = document.documentElement.innerHTML;
			return { html };
		});

		const { latitude, longitude } = await extractCoordinatesFromHTML(detailInfo.html);
		const bedrooms = extractBedroomsFromHTML(detailInfo.html.replace(/<[^>]+>/g, ' '));

		return { bedrooms, latitude, longitude };
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
		await page.waitForLoadState('networkidle', { timeout: 45000 }).catch(() => { });
		await page.waitForTimeout(2000);
	} catch (e) {
		logger.error(`Load timeout or network idle failure on ${label}`);
	}

	const properties = await page.evaluate(() => {
    const seen = new Map();
    
    // Target ALL links that go to /property/
    const linkElements = document.querySelectorAll('a[href*="/property/"]');
    
    linkElements.forEach(a => {
        let href = a.getAttribute('href');
        if (!href) return;

        const fullLink = new URL(href, window.location.origin).href;
        const pathname = new URL(fullLink).pathname;

        if (!pathname.startsWith('/property/')) return;

        // Get the closest container that likely holds the full card info
        const cardRoot = a.closest('li, div, article, section') || 
                        a.parentElement?.parentElement || 
                        a.parentElement;

        // Title - try multiple strategies
        let title = a.innerText.trim() || 
                   (cardRoot ? cardRoot.querySelector('h1, h2, h3, strong')?.innerText.trim() : '') ||
                   'Property';

        // Clean title
        title = title.replace(/\s{2,}/g, ' ').trim();

        const cardText = cardRoot ? cardRoot.innerText : a.parentElement?.innerText || document.body.innerText;

        // Price
        const priceMatch = cardText.match(/£\s*[\d,]+(\s*pm)?/i);
        const priceRaw = priceMatch ? priceMatch[0] : '';

        // Bedrooms
        let bedrooms = null;
        const bedMatch = cardText.match(/(\d+)\s*(?:bed|bedroom|beds)/i);
        if (bedMatch) bedrooms = parseInt(bedMatch[1], 10);

        if (!seen.has(fullLink)) {
            seen.set(fullLink, {
                link: fullLink,
                title,
                priceRaw,
                bedrooms
            });
        }
    });

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

	logger.step(`Starting Agent ${AGENT_ID} - CPS Property (NI)`);
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
			let url = type.baseUrl;
			if (p > 1) {
				const urlObj = new URL(url);
				urlObj.pathname = urlObj.pathname.endsWith('/') 
					? `${urlObj.pathname}Page${p}/` 
					: `${urlObj.pathname}/Page${p}/`;
				url = urlObj.href;
			}

			allRequests.push({
				url,
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

