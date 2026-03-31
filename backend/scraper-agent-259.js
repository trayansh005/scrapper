'use strict';

const { PlaywrightCrawler } = require('crawlee');
const { createAgentLogger } = require('./lib/logger-helpers.js');
const { isSoldProperty, parsePrice, formatPriceDisplay } = require('./lib/property-helpers.js');
const { updatePriceByPropertyURLOptimized, processPropertyWithCoordinates } = require('./lib/db-helpers.js');
const { updateRemoveStatus } = require('./db.js');

// Marriotts scraper using Playwright with Crawlee
// Agent ID: 259
// Website: https://marriotts.net/ (Property Hive + Uncode theme)
// Usage: node backend/scraper-agent-259.js [startPage]

const AGENT_ID = 259;
const AGENT_NAME = 'Marriotts';
const logger = createAgentLogger(AGENT_ID);

const BROWSERLESS_URL = process.env.BROWSERLESS_URL || 'ws://browserless-e44co4wws040gcokws8k0c00:3000';

const PROPERTY_TYPES = [
	{
		baseUrl: 'https://marriotts.net/property-search/?address_keyword=&minimum_price=&maximum_price=&minimum_bedrooms=&availability=17&minimum_rent=&maximum_rent=&department=residential-sales',
		totalPages: 7,
		isRental: false,
		label: 'SALES',
	},
	{
		baseUrl: 'https://marriotts.net/property-search/?address_keyword=&minimum_price=&maximum_price=&minimum_bedrooms=&availability=&minimum_rent=&maximum_rent=&department=residential-lettings',
		totalPages: 5,
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

const scrapeStartTime = new Date();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockNonEssentialResources(page) {
	return page.route('**/*', (route) => {
		const url = route.request().url();
		if (/\.(png|jpg|jpeg|gif|webp|svg|woff|woff2|ttf|eot|mp4|webm)$/i.test(url)) {
			route.abort();
		} else {
			route.continue();
		}
	});
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {
	await sleep(600);

	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: 'domcontentloaded',
			timeout: 90000,
		});

		await detailPage.waitForTimeout(800);

		// Extract coordinates from detail page
		const coords = await detailPage.evaluate(() => {
			// Method 1: JSON-LD schema (primary)
			const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
			for (const script of scripts) {
				try {
					const data = JSON.parse(script.textContent);
					if (data?.geo?.latitude && data?.geo?.longitude) {
						return {
							latitude: parseFloat(data.geo.latitude),
							longitude: parseFloat(data.geo.longitude),
						};
					}
				} catch (e) {
					// Invalid JSON, continue
				}
			}

			// Method 2: Google Maps JavaScript (fallback)
			const allScripts = Array.from(document.querySelectorAll('script'));
			for (const script of allScripts) {
				const content = script.textContent;
				if (content && content.includes('google.maps.LatLng')) {
					const coordMatch = content.match(/google\.maps\.LatLng\s*\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)/);
					if (coordMatch && coordMatch[1] && coordMatch[2]) {
						const lat = parseFloat(coordMatch[1]);
						const lon = parseFloat(coordMatch[2]);
						if (!isNaN(lat) && !isNaN(lon) && lat !== 0 && lon !== 0) {
							return { latitude: lat, longitude: lon };
						}
					}
				}
			}

			return null;
		});

		return {
			coords: coords || { latitude: null, longitude: null },
		};
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, totalPages, isRental, label } = request.userData;

	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForSelector('.propertyhive ul.properties li.property', { timeout: 15000 });
	} catch (e) {
		logger.error('Property container not found', e, pageNum, label);
	}

	await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => null);
	await page.waitForTimeout(1000);

	// Extract properties from listing page
	const extractionResult = await page.evaluate(() => {
		const results = [];
		const seen = new Set();
		const debugInfo = {
			matching_selector: null,
			elements_found: 0,
			final_count: 0,
		};

		// Marriotts uses Property Hive with Uncode theme
		const selectorPatterns = [
			'ul.properties li.property',
			'.propertyhive ul.properties li.property',
			'li.property',
			'li.type-property',
			'ul.properties li',
			'.propertyhive li.property',
		];

		for (const selector of selectorPatterns) {
			const elements = document.querySelectorAll(selector);
			if (elements.length > 0) {
				debugInfo.matching_selector = selector;
				debugInfo.elements_found = elements.length;

				for (const el of elements) {
					try {
						// Extract link from h3 > a
						const linkEl = el.querySelector('h3 a');
						if (!linkEl) continue;

						let href = linkEl.getAttribute('href');
						if (!href) continue;

						const fullLink = href.startsWith('http') ? href : new URL(href, window.location.origin).href;
						if (seen.has(fullLink)) continue;

						// Skip if URL doesn't look like a property page
						if (!fullLink.includes('/property/')) continue;

						seen.add(fullLink);

						// Extract title
						const title = linkEl.textContent?.trim() || '';

						// Extract price
						const priceElm = el.querySelector('.price');
						const priceText = priceElm ? priceElm.textContent.trim() : '';

						// Extract bedrooms
						const bedroomsElm = el.querySelector('.room.bedrooms');
						const bedroomsText = bedroomsElm ? bedroomsElm.textContent.trim() : '';

						// Extract status from flag
						const flagElm = el.querySelector('.flag');
						const status = flagElm ? flagElm.textContent.trim() : '';

						// Extract summary
						const summaryElm = el.querySelector('.summary');
						const summary = summaryElm ? summaryElm.textContent.trim() : '';

						results.push({
							title,
							link: fullLink,
							price: priceText,
							bedrooms: bedroomsText,
							status,
							summary,
						});
					} catch (err) {
						// Skip problematic elements
					}
				}

				debugInfo.final_count = results.length;
				break; // Use first matching selector
			}
		}

		return { results, debugInfo };
	});

	logger.page(
		pageNum,
		label,
		`Extraction details - Selector: "${extractionResult.debugInfo.matching_selector || 'NONE'}" | Elements found: ${extractionResult.debugInfo.elements_found} | Properties extracted: ${extractionResult.debugInfo.final_count}`,
		totalPages
	);

	// Process each property
	const browserContext = page.context();
	for (const property of extractionResult.results) {
		try {
			counts.totalScraped++;

			// Parse price
			const priceMatch = property.price.match(/£([\d,]+(?:\.\d{2})?)/);
			const price = priceMatch ? parseFloat(priceMatch[1].replace(/,/g, '')) : null;

			// Parse bedrooms
			const bedroomsMatch = property.bedrooms.match(/(\d+)/);
			const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[1]) : null;

			// Check if property is already in database
			const priceResult = await updatePriceByPropertyURLOptimized(property.link, price, AGENT_ID);

			if (priceResult.isExisting && process.env.SKIP_DETAIL_PAGES !== 'true') {
				// Property exists, just update price
				logger.property(property.title, property.link, 'UPDATED', {
					price: formatPriceDisplay(price),
					bedrooms,
				});

				if (priceResult.updated) {
					counts.totalSaved++;
					if (!isRental) counts.savedSales++;
					else counts.savedRentals++;
				} else {
					logger.property(property.title, property.link, 'UNCHANGED', {});
				}
			} else if (!priceResult.isExisting) {
				// New property - scrape detail page for coordinates
				const detailResult = await scrapePropertyDetail(browserContext, property);

				const coords = detailResult?.coords || { latitude: null, longitude: null };

				await processPropertyWithCoordinates(
					property.link,
					price,
					property.title,
					bedrooms,
					AGENT_ID,
					isRental,
					null,
					coords.latitude,
					coords.longitude
				);

				counts.totalSaved++;
				if (!isRental) counts.savedSales++;
				else counts.savedRentals++;

				logger.property(property.title, property.link, 'CREATED', {
					price: formatPriceDisplay(price),
					bedrooms,
					latitude: coords.latitude,
					longitude: coords.longitude,
				});

				// Only sleep on CREATED for politeness
				await sleep(500);
			}
		} catch (error) {
			logger.error(`Error processing property ${property.link}`, error);
			logger.property(property.title, property.link, 'ERROR', {
				error: error.message,
			});
		}
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function run() {
	const startPageArg = parseInt(process.argv[2], 10);
	const startPage = !isNaN(startPageArg) && startPageArg > 0 ? startPageArg : 1;

	logger.step(`Starting ${AGENT_NAME} scraper (Agent ${AGENT_ID})`);

	try {
		const initialRequests = [];

		for (const propertyType of PROPERTY_TYPES) {
			const { baseUrl, totalPages, isRental, label } = propertyType;

			// Handle pagination
			for (let pageNum = startPage; pageNum <= totalPages; pageNum++) {
				// Marriotts uses format: /property-search/page/2/?...
				let url = baseUrl;
				if (pageNum > 1) {
					// Insert page number into the URL path
					url = baseUrl.replace('/property-search/', `/property-search/page/${pageNum}/`);
				}

				initialRequests.push({
					url,
					userData: {
						pageNum,
						totalPages,
						isRental,
						label,
					},
				});
			}
		}

		const crawler = new PlaywrightCrawler({
			maxConcurrency: 1,
			maxRequestRetries: 2,
			navigationTimeoutSecs: 60,
			requestHandlerTimeoutSecs: 180,

			launchContext: {
				launchOptions: {
					browserWSEndpoint: BROWSERLESS_URL,
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

			failedRequestHandler: async ({ request }) => {
				const { pageNum, label } = request.userData;
				logger.error(`Failed listing page - ${label} page ${pageNum}: ${request.url}`);
			},
		});

		await crawler.run(initialRequests);

		logger.step(`Completed ${AGENT_NAME} - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`);
		logger.step(`Breakdown - SALES: ${counts.savedSales}, RENTALS: ${counts.savedRentals}`);

		// Update remove status for properties no longer on site
		if (startPage === 1) {
			logger.step(`Updating remove status for properties not found in this run...`);
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		}
	} catch (error) {
		logger.error(`Fatal error in ${AGENT_NAME} scraper`, error);
		process.exit(1);
	}
}

// Execute
run().catch((err) => {
	logger.error('Unhandled error', err);
	process.exit(1);
});
