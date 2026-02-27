// Entwistle Green scraper refactor using Playwright with Crawlee
// Agent ID: 219
// Website: entwistlegreen.co.uk
// Usage:
// node backend/scraper-agent-219.refactored.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 219;
const logger = createAgentLogger(AGENT_ID);

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// Configuration for lettings (Entwistle Green)
const PROPERTY_TYPES = [
	{
		urlBase:
			"https://www.entwistlegreen.co.uk/properties/lettings/status-available/most-recent-first",
		totalPages: 18, // 173 properties / 10 per page = 18 pages
		isRental: true,
		label: "RENTALS",
	},
];

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, `Processing ${request.url}`, totalPages);

	try {
		await page.waitForTimeout(1500);
		await page.waitForSelector(".results-page", { timeout: 20000 }).catch(() => {
			logger.page(pageNum, label, "No listing container found");
		});

		const properties = await page.evaluate(() => {
			try {
				const container = document.querySelector('.results-page');
				if (!container) return [];
				const cards = Array.from(container.querySelectorAll('.card'));
				return cards
					.map(card => {
						const linkEl = card.querySelector('a.card__link');
						const href = linkEl ? linkEl.getAttribute('href') : null;
						const link = href ? (href.startsWith('http') ? href : 'https://www.entwistlegreen.co.uk' + href) : null;
						const priceEl = card.querySelector('a.card__link span');
						const price = priceEl ? priceEl.textContent.trim() : '';
						const titleEl = card.querySelector('p.card__text-content');
						const title = titleEl ? titleEl.textContent.trim() : '';
						const spec1 = card.querySelector('.card-content__spec-list li:nth-child(1) .card-content__spec-list-number')?.textContent.trim() || null;
						const spec2 = card.querySelector('.card-content__spec-list li:nth-child(2) .card-content__spec-list-number')?.textContent.trim() || null;
						const spec3 = card.querySelector('.card-content__spec-list li:nth-child(3) .card-content__spec-list-number')?.textContent.trim() || null;
						const statusText = card.innerText || '';
						return { link, title, price, bedrooms: spec1, reception: spec3, bathrooms: spec2, statusText };
					})
					.filter(p => p && p.link);
			} catch (e) {
				console.log('Error extracting properties:', e);
				return [];
			}
		});

		logger.page(pageNum, label, `Found ${properties.length} properties`);
		stats.totalScraped += properties.length;

		const batchSize = 2;
		for (let i = 0; i < properties.length; i += batchSize) {
			const batch = properties.slice(i, i + batchSize);
			await Promise.all(batch.map(async (property) => {
				if (!property.link) return;
				if (processedUrls.has(property.link)) return;
				if (isSoldProperty(property.statusText || '')) return;

				processedUrls.add(property.link);

				let coords = { latitude: null, longitude: null };
				let detailHtml = null;
				try {
					const detailPage = await page.context().newPage();
					try {
						await detailPage.goto(property.link, { waitUntil: 'domcontentloaded', timeout: 30000 });
						await detailPage.waitForTimeout(500);
						detailHtml = await detailPage.content();
						const extracted = extractCoordinatesFromHTML(detailHtml);
						if (extracted) {
							coords.latitude = extracted.latitude;
							coords.longitude = extracted.longitude;
						}
					} catch (err) {
						// ignore detail page errors
					} finally {
						await detailPage.close();
					}
				} catch (e) {
					// ignore
				}

				try {
					const priceClean = property.price ? property.price.replace(/[^0-9.]/g, '') : null;
					const priceNum = priceClean ? parseFloat(priceClean) : null;
					if (!priceNum) {
						logger.property(pageNum, label, property.title, 'N/A', property.link, true, totalPages, 'ERROR');
						return;
					}

					const dbPrice = Number(priceNum).toLocaleString("en-GB");// already returns formatted string without currency symbol
					const updateResult = await updatePriceByPropertyURLOptimized(
						property.link.trim(),
						dbPrice,
						property.title,
						property.bedrooms,
						AGENT_ID,
						isRental,
					);

					let persisted = !!updateResult.updated;
					if (!updateResult.isExisting) {
						// New property - use helper to process with coordinates and bedrooms
						await processPropertyWithCoordinates(
							property.link.trim(),
							dbPrice,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
							detailHtml,
							coords.latitude,
							coords.longitude,
						);
						persisted = true;
					}

					if (persisted) {
						stats.totalSaved++;
						if (isRental) stats.savedRentals++; else stats.savedSales++;
						logger.property(pageNum, label, property.title, `£${dbPrice}`, property.link, isRental, totalPages, 'CREATED');
					}
				} catch (dbErr) {
					logger.error('DB error', dbErr, pageNum, label);
				}
			}));

			await page.waitForTimeout(500);
		}
	} catch (error) {
		logger.error(`Error in ${label} page ${pageNum}`, error, pageNum, label);
	}
}

async function scrapeEntwistleGreen() {
	logger.step(`Starting Entwistle Green scraper (Agent ${AGENT_ID})`);
	const browserWSEndpoint = getBrowserlessEndpoint();
	logger.step(`Connecting to browserless: ${browserWSEndpoint.split('?')[0]}`);

	for (const propertyType of PROPERTY_TYPES) {
		logger.step(`Processing ${propertyType.label} (${propertyType.totalPages} pages)`);
		const crawler = createCrawler(browserWSEndpoint);
		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = pg === 1 ? `${propertyType.urlBase}#/` : `${propertyType.urlBase}/page-${pg}#/`;
			requests.push({ url, userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label, totalPages: propertyType.totalPages } });
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	logger.step(`Scraping complete. Total scraped: ${stats.totalScraped}, total saved: ${stats.totalSaved}`);
}

(async () => {
	try {
		await scrapeEntwistleGreen();
		await updateRemoveStatus(AGENT_ID);
		logger.step('All done!');
		process.exit(0);
	} catch (err) {
		logger.error('Fatal error', err);
		process.exit(1);
	}
})();