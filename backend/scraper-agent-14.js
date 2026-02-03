// Chestertons scraper using Playwright with Crawlee
// Agent ID: 14
//
// Usage:
// node backend/scraper-agent-14.js [startPage]
// Example: node backend/scraper-agent-14.js 10 (starts from page 10)

const { CheerioCrawler, PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");
const { logMemoryUsage } = require("./lib/scraper-utils.js");
const { updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { EventEmitter } = require("events");

// Increase max listeners to prevent memory leak warnings
EventEmitter.defaultMaxListeners = 100;

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 14;
let totalScraped = 0;
let totalSaved = 0;

// Small helper utilities to avoid rate-limiting
const userAgents = [
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15",
	"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
];

function sleep(ms) {
	return new Promise((res) => setTimeout(res, ms));
}

function randBetween(min, max) {
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

// Start page
const START_PAGE = 1;

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.chestertons.co.uk/properties/sales/status-available",
		isRental: false,
		label: "SALES",
		totalRecords: 1747,
		recordsPerPage: 12,
	},
	{
		urlBase: "https://www.chestertons.co.uk/properties/lettings/status-available",
		isRental: true,
		label: "LETTINGS",
		totalRecords: 1132,
		recordsPerPage: 12,
	},
];

async function scrapeChestertons() {
	console.log(`\n🚀 Starting Chestertons scraper (Agent ${AGENT_ID})...\n`);
	logMemoryUsage("START");

	// Browserless configuration
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;

	console.log(
		`🌐 Connecting to browserless for listing and detail pages: ${browserWSEndpoint.split("?")[0]}`,
	);

	// Create a unified Playwright crawler that handles both listing and detail pages
	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		requestHandlerTimeoutSecs: 120,

		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label, property, isDetailPage } = request.userData || {};

			// Handle detail pages
			if (isDetailPage) {
				// Add 1 second delay between each detail page visit
				await sleep(1000);

				try {
					const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
					await page.setUserAgent(ua);
					await page.setExtraHTTPHeaders({ "accept-language": "en-GB,en;q=0.9" });
				} catch (e) {}

				await sleep(randBetween(800, 1800));
				const resp = await page.goto(request.url, {
					waitUntil: "domcontentloaded",
					timeout: 30000,
				});

				if (resp?.status?.() === 429) {
					console.warn(`⚠️ 429 on ${request.url} — backing off`);
					await sleep(60000);
					throw new Error("429");
				}

				await page.waitForTimeout(1000);

				// Extract coordinates using helper function
				const htmlContent = await page.content();
				const coords = await extractCoordinatesFromHTML(htmlContent);

				await updatePriceByPropertyURL(
					property.link,
					property.price,
					property.title,
					property.bedrooms,
					AGENT_ID,
					property.isRental,
					coords.latitude,
					coords.longitude,
				);
				totalScraped++;
				totalSaved++;

				const coordsStr =
					coords.latitude && coords.longitude
						? `${coords.latitude}, ${coords.longitude}`
						: "No coords";
				console.log(`✅ ${property.title} - ${formatPrice(property.price)} - ${coordsStr}`);
				return;
			}

			// Handle listing pages
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			try {
				const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
				await page.setUserAgent(ua);
				await page.setExtraHTTPHeaders({ "accept-language": "en-GB,en;q=0.9" });
			} catch (e) {}

			await sleep(randBetween(500, 1200));
			const resp = await page.goto(request.url, {
				waitUntil: "domcontentloaded",
				timeout: 30000,
			});

			if (resp?.status?.() === 429) {
				console.warn(`⚠️ 429 on ${request.url} — backing off`);
				await sleep(60000);
				throw new Error("429");
			}

			// Wait for property cards to load
			await page.waitForTimeout(2000);

			// Extract properties using Playwright
			const htmlContent = await page.content();
			const $ = cheerio.load(htmlContent);

			const properties = [];
			$(".pegasus-property-card").each((index, element) => {
				try {
					const $card = $(element);
					const linkEl = $card.find("a[href*='/properties/']").first();
					let href = linkEl.attr("href");
					if (!href) return;
					if (!href.startsWith("http")) href = "https://www.chestertons.co.uk" + href;

					let price = null;
					$card.find("span").each((i, span) => {
						const text = $(span).text().trim();
						const m = text.match(/£([\d,]+)/);
						if (m) {
							price = m[1].replace(/,/g, "");
							return false; // break
						}
					});

					const title = linkEl.attr("title") || linkEl.text().trim();
					if (!title || !href || !price) return;

					let bedrooms = null;
					$card.find("svg[aria-labelledby]").each((i, svg) => {
						const titleText = $(svg).find("title").text();
						if (titleText === "Bedrooms") {
							bedrooms = $(svg).parent().next().text().trim();
							return false; // break
						}
					});

					properties.push({ link: href, title, price, bedrooms });
				} catch (e) {
					// Skip this card
				}
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process properties sequentially - update prices and scrape details for new ones
			for (const p of properties) {
				// First check if property exists using optimized method
				const result = await updatePriceByPropertyURLOptimized(
					p.link,
					p.price,
					p.title,
					p.bedrooms,
					AGENT_ID,
					isRental,
				);

				// If it's a new property, scrape detail page immediately
				if (!result.isExisting && !result.error) {
					console.log(`🆕 Scraping detail for new property: ${p.title}`);
					await crawler.addRequests([
						{
							url: p.link,
							userData: { property: { ...p, isRental }, isDetailPage: true },
						},
					]);
				}
			}
		},

		failedRequestHandler({ request }) {
			const { isDetailPage } = request.userData || {};
			if (isDetailPage) {
				console.error(`❌ Failed detail page: ${request.url}`);
			} else {
				console.error(`❌ Failed listing page: ${request.url}`);
			}
		},
	});

	// Get starting page from command line argument (default to START_PAGE)
	const args = process.argv.slice(2);
	const startPageArg = args.length > 0 ? parseInt(args[0]) : START_PAGE;

	// Queue pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		const totalPages =
			propertyType.totalRecords && propertyType.recordsPerPage
				? Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage)
				: 1;
		console.log(`🏠 Queueing ${propertyType.label} pages: ${totalPages} pages`);
		const startPage =
			!isNaN(startPageArg) && startPageArg > 0 && startPageArg <= totalPages
				? startPageArg
				: Math.max(1, START_PAGE);

		console.log(`📋 Starting from page ${startPage} to ${totalPages}`);

		// Queue all pages - they will be processed sequentially due to maxConcurrency: 1
		const requests = [];
		for (let page = startPage; page <= totalPages; page++) {
			// Chestertons uses ?page=N query parameter
			const url = page === 1 ? propertyType.urlBase : `${propertyType.urlBase}?page=${page}`;
			const uniqueKey = `${propertyType.label}_page_${page}`;

			requests.push({
				url,
				uniqueKey,
				userData: {
					pageNum: page,
					isRental: propertyType.isRental,
					label: propertyType.label,
					isDetailPage: false,
				},
			});
		}

		// Add all requests and run - maxConcurrency: 1 ensures sequential processing
		await crawler.addRequests(requests);
		await crawler.run();

		logMemoryUsage(`After ${propertyType.label}`);
	}

	console.log(
		`\n✅ Completed Chestertons - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`,
	);
	logMemoryUsage("END");
}

(async () => {
	try {
		await scrapeChestertons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
