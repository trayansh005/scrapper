// Chestertons scraper using Playwright with Crawlee
// Agent ID: 14
//
// Usage:
// node backend/scraper-agent-14.js [startPage]
// Example: node backend/scraper-agent-14.js 10 (starts from page 10)

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");
const { logMemoryUsage } = require("./lib/scraper-utils.js");
const { updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");

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

	console.log(`🌐 Connecting to browserless at: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 5,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},

		async requestHandler({ page, request }) {
			await sleep(randBetween(1200, 3200));
			try {
				await page.setExtraHTTPHeaders({ "accept-language": "en-GB,en;q=0.9" });
			} catch (e) {}

			const { pageNum, isRental, label } = request.userData || {};
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(2000);
			// Wait for property cards to load (don't log "no properties" - extraction happens anyway)
			await page.waitForSelector(".properties-results", { timeout: 30000 }).catch(() => {});

			// Extract properties from listing page
			const properties = await page.$$eval(".pegasus-property-card", (cards) => {
				return cards
					.map((c) => {
						try {
							const linkEl = c.querySelector("a[href*='/properties/']");
							if (!linkEl) return null;
							let href = linkEl.getAttribute("href");
							if (!href) return null;
							if (!href.startsWith("http")) href = "https://www.chestertons.co.uk" + href;

							let price = null;
							const priceSpans = c.querySelectorAll("span");
							for (const span of priceSpans) {
								const m = span.textContent.trim().match(/£([\d,]+)/);
								if (m) {
									price = m[1].replace(/,/g, "");
									break;
								}
							}

							const title = linkEl.getAttribute("title") || linkEl.textContent.trim();
							if (!title || !href || !price) return null;

							let bedrooms = null;
							const bedroomSvg = Array.from(c.querySelectorAll("svg[aria-labelledby]")).find(
								(svg) => svg.querySelector("title")?.textContent === "Bedrooms",
							);
							if (bedroomSvg) {
								bedrooms = bedroomSvg.parentElement?.nextElementSibling?.textContent.trim();
							}

							return { link: href, title, price, bedrooms };
						} catch (e) {
							return null;
						}
					})
					.filter(Boolean);
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			// Process each property one by one
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

				// Only visit detail page for new properties
				if (!result.isExisting && !result.error) {
					const detailPage = await page.context().newPage();
					try {
						try {
							const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
							await detailPage.setUserAgent(ua);
							await detailPage.setExtraHTTPHeaders({
								"accept-language": "en-GB,en;q=0.9",
								referer: request.url,
							});
						} catch (e) {}

						await sleep(randBetween(800, 1800));
						const resp = await detailPage.goto(p.link, {
							waitUntil: "domcontentloaded",
							timeout: 30000,
						});

						if (resp?.status?.() === 429) {
							console.warn(`⚠️ 429 on ${p.link} — backing off`);
							await sleep(60000);
							throw new Error("429");
						}

						await detailPage.waitForTimeout(1000);

						// Extract coordinates using helper function
						const htmlContent = await detailPage.content();
						const coords = await extractCoordinatesFromHTML(htmlContent);

						await updatePriceByPropertyURL(
							p.link,
							p.price,
							p.title,
							p.bedrooms,
							AGENT_ID,
							isRental,
							coords.latitude,
							coords.longitude,
						);
						totalScraped++;
						totalSaved++;

						const coordsStr =
							coords.latitude && coords.longitude
								? `${coords.latitude}, ${coords.longitude}`
								: "No coords";
						console.log(`✅ ${p.title} - ${formatPrice(p.price)} - ${coordsStr}`);
					} catch (err) {
						console.error(`❌ Error processing ${p.link}: ${err.message}`);
					} finally {
						await detailPage.close();
					}
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
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
		const requests = [];
		const startPage =
			!isNaN(startPageArg) && startPageArg > 0 && startPageArg <= totalPages
				? startPageArg
				: Math.max(1, START_PAGE);

		console.log(`📋 Starting from page ${startPage} to ${totalPages}`);

		for (let page = startPage; page <= totalPages; page++) {
			// Chestertons uses ?page=N query parameter
			const url = page === 1 ? propertyType.urlBase : `${propertyType.urlBase}?page=${page}`;
			const uniqueKey = `${propertyType.label}_page_${page}`;
			requests.push({
				url,
				uniqueKey,
				userData: { pageNum: page, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

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
