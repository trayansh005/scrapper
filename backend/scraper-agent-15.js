// Sequence Home scraper using Playwright with Crawlee
// Agent ID: 15
//
// Usage:
// node backend/scraper-agent-15.js [startPage]
// Example: node backend/scraper-agent-15.js 10 (starts from page 10)

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { logMemoryUsage } = require("./lib/scraper-utils.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { EventEmitter } = require("events");

// Increase max listeners to prevent memory leak warnings
EventEmitter.defaultMaxListeners = 100;

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 15;
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

function normalizeBedrooms(value) {
	if (!value) return null;
	const match = String(value).match(/\d+/);
	return match ? match[0] : null;
}

// Start page
const START_PAGE = 1;

// Configuration for Sequence Home
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.sequencehome.co.uk/properties/sales",
		isRental: false,
		label: "SALES",
		totalRecords: 16362,
		recordsPerPage: 10,
	},
	{
		urlBase: "https://www.sequencehome.co.uk/properties/lettings",
		isRental: true,
		label: "LETTINGS",
		totalRecords: 1907,
		recordsPerPage: 10,
	},
];

async function scrapeSequenceHome() {
	console.log(`\n🚀 Starting Sequence Home scraper (Agent ${AGENT_ID})...\n`);
	logMemoryUsage("START");

	// Create a unified Playwright crawler that handles both listing and detail pages
	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 120,
		blockedStatusCodes: [429],

		launchContext: {
			launchOptions: {
				headless: true,
			},
		},

		preNavigationHooks: [
			async ({ page }) => {
				await page.route("**/*", (route) => {
					const resourceType = route.request().resourceType();
					if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
						route.abort();
					} else {
						route.continue();
					}
				});
			},
		],

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label, property, isDetailPage } = request.userData || {};

			const scrapeDetailPage = async (browserContext, prop) => {
				await sleep(1000);
				const detailPage = await browserContext.newPage();
				try {
					try {
						const ua = userAgents[Math.floor(Math.random() * userAgents.length)];
						await detailPage.setUserAgent(ua);
						await detailPage.setExtraHTTPHeaders({ "accept-language": "en-GB,en;q=0.9" });
					} catch (e) {}

					await sleep(randBetween(800, 1800));
					const resp = await detailPage.goto(prop.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});

					if (resp?.status?.() === 403) {
						console.warn(`⚠️ 403 on ${prop.link} — backing off`);
						await sleep(45000);
						throw new Error("403");
					}

					if (resp?.status?.() === 429) {
						console.warn(`⚠️ 429 on ${prop.link} — backing off`);
						await sleep(60000);
						throw new Error("429");
					}

					await detailPage.waitForTimeout(1000);
					const htmlContent = await detailPage.content();

					await processPropertyWithCoordinates(
						prop.link,
						prop.price,
						prop.title,
						prop.bedrooms || null,
						AGENT_ID,
						prop.isRental,
						htmlContent,
					);

					totalScraped++;
					totalSaved++;

					const coords = await extractCoordinatesFromHTML(htmlContent);
					const coordsStr =
						coords.latitude && coords.longitude
							? `${coords.latitude}, ${coords.longitude}`
							: "No coords";
					console.log(`✅ ${prop.title} - ${formatPrice(prop.price)} - ${coordsStr}`);
				} finally {
					await detailPage.close();
				}
			};

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

				if (resp?.status?.() === 403) {
					console.warn(`⚠️ 403 on ${request.url} — backing off`);
					await sleep(45000);
					throw new Error("403");
				}

				if (resp?.status?.() === 429) {
					console.warn(`⚠️ 429 on ${request.url} — backing off`);
					await sleep(60000);
					throw new Error("429");
				}

				await page.waitForTimeout(1000);

				// Extract coordinates using helper function
				const htmlContent = await page.content();
				const coords = await extractCoordinatesFromHTML(htmlContent);

				// Use helper to process property with coordinates
				await processPropertyWithCoordinates(
					property.link,
					property.price,
					property.title,
					property.bedrooms || null,
					AGENT_ID,
					property.isRental,
					htmlContent,
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

			if (resp?.status?.() === 403) {
				console.warn(`⚠️ 403 on ${request.url} — backing off`);
				await sleep(45000);
				throw new Error("403");
			}

			if (resp?.status?.() === 429) {
				console.warn(`⚠️ 429 on ${request.url} — backing off`);
				await sleep(60000);
				throw new Error("429");
			}

			// Wait for property cards to load
			await page.waitForTimeout(2000);

			// Wait for the specific page container (page 1 often lacks data-page-no)
			const containerSelector = `div[data-page-no="${pageNum}"]`;
			if (pageNum === 1) {
				try {
					await page.waitForSelector(".property.list_block[data-property-id]", { timeout: 15000 });
				} catch (e) {
					console.log(`⚠️ No property cards found on page ${pageNum}`);
					return;
				}
			} else {
				try {
					await page.waitForSelector(containerSelector, { timeout: 15000 });
				} catch (e) {
					console.log(`⚠️ Container ${containerSelector} not found on page ${pageNum}`);
					// Fallback to generic selector check just in case, but warn
					try {
						await page.waitForSelector(".property.list_block[data-property-id]", {
							timeout: 5000,
						});
						console.log(
							`⚠️ Found properties but not in expected container. Proceeding with caution.`,
						);
					} catch (e2) {
						console.log(`⚠️ No property cards found on page ${pageNum}`);
						return;
					}
				}
			}

			// Extract properties using Playwright
			const htmlContent = await page.content();
			const $ = cheerio.load(htmlContent);

			const properties = [];

			// Scope to the specific page container to avoid duplicates
			const containerSelector2 = `div[data-page-no="${pageNum}"]`;
			let rootSelector = containerSelector2;
			if ($(containerSelector2).length === 0) {
				// Fallback to document if no specific container
				rootSelector = "";
			}

			const selector = rootSelector
				? `${rootSelector} .property.list_block[data-property-id]`
				: ".property.list_block[data-property-id]";

			$(selector).each((index, element) => {
				try {
					const $card = $(element);
					const linkEl = $card.find("a.property-list-link").first();
					let href = linkEl.attr("href");
					if (!href) return;

					const link = href.startsWith("http") ? href : "https://www.sequencehome.co.uk" + href;

					const title = $card.find(".address")?.text()?.trim() || "";
					const priceText = $card.find(".price-value")?.text()?.trim() || "";
					const cardText = $card.text() || "";

					if (isSoldProperty(cardText) || isSoldProperty(priceText)) return;

					// Parse price
					const priceMatch = priceText.match(/[0-9][0-9,\.\s]*/);
					const priceClean = priceMatch ? priceMatch[0].replace(/[^0-9]/g, "") : "";
					const price = priceClean ? parseInt(priceClean) : null;

					let bedrooms = null;
					const roomsEl = $card.find(".rooms");
					if (roomsEl.length) {
						bedrooms = roomsEl.text().trim();
						if (!bedrooms && roomsEl.attr("title")) {
							bedrooms = roomsEl.attr("title");
						}
					}
					bedrooms = normalizeBedrooms(bedrooms);

					if (link && title && price) {
						properties.push({
							link,
							title,
							price,
							bedrooms,
						});
					}
				} catch (e) {
					// Skip this card
				}
			});

			// Deduplicate properties based on link
			const uniqueProperties = [];
			const seenLinks = new Set();
			for (const p of properties) {
				if (!seenLinks.has(p.link)) {
					seenLinks.add(p.link);
					uniqueProperties.push(p);
				}
			}

			console.log(`🔗 Found ${uniqueProperties.length} properties on page ${pageNum}`);

			// Process properties sequentially - update prices and scrape details for new ones
			for (const p of uniqueProperties) {
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
					await scrapeDetailPage(page.context(), { ...p, isRental });
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

	// Process pages per property type
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

		// Queue all pages for this property type
		const requests = [];
		for (let page = startPage; page <= totalPages; page++) {
			// Sequence Home uses /page-N/ format
			const url = page === 1 ? `${propertyType.urlBase}` : `${propertyType.urlBase}/page-${page}/`;
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

		// Add all pages and run - they'll process sequentially due to maxConcurrency: 1
		// Detail pages are added during listing processing and handled immediately
		await crawler.addRequests(requests);
		await crawler.run();

		logMemoryUsage(`After ${propertyType.label}`);
	}

	console.log(
		`\n✅ Completed Sequence Home - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`,
	);
	logMemoryUsage("END");
}

(async () => {
	try {
		await scrapeSequenceHome();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
