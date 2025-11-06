// Chestertons scraper using Playwright with Crawlee
// Agent ID: 14
//
// Usage:
// node backend/scraper-agent-14.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, promisePool } = require("./db.js");

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

// Start page
const START_PAGE = 120;

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.chestertons.co.uk/properties/sales/status-available",
		isRental: false,
		label: "SALES",
		totalRecords: 1747,
		recordsPerPage: 12,
	},
	// {
	// 	urlBase: "https://www.chestertons.co.uk/properties/lettings/status-available",
	// 	isRental: true,
	// 	label: "LETTINGS",
	// 	totalRecords: 1132,
	// 	recordsPerPage: 12,
	// },
];

async function scrapeChestertons() {
	console.log(`\n🚀 Starting Chestertons scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 5,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
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
								(svg) => svg.querySelector("title")?.textContent === "Bedrooms"
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

					// Extract coordinates from Google Maps link
					let latitude = null;
					let longitude = null;

					try {
						const coordsFromLink = await detailPage.evaluate(() => {
							const link = Array.from(
								document.querySelectorAll("a[href*='google.com/maps/dir']")
							).find((l) => l.getAttribute("href")?.match(/\/(\d+\.\d+),(-?\d+\.\d+)/));
							if (link) {
								const coordMatch = link.getAttribute("href").match(/\/(\d+\.\d+),(-?\d+\.\d+)/);
								return coordMatch ? { lat: coordMatch[1], lng: coordMatch[2] } : null;
							}
							return null;
						});

						if (coordsFromLink?.lat && coordsFromLink?.lng) {
							latitude = parseFloat(coordsFromLink.lat);
							longitude = parseFloat(coordsFromLink.lng);
						}
					} catch (e) {
						console.error(`❌ Coords extraction error: ${e.message}`);
					}

					await updatePriceByPropertyURL(
						p.link,
						p.price,
						p.title,
						p.bedrooms,
						AGENT_ID,
						isRental,
						latitude,
						longitude
					);
					totalScraped++;
					totalSaved++;

					const coordsStr = latitude && longitude ? `${latitude}, ${longitude}` : "No coords";
					console.log(`✅ ${p.title} - £${p.price} - ${coordsStr}`);
				} catch (err) {
					console.error(`❌ Error processing ${p.link}: ${err.message}`);
				} finally {
					await detailPage.close();
				}
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Queue pages per property type
	for (const propertyType of PROPERTY_TYPES) {
		const totalPages =
			propertyType.totalRecords && propertyType.recordsPerPage
				? Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage)
				: 1;
		console.log(`🏠 Queueing ${propertyType.label} pages: ${totalPages} pages`);
		const requests = [];
		const startPage = Math.max(1, START_PAGE);
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
	}

	console.log(
		`\n✅ Completed Chestertons - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

async function updateRemoveStatus(agent_id) {
	try {
		const remove_status = 1;
		await promisePool.query(
			`UPDATE property_for_sale SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
			[remove_status, agent_id]
		);
		await promisePool.query(
			`UPDATE property_for_rent SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
			[remove_status, agent_id]
		);
		console.log(`🧹 Removed old properties for agent ${agent_id}`);
	} catch (error) {
		console.error("Error updating remove status:", error.message);
	}
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
