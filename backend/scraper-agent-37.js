// Chase Evans scraper using Playwright with Crawlee
// Agent ID: 37
//
// Usage:
// node backend/scraper-agent-37.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 37;
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

function formatPrice(raw) {
	if (!raw) return null;
	const digits = raw.replace(/[^0-9]/g, "");
	if (!digits) return null;
	return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

// Start page
const START_PAGE = 1;

const PROPERTY_TYPES = [
	{
		// Sales
		urlBase: "https://www.chaseevans.com/property/for-sale/in-london/exclude-sale-agreed/",
		isRental: false,
		label: "SALES",
		totalRecords: 263,
		recordsPerPage: 18,
	},
	// {
	// 	// Rent
	// 	urlBase: "https://www.chaseevans.com/property/to-rent/in-london/exclude-let-agreed/",
	// 	isRental: true,
	// 	label: "LETTINGS",
	// 	totalRecords: 118,
	// 	recordsPerPage: 18,
	// },
];

async function scrapeChaseEvans() {
	console.log(`\n🚀 Starting Chase Evans scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 5,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: false,
			},
		},

		async requestHandler({ page, request }) {
			// Small randomized delay to avoid bursts
			await sleep(randBetween(1200, 3200));
			try {
				await page.setExtraHTTPHeaders({ "accept-language": "en-GB,en;q=0.9" });
			} catch (e) {}

			const { pageNum, isRental, label } = request.userData || {};

			// Listing page
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(2000);
			await page.waitForSelector(".sales-wrapper", { timeout: 30000 }).catch(() => {
				console.log(`⚠️ No properties found on page ${pageNum}`);
			});

			// Extract properties from .sales-wrapper
			const properties = await page.$$eval(".sales-wrap", (cards) => {
				const results = [];
				cards.forEach((c) => {
					try {
						// Link - find main link in the card
						const linkEl = c.querySelector("a[href]");
						if (!linkEl) return;
						let href = linkEl.getAttribute("href");
						if (!href) return;
						if (!href.startsWith("http")) href = "https://www.chaseevans.com" + href;

						// Price
						let price = null;
						const priceEl = c.querySelector(".highlight-text");
						if (priceEl) {
							const txt = priceEl.textContent || "";
							price = formatPrice(txt);
						}

						// Title - from h3
						let title = "";
						const titleEl = c.querySelector("h3");
						if (titleEl) title = titleEl.textContent.trim();

						// Bedrooms - look for bed icon span
						let bedrooms = null;
						const bedElems = c.querySelectorAll(".icon-bed");
						if (bedElems.length > 0) {
							const bedSpan = bedElems[0].nextElementSibling;
							if (bedSpan) bedrooms = bedSpan.textContent.trim();
						}

						if (href && title && price) results.push({ link: href, title, price, bedrooms });
					} catch (e) {
						// skip
					}
				});
				return results;
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			if (properties.length === 0) return;

			// Process detail pages
			const chunkSize = 1;
			for (let start = 0; start < properties.length; start += chunkSize) {
				const chunk = properties.slice(start, start + chunkSize);
				await Promise.all(
					chunk.map(async (p) => {
						const detailPage = await page.context().newPage();
						try {
							// rotate UA and add delay
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
							if (resp && resp.status && resp.status() === 429) {
								console.warn(`⚠️ 429 on ${p.link} — backing off`);
								await sleep(60000);
								throw new Error("429");
							}
							await detailPage.waitForTimeout(1000);

							// Extract coordinates from Street View Google Maps URL
							let latitude = null;
							let longitude = null;

							try {
								console.log(`🔍 DEBUG: Attempting to extract coordinates for ${p.title}`);

								// Method 1: Look for Google Maps iframe with lat/lng parameters
								console.log(`🔍 DEBUG: Checking for iframe with location data...`);
								const coordsFromIframe = await detailPage.evaluate(() => {
									const iframes = document.querySelectorAll("iframe");
									console.log(`Found ${iframes.length} iframes`);
									for (const iframe of iframes) {
										const src = iframe.getAttribute("src") || iframe.getAttribute("data-src");
										if (src) {
											console.log(`Iframe src: ${src.substring(0, 100)}`);
											// Look for lat and lng parameters
											const latMatch = src.match(/[&?]lat=([\d.-]+)/);
											const lngMatch = src.match(/[&?]lng=([\d.-]+)/);
											if (latMatch && lngMatch) {
												return {
													lat: latMatch[1],
													lng: lngMatch[1],
												};
											}
										}
									}
									return null;
								});

								if (coordsFromIframe && coordsFromIframe.lat && coordsFromIframe.lng) {
									latitude = parseFloat(coordsFromIframe.lat);
									longitude = parseFloat(coordsFromIframe.lng);
									console.log(
										`✅ DEBUG: Extracted coords from iframe URL: ${latitude}, ${longitude}`
									);
								} else {
									console.log(`⚠️ DEBUG: No coords found in iframe URL parameters`);
								}
							} catch (e) {
								console.error(`❌ DEBUG: Error extracting coords: ${e.message}`);
							}

							const tableName = isRental ? "property_for_rent" : "property_for_sale";

							const [existingRows] = await promisePool.query(
								`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
								[p.link.trim(), AGENT_ID]
							);
							const [otherAgentRows] = await promisePool.query(
								`SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id != ?`,
								[p.link.trim(), AGENT_ID]
							);

							if (existingRows.length > 0) {
								await promisePool.query(
									`UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
									[p.price, latitude, longitude, p.link.trim(), AGENT_ID]
								);
								console.log(
									`✅ Updated: ${p.link.substring(0, 60)}... | Price: £${
										p.price
									} | Coords: ${latitude}, ${longitude}`
								);
							} else if (otherAgentRows.length > 0) {
								const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
								const logo = isRental ? "property_for_rent/logo.png" : "property_for_sale/logo.png";
								const currentTime = new Date();
								await promisePool.query(insertQuery, [
									p.title,
									AGENT_ID,
									p.price,
									p.bedrooms,
									p.link.trim(),
									logo,
									latitude,
									longitude,
									currentTime,
									currentTime,
								]);
								console.log(
									`✅ Created: ${p.link.substring(0, 60)}... | Price: £${
										p.price
									} | Coords: ${latitude}, ${longitude}`
								);
							} else {
								const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
								const logo = isRental ? "property_for_rent/logo.png" : "property_for_sale/logo.png";
								const currentTime = new Date();
								await promisePool.query(insertQuery, [
									p.title,
									AGENT_ID,
									p.price,
									p.bedrooms,
									p.link.trim(),
									logo,
									latitude,
									longitude,
									currentTime,
									currentTime,
								]);
								console.log(
									`✅ Created: ${p.link.substring(0, 60)}... | Price: £${
										p.price
									} | Coords: ${latitude}, ${longitude}`
								);
							}

							totalSaved++;
							totalScraped++;

							if (latitude && longitude) {
								console.log(`✅ ${p.title} - £${p.price} - ${latitude}, ${longitude}`);
							} else {
								console.log(`✅ ${p.title} - £${p.price} - No coords`);
							}
						} catch (err) {
							console.error(`❌ Error processing ${p.link}: ${err.message}`);
						} finally {
							await detailPage.close();
						}
					})
				);

				await new Promise((r) => setTimeout(r, 500));
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
			// Chase Evans uses /page-N/ path format
			const baseWithoutTrailingSlash = propertyType.urlBase.replace(/\/$/, "");
			const url = page === 1 ? propertyType.urlBase : `${baseWithoutTrailingSlash}/page-${page}/`;
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
		`\n✅ Completed Chase Evans - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeChaseEvans();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
