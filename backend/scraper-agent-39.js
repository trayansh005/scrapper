// John D Wood scraper using Playwright with Crawlee
// Agent ID: 39
//
// Usage:
// node backend/scraper-agent-39.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized,} = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty,} = require("./lib/property-helpers.js");
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 39;
let totalScraped = 0;
let totalSaved = 0;

// Small helper utilities to avoid rate-limiting
const userAgents = [
	// a short list of common UAs (rotate to look more human)
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

// Start page and number of pages to process
const START_PAGE = 1;

const PROPERTY_TYPES = [
	{
		// Sales
		urlBase:
			"https://www.johndwood.co.uk/all-properties-for-sale/status-available/most-recent-first#/",
		isRental: false,
		label: "SALES",
		// Provided: total 887 properties, 30 per page
		totalRecords: 887,
		recordsPerPage: 30,
	},
	{
		// Rent
		urlBase:
			"https://www.johndwood.co.uk/all-properties-to-rent/status-available/most-recent-first#/",
		isRental: true,
		label: "LETTINGS",
		// Provided: total 378 properties, 30 per page
		totalRecords: 378,
		recordsPerPage: 30,
	},
];

async function scrapeJohnDWood() {
	console.log(`\n🚀 Starting John D Wood scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
		// increase retries to tolerate transient 429s/backoffs
		maxRequestRetries: 5,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
			},
		},

		async requestHandler({ page, request, crawler }) {
			// Small randomized delay at the start of each request to avoid bursts
			await sleep(randBetween(600, 1500));
			// set a gentle accept-language h6der for the page
			try {
				await page.setExtraHTTPHeaders({ "accept-language": "en-GB,en;q=0.9" });
			} catch (e) { }
			const { pageNum, isRental, label, isDetailPage, propertyData } = request.userData || {};

			if (isDetailPage) {
				const property = propertyData;
				try {
					console.log(`🔎 Detail - ${property.link}`);

					// Get page HTML to search for coordinates
					const html = await page.content();
					let latitude = null;
					let longitude = null;

					// Try several patterns: HTML comments, loose key:value, JSON-like, or plain lat/long nearby
					try {
						// comment-style or key-style: property-latitude:"51.489597" or property-latitude: 51.489597
						const latMatch =
							html.match(/property-latitude\s*[:\"]\s*\"?([\d.-]+)\"?/i) ||
							html.match(/propertyLatitude\s*[:=]\s*\"?([\d.-]+)\"?/i);
						const lonMatch =
							html.match(/property-longitude\s*[:\"]\s*\"?([\d.-]+)\"?/i) ||
							html.match(/propertyLongitude\s*[:=]\s*\"?([\d.-]+)\"?/i);
						if (latMatch) latitude = parseFloat(latMatch[1]);
						if (lonMatch) longitude = parseFloat(lonMatch[1]);

						// Generic latitude/longitude keys in JSON or scripts
						if (!latitude || !longitude) {
							const latAny = html.match(
								/(?:\"latitude\"|\"lat\"|latitude\s*:)\s*[:=]?\s*\"?([\d]{1,3}\.\d+)\"?/i
							);
							const lonAny = html.match(
								/(?:\"longitude\"|\"lng\"|\"lon\"|longitude\s*:)\s*[:=]?\s*\"?(-?\d{1,3}\.\d+)\"?/i
							);
							if (latAny && lonAny) {
								latitude = parseFloat(latAny[1]);
								longitude = parseFloat(lonAny[1]);
							}
						}
					} catch (e) { }

					// Try data-location attribute
					if (!latitude || !longitude) {
						try {
							const dataLocation = await page
								.$eval(".google-map-embed", (el) => el.getAttribute("data-location"))
								.catch(() => null);
							if (dataLocation) {
								const parts = dataLocation.split(",").map((s) => s.trim());
								if (parts.length >= 2) {
									latitude = parseFloat(parts[0]);
									longitude = parseFloat(parts[1]);
								}
							}
						} catch (e) { }
					}

					// Fallback to JSON-LD
					if (!latitude || !longitude) {
						try {
							const jsonLdTags = await page.$$eval('script[type="application/ld+json"]', (tags) =>
								tags.map((t) => t.textContent)
							);
							for (const s of jsonLdTags) {
								try {
									const parsed = JSON.parse(s);
									const items = Array.isArray(parsed) ? parsed : [parsed];
									for (const item of items) {
										if (!item) continue;
										if (item.geo && item.geo.latitude && item.geo.longitude) {
											latitude = parseFloat(item.geo.latitude);
											longitude = parseFloat(item.geo.longitude);
											break;
										}
										if (item.latitude && item.longitude) {
											latitude = parseFloat(item.latitude);
											longitude = parseFloat(item.longitude);
											break;
										}
									}
									if (latitude && longitude) break;
								} catch (e) { }
							}
						} catch (e) { }
					}

					const tableName = isRental ? "property_for_rent" : "property_for_sale";

					// DB checks
					const [rows] = await promisePool.query(
						`SELECT agent_id FROM ${tableName} WHERE property_url = ?`,
						[p.link.trim()]
					);

					const existingForThisAgent = rows.find(r => r.agent_id === AGENT_ID);
					const existsForOtherAgent = rows.find(r => r.agent_id !== AGENT_ID);

					if (existingRows.length > 0) {
						await promisePool.query(
							`UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
							[property.price, latitude, longitude, property.link.trim(), AGENT_ID]
						);
						console.log(
							`✅ Updated: ${property.link.substring(0, 60)}... | Price: £${property.price
							} | Coords: ${latitude}, ${longitude}`
						);
					} else if (otherAgentRows.length > 0) {
						const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
						const logo = isRental ? "property_for_rent/logo.png" : "property_for_sale/logo.png";
						const currentTime = new Date();
						await promisePool.query(insertQuery, [
							property.title,
							AGENT_ID,
							property.price,
							property.bedrooms,
							property.link.trim(),
							logo,
							latitude,
							longitude,
							currentTime,
							currentTime,
						]);
						console.log(
							`✅ Created: ${property.link.substring(0, 60)}... | Price: £${property.price
							} | Coords: ${latitude}, ${longitude}`
						);
					} else {
						const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
						const logo = isRental ? "property_for_rent/logo.png" : "property_for_sale/logo.png";
						const currentTime = new Date();
						await promisePool.query(insertQuery, [
							property.title,
							AGENT_ID,
							property.price,
							property.bedrooms,
							property.link.trim(),
							logo,
							latitude,
							longitude,
							currentTime,
							currentTime,
						]);
						console.log(
							`✅ Created: ${property.link.substring(0, 60)}... | Price: £${property.price
							} | Coords: ${latitude}, ${longitude}`
						);
					}

					totalSaved++;
					totalScraped++;

					if (latitude && longitude) {
						console.log(`✅ ${property.title} - £${property.price} - ${latitude}, ${longitude}`);
					} else {
						console.log(`✅ ${property.title} - £${property.price} - No coords`);
					}
				} catch (err) {
					console.error(`❌ Error processing ${property.link}: ${err.message}`);
				}

				return;
			}

			// Listing page
			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(2000);
			await page.waitForSelector(".results-page", { timeout: 30000 }).catch(() => {
				console.log(`⚠️ No properties found on page ${pageNum}`);
			});

			// Extract properties from results-page
			const properties = await page.$$eval(".card--list .card", (cards) => {
				// Define formatPrice inside the evaluation context
				const formatPrice = (raw) => {
					if (!raw) return null;
					const digits = raw.replace(/[^0-9]/g, "");
					if (!digits) return null;
					return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
				};

				const results = [];
				cards.forEach((c) => {
					try {
						const linkEl = c.querySelector("a.card__link");
						if (!linkEl) return;
						let href = linkEl.getAttribute("href");
						if (!href) return;
						if (!href.startsWith("http")) href = "https://www.johndwood.co.uk" + href;

						// Price
						let price = null;
						const priceEl = c.querySelector(".card__heading");
						if (priceEl) {
							const txt = priceEl.textContent || "";
							price = txt;
						}

						// Title / short description
						let title = "";
						const titleEl = c.querySelector(".card__text-title");
						if (titleEl) title = titleEl.textContent.trim();
						if (!title) {
							const linkText = linkEl.textContent || "";
							title = linkText.replace(/\s+/g, " ").trim();
						}

						// Bedrooms - first spec-list number
						let bedrooms = null;
						const spec = c.querySelector(
							".card-content__spec-list .card-content__spec-list-item .card-content__spec-list-number"
						);
						if (spec) {
							const txt = spec.textContent || "";
							const m = txt.match(/(\d+)/);
							if (m) bedrooms = m[1];
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

			const chunkSize = 3;
			for (const p of properties) {

				if (!p.link) continue;

				// Skip sold properties
				if (isSoldProperty(p.title || "")) continue;

				const price = formatPriceUk(p.price);
				if (!price) {
					console.log(`⏭ Skipping property (no valid price): ${p.link}`);
					continue;
				}

				const result = await updatePriceByPropertyURLOptimized(
					p.link,
					price,
					p.title,
					p.bedrooms,
					AGENT_ID,
					isRental
				);

				if (result.updated) {
					totalSaved++;
				}

				// Only open detail page if property is NEW
				if (!result.isExisting && !result.error) {

					const newPage = await page.context().newPage();

					try {
						await newPage.route("**/*", (route) => {
							const type = route.request().resourceType();
							if (["image", "font", "stylesheet", "media"].includes(type)) {
								route.abort();
							} else {
								route.continue();
							}
						});

						await newPage.goto(p.link, {
							waitUntil: "domcontentloaded",
							timeout: 60000,
						});

						await newPage.waitForTimeout(1200);

						const html = await newPage.content();
						const coords = await extractCoordinatesFromHTML(html);

						await updatePriceByPropertyURL(
							p.link.trim(),
							price,
							p.title,
							p.bedrooms || null,
							AGENT_ID,
							isRental,
							coords?.latitude || null,
							coords?.longitude || null
						);

						totalSaved++;
						totalScraped++;

						console.log(`✅ Created: ${p.title} - £${price}`);

					} catch (err) {
						console.error(`❌ Detail error ${p.link}:`, err.message);
					} finally {
						await newPage.close();
					}
				}

				await sleep(500); // rate-limit protection
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Queue pages per property type (use totalRecords/recordsPerPage to compute pages)
	for (const propertyType of PROPERTY_TYPES) {
		const totalPages =
			propertyType.totalRecords && propertyType.recordsPerPage
				? Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage)
				: 1;
		console.log(`🏠 Queueing ${propertyType.label} pages: ${totalPages} pages`);
		const requests = [];
		const startPage = Math.max(1, START_PAGE);
		for (let page = startPage; page <= totalPages; page++) {
			// The site uses /page-N/ in the path, not ?page=N
			// Base URL ends with #/, so insert /page-N/ before the hash
			const baseWithoutHash = propertyType.urlBase.replace(/#\/$/, "");
			const url = page === 1 ? propertyType.urlBase : `${baseWithoutHash}/page-${page}#/`;
			// Provide a uniqueKey to avoid deduplication issues with fragment/hash URLs
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
		`\n✅ Completed John D Wood - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeJohnDWood();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
