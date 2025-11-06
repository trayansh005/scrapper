// Winkworth scraper using Playwright with Crawlee
// Agent ID: 36
//
// Usage:
// node backend/scraper-agent-36.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, promisePool } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 36;
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
const START_PAGE = 1;

const PROPERTY_TYPES = [
	// {
	// 	// Sales - Hertfordshire and Berkshire
	// 	urlBase: "https://www.winkworth.co.uk/PropertiesSearch/Index",
	// 	params: {
	// 		locationName: "",
	// 		countyName: "Hertfordshire",
	// 		office: "",
	// 		orderBy: "",
	// 		status: "",
	// 		channel: "7f45d0b8-2d58-4403-a338-2f99b676254f",
	// 		viewType: "",
	// 		Location: "berkshire",
	// 		priceFrom: "",
	// 		priceTo: "",
	// 		bedroomsFrom: "",
	// 		bedroomsTo: "",
	// 		propertyType: "all",
	// 		IncludeUnderOffer: "false",
	// 		IncludeSoldLet: "false",
	// 	},
	// 	isRental: false,
	// 	label: "SALES",
	// 	totalRecords: 3721,
	// 	recordsPerPage: 20,
	// },
	{
		// Rentals - London
		urlBase: "https://www.winkworth.co.uk/london/london/properties-to-let",
		params: {
			statusunderoffer: "false",
			propertytype: "all",
		},
		isRental: true,
		label: "LETTINGS",
		totalRecords: 507,
		recordsPerPage: 20,
	},
];

/**
 * Build URL with query parameters
 */
function buildUrl(urlBase, params, pageNum) {
	const url = new URL(urlBase);
	Object.entries(params).forEach(([key, value]) => {
		if (value !== "") {
			url.searchParams.append(key, value);
		}
	});
	if (pageNum > 1) {
		url.searchParams.append("page", pageNum);
	}
	return url.toString();
}

async function scrapeWinkworth() {
	console.log(`\n🚀 Starting Winkworth scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
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

			// Wait for property cards to load
			await page.waitForSelector(".search-results-list__inner", { timeout: 30000 }).catch(() => {});

			// Extract properties from listing page
			const properties = await page.$$eval("article.search-result-property", (cards) => {
				return cards
					.map((card) => {
						try {
							// Get title and link
							const titleLink = card.querySelector(".search-result-property__content-card-link");
							if (!titleLink) return null;

							const link = titleLink.getAttribute("href");
							if (!link) return null;

							let fullLink = link;
							if (!fullLink.startsWith("http")) {
								fullLink = "https://www.winkworth.co.uk" + fullLink;
							}

							// Get title
							const titleEl = card.querySelector(".search-result-property__title");
							const title = titleEl ? titleEl.textContent.trim() : "Unknown";

							// Get price
							const priceEl = card.querySelector(".search-result-property__price");
							let price = null;
							if (priceEl) {
								// Extract first price value (e.g., "£7,400 per week" -> "7400")
								const priceText = priceEl.textContent.trim();
								const match = priceText.match(/£([\d,]+)/);
								if (match) {
									price = match[1].replace(/,/g, "");
								}
							}

							// Get bedrooms
							let bedrooms = null;
							const bedroomsSpecs = card.querySelectorAll(".specs__item");
							if (bedroomsSpecs.length > 0) {
								// First spec is typically bedrooms
								const bedsText = bedroomsSpecs[0].querySelector(".specs__text");
								if (bedsText) {
									bedrooms = bedsText.textContent.trim();
								}
							}

							if (!title || !fullLink || !price) return null;

							return { link: fullLink, title, price, bedrooms };
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

					// Extract coordinates from Google Maps link (similar to agent 14)
					let latitude = null;
					let longitude = null;

					try {
						const coordsFromLink = await detailPage.evaluate(() => {
							// Try to find Google Maps links with coordinates
							const link = Array.from(document.querySelectorAll("a[href*='google.com/maps']")).find(
								(l) => l.getAttribute("href")?.match(/\/(\d+\.\d+),(-?\d+\.\d+)/)
							);
							if (link) {
								const coordMatch = link.getAttribute("href").match(/\/(\d+\.\d+),(-?\d+\.\d+)/);
								return coordMatch ? { lat: coordMatch[1], lng: coordMatch[2] } : null;
							}

							// Fallback: try data attributes
							const propertyMaps = document.querySelector("[data-dc-property-maps]");
							if (propertyMaps) {
								try {
									const jsonStr = propertyMaps
										.getAttribute("data-dc-property-maps")
										.replace(/&quot;/g, '"');
									const data = JSON.parse(jsonStr);
									if (data.lat && data.lng) {
										return { lat: data.lat, lng: data.lng };
									}
								} catch (e) {}
							}

							// Fallback: try street view data
							const streetView = document.querySelector("[data-dc-street-view]");
							if (streetView) {
								try {
									const jsonStr = streetView
										.getAttribute("data-dc-street-view")
										.replace(/&quot;/g, '"');
									const data = JSON.parse(jsonStr);
									if (data.position && data.position.lat && data.position.lng) {
										return { lat: data.position.lat, lng: data.position.lng };
									}
								} catch (e) {}
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
			const url = buildUrl(propertyType.urlBase, propertyType.params, page);
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
		`\n✅ Completed Winkworth - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
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
		await scrapeWinkworth();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
