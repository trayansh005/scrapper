// Sequence Home scraper using Playwright with Crawlee
// Agent ID: 15
// Usage:
// node backend/scraper-agent-15.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool } = require("./db.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 15;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for Sequence Home
const PROPERTY_TYPES = [
	// {
	// 	// Sales
	// 	urlBase: "https://www.sequencehome.co.uk/properties/sales",
	// 	totalPages: 1667, // 16667 items / 10
	// 	recordsPerPage: 10,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		// Rentals
		urlBase: "https://www.sequencehome.co.uk/properties/lettings",
		totalPages: 191, // 1907 items / 10
		recordsPerPage: 10,
		isRental: true,
		label: "RENTALS",
	},
];

async function saveProperty(property, coords, isRental) {
	const tableName = isRental ? "property_for_rent" : "property_for_sale";
	const logo = isRental ? "property_for_rent/logo.png" : "property_for_sale/logo.png";
	const currentTime = new Date();

	try {
		const rawPrice = (property.price || "").toString();
		const numMatch = rawPrice.match(/[0-9][0-9,\.\s]*/);
		const priceClean = numMatch ? numMatch[0].replace(/[^0-9]/g, "") : "";

		// Check if property exists for THIS agent
		const [existingRows] = await promisePool.query(
			`SELECT agent_id FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
			[property.link.trim(), AGENT_ID]
		);

		if (existingRows.length > 0) {
			// Update existing
			await promisePool.query(
				`UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
				[priceClean || null, coords.latitude, coords.longitude, property.link.trim(), AGENT_ID]
			);
			console.log(`✅ Updated: ${property.link}`);
		} else {
			// Check if exists for OTHER agent (to avoid duplicates if that's the logic, but here we just insert for our agent)
			// The original logic checked for other agents but then inserted a NEW record for THIS agent anyway.
			// So effectively, we always insert if it doesn't exist for THIS agent.

			const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
			await promisePool.query(insertQuery, [
				property.title || "",
				AGENT_ID,
				priceClean || null,
				property.bedrooms || null,
				property.link.trim(),
				logo,
				coords.latitude,
				coords.longitude,
				currentTime,
				currentTime,
			]);
			console.log(`🆕 Created: ${property.link}`);
		}
		totalSaved++;
		totalScraped++;
	} catch (dbErr) {
		console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
	}
}

async function scrapeSequenceHome() {
	console.log(`\n🚀 Starting Sequence Home scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1, // Process one page at a time
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(1000);

			// Wait for the specific page container
			const containerSelector = `div[data-page-no="${pageNum}"]`;
			try {
				await page.waitForSelector(containerSelector, { timeout: 15000 });
			} catch (e) {
				console.log(`⚠️ Container ${containerSelector} not found on page ${pageNum}`);
				// Fallback to generic selector check just in case, but warn
				try {
					await page.waitForSelector(".property.list_block[data-property-id]", { timeout: 5000 });
					console.log(
						`⚠️ Found properties but not in expected container. Proceeding with caution.`
					);
				} catch (e2) {
					console.log(`⚠️ No property cards found on page ${pageNum}`);
					return;
				}
			}

			const properties = await page.evaluate((pageNum) => {
				try {
					// Scope to the specific page container to avoid duplicates from other pages (e.g. infinite scroll)
					const containerSelector = `div[data-page-no="${pageNum}"]`;
					const container = document.querySelector(containerSelector);

					// If specific container is missing, check if we have ANY data-page-no divs
					if (!container) {
						const anyPage = document.querySelector("div[data-page-no]");
						if (anyPage) {
							// If there are page containers but not ours, we probably shouldn't scrape the whole document
							// because we'll pick up the wrong page's items.
							return [];
						}
					}

					const root = container || document;

					// Use a more robust selector that relies on the data attribute
					// Added .list_block to avoid selecting map pins or other duplicates
					const items = Array.from(root.querySelectorAll(".property.list_block[data-property-id]"));
					return items
						.map((el) => {
							const linkEl = el.querySelector("a.property-list-link");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: "https://www.sequencehome.co.uk" + href
								: null;

							const title = el.querySelector(".address")?.textContent?.trim() || "";
							const price = el.querySelector(".price-value")?.textContent?.trim() || "";

							let bedrooms = null;
							const roomsEl = el.querySelector(".rooms");
							if (roomsEl) {
								bedrooms = roomsEl.textContent.trim();
								if (!bedrooms && roomsEl.getAttribute("title")) {
									const match = roomsEl.getAttribute("title").match(/(\d+)/);
									if (match) bedrooms = match[1];
								}
							}

							return { link, price, title, bedrooms };
						})
						.filter((p) => p.link);
				} catch (e) {
					return [];
				}
			}, pageNum);

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

			// Process properties sequentially to avoid rate limiting
			for (const property of uniqueProperties) {
				let coords = { latitude: null, longitude: null };

				// Open detail page to get coordinates
				const detailPage = await page.context().newPage();
				try {
					await detailPage.goto(property.link, {
						waitUntil: "domcontentloaded",
						timeout: 30000,
					});

					// Extract lat/long from comments
					const detailCoords = await detailPage.evaluate(() => {
						try {
							const iterator = document.createNodeIterator(
								document.documentElement,
								NodeFilter.SHOW_COMMENT,
								null,
								false
							);
							let node;
							let lat = null;
							let lng = null;

							while ((node = iterator.nextNode())) {
								const content = node.nodeValue;
								if (content.includes("property-latitude")) {
									const match = content.match(/property-latitude:"([0-9.-]+)"/);
									if (match) lat = parseFloat(match[1]);
								}
								if (content.includes("property-longitude")) {
									const match = content.match(/property-longitude:"([0-9.-]+)"/);
									if (match) lng = parseFloat(match[1]);
								}
							}
							return lat !== null && lng !== null ? { lat, lng } : null;
						} catch (e) {
							return null;
						}
					});

					if (detailCoords) {
						coords.latitude = detailCoords.lat;
						coords.longitude = detailCoords.lng;
					}
				} catch (err) {
					// ignore detail page errors
				} finally {
					await detailPage.close();
				}

				await saveProperty(property, coords, isRental);

				// Small delay between properties
				await new Promise((resolve) => setTimeout(resolve, 500));
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Enqueue pages
	for (const propertyType of PROPERTY_TYPES) {
		console.log(`🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			const url = pg === 1 ? `${propertyType.urlBase}/` : `${propertyType.urlBase}/page-${pg}/`;
			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Sequence Home - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

async function updateRemoveStatus(agent_id) {
	try {
		const remove_status = 1;
		await promisePool.query(
			`UPDATE property_for_sale SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
			[remove_status, agent_id]
		);
		console.log(`🧹 Removed old properties for agent ${agent_id}`);
	} catch (error) {
		console.error("Error updating remove status:", error.message);
	}
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
