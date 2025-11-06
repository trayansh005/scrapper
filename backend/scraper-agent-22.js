// Allsop scraper using Playwright with Crawlee
// Agent ID: 22
//
// Usage:
// node backend/scraper-agent-22.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 22;
let totalScraped = 0;
let totalSaved = 0;

// Extract coordinates from script tag or Google Maps link
function extractCoordinatesFromHTML(html) {
	const latMatch = html.match(/["']?lat(?:itude)?["']?\s*:\s*([0-9.-]+)/i);
	const lngMatch = html.match(/["']?lng|lon(?:gitude)?["']?\s*:\s*([0-9.-]+)/i);

	if (latMatch && lngMatch) {
		return {
			latitude: parseFloat(latMatch[1]),
			longitude: parseFloat(lngMatch[1]),
		};
	}

	return { latitude: null, longitude: null };
}

async function scrapeAllsop() {
	console.log(`\n🚀 Starting Allsop scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 2, // Process 2 pages in parallel
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launchOptions: {
				headless: true,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request, crawler }) {
			const { isDetailPage, propertyData, pageNum } = request.userData;

			if (isDetailPage) {
				// Processing detail page
				console.log(`📍 Detail: ${propertyData.title}`);

				await page.waitForTimeout(1000);

				// Get the actual URL
				const actualLink = page.url();
				propertyData.link = actualLink;

				let coords = { latitude: null, longitude: null };

				// Try to click the Street View tab and extract coordinates
				try {
					const streetViewTab = await page.$('a[data-tab="street"]');

					if (streetViewTab) {
						await streetViewTab.click();
						await page.waitForTimeout(3000);

						// Try multiple selectors for the Google Maps link
						let googleMapsLink = await page.$('.gm-iv-address-link a[href*="maps.google.com"]');
						if (!googleMapsLink) {
							googleMapsLink = await page.$('a[href*="maps.google.com/maps/@"]');
						}

						if (googleMapsLink) {
							const href = await googleMapsLink.getAttribute("href");
							const coordMatch = href.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/);
							if (coordMatch) {
								coords.latitude = parseFloat(coordMatch[1]);
								coords.longitude = parseFloat(coordMatch[2]);
							}
						}
					}
				} catch (err) {
					const htmlContent = await page.content();
					coords = extractCoordinatesFromHTML(htmlContent);
				}

				// Save to database
				const tableName = "property_for_sale";

				const [existingRows] = await promisePool.query(
					`SELECT agent_id FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
					[propertyData.link.trim(), AGENT_ID]
				);

				const [otherAgentRows] = await promisePool.query(
					`SELECT agent_id FROM ${tableName} WHERE property_url = ? AND agent_id != ?`,
					[propertyData.link.trim(), AGENT_ID]
				);

				if (existingRows.length > 0) {
					await promisePool.query(
						`UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
						[
							propertyData.price,
							coords.latitude,
							coords.longitude,
							propertyData.link.trim(),
							AGENT_ID,
						]
					);
					console.log(
						`✅ Updated: ${propertyData.title} - £${propertyData.price} - ${coords.latitude}, ${coords.longitude}`
					);
				} else if (otherAgentRows.length > 0) {
					const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
					const logo = "property_for_sale/logo.png";
					const currentTime = new Date();

					await promisePool.query(insertQuery, [
						propertyData.title,
						AGENT_ID,
						propertyData.price,
						propertyData.bedrooms,
						propertyData.link.trim(),
						logo,
						coords.latitude,
						coords.longitude,
						currentTime,
						currentTime,
					]);
					console.log(
						`✅ Created: ${propertyData.title} - £${propertyData.price} - ${coords.latitude}, ${coords.longitude}`
					);
				} else {
					const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
					const logo = "property_for_sale/logo.png";
					const currentTime = new Date();

					await promisePool.query(insertQuery, [
						propertyData.title,
						AGENT_ID,
						propertyData.price,
						propertyData.bedrooms,
						propertyData.link.trim(),
						logo,
						coords.latitude,
						coords.longitude,
						currentTime,
						currentTime,
					]);
					console.log(
						`✅ Created: ${propertyData.title} - £${propertyData.price} - ${coords.latitude}, ${coords.longitude}`
					);
				}

				totalSaved++;
				totalScraped++;
			} else {
				// Processing listing page
				console.log(`📋 Page ${pageNum} - ${request.url}`);

				await page.waitForTimeout(2000);
				await page.waitForSelector(".col-sm-6", { timeout: 30000 }).catch(() => {});

				// Extract all properties and click on each .__lot_image to get detail URLs
				const propertyCards = await page.$$(".col-sm-6");
				const detailRequests = [];

				for (let cardIndex = 0; cardIndex < propertyCards.length; cardIndex++) {
					try {
						const column = propertyCards[cardIndex];

						// Extract property info from the card
						const lot = await column.$(".__lot_container");
						if (!lot) continue;

						const locationEl = await lot.$(".__location");
						const title = locationEl
							? await locationEl.evaluate((el) => el.textContent.trim())
							: null;

						const priceEl = await lot.$(".__lot_price_grid");
						const priceText = priceEl ? await priceEl.evaluate((el) => el.textContent.trim()) : "";

						if (
							priceText === "Withdrawn" ||
							priceText === "Sold Prior" ||
							priceText === "Sold After"
						) {
							continue;
						}

						const priceMatch = priceText.match(/£([\d,]+)/);
						const price = priceMatch ? priceMatch[1].replace(/,/g, "") : null;

						const bylineEl = await lot.$(".__byline span");
						const description = bylineEl
							? await bylineEl.evaluate((el) => el.textContent.trim())
							: "";
						const bedroomsMatch = description.match(/(\d+)\s*bed/i);
						const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

						const linkEl = await lot.$(".__lot_image a");
						const titleAttr = linkEl ? await linkEl.evaluate((el) => el.getAttribute("title")) : "";
						const lotMatch = titleAttr.match(/LOT\s*(\d+)/i);
						const lotNumber = lotMatch ? lotMatch[1] : null;

						if (!price || !title) continue;

						const propertyData = {
							title: lotNumber ? `LOT ${lotNumber} - ${title}` : title,
							price,
							bedrooms,
						};

						// Click on .__lot_image to navigate to detail page
						const lotImage = await column.$(".__lot_image");
						if (lotImage) {
							await lotImage.click();
							await page.waitForLoadState("networkidle").catch(() => {});

							// Get the detail page URL
							const detailUrl = page.url();

							// Add to requests queue
							detailRequests.push({
								url: detailUrl,
								userData: {
									isDetailPage: true,
									propertyData: propertyData,
								},
							});

							// Go back to the listing page
							await page.goBack();
							await page.waitForTimeout(500);

							// Re-fetch the property cards since DOM might have changed
							propertyCards.splice(0, propertyCards.length, ...(await page.$$(".col-sm-6")));
						}
					} catch (err) {
						console.warn(`⚠️ Error processing property card ${cardIndex + 1}: ${err.message}`);
					}
				}

				console.log(
					`🔗 Found and processed ${detailRequests.length} properties on page ${pageNum}`
				);
				await crawler.addRequests(detailRequests);
			}
		},

		failedRequestHandler({ request }) {
			console.error(`❌ Failed: ${request.url}`);
		},
	});

	// Add all listing pages
	const requests = [];
	const totalPages = 10;
	for (let page = 1; page <= totalPages; page++) {
		requests.push({
			url: `https://www.allsop.co.uk/property-search?auction_id=f76e435a-46a5-11f0-ba8f-0242ac110002&page=${page}`,
			userData: { isDetailPage: false, pageNum: page },
		});
	}

	await crawler.addRequests(requests);
	await crawler.run();

	console.log(`\n✅ Completed Allsop - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

// Local implementation of updateRemoveStatus
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

// Main execution
(async () => {
	try {
		await scrapeAllsop();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
