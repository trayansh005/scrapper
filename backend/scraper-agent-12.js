// Purplebricks scraper using Playwright with Crawlee
// Agent ID: 12
// Usage:
// node backend/scraper-agent-12.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 12;
let totalScraped = 0;
let totalSaved = 0;

// Configure the Purplebricks searches (updated per user counts)
const PROPERTY_TYPES = [
	// {
	// 	// Greater London / London sales (716 properties -> 72 pages)
	// 	urlBase:
	// 		"https://www.purplebricks.co.uk/search/property-for-sale/greater-london/london?page=1&sortBy=2&betasearch=true&latitude=51.5072178&longitude=-0.1275862&location=london&searchRadius=2&searchType=ForSale&soldOrLet=false",
	// 	totalPages: 72,
	// 	isRental: false,
	// 	label: "LONDON_SALES",
	// },
	// {
	// 	// West Midlands / Birmingham search (136 properties -> 14 pages)
	// 	urlBase:
	// 		"https://www.purplebricks.co.uk/search/property-for-sale/west-midlands/birmingham?page=1&sortBy=2&betasearch=true&latitude=52.4822694&longitude=-1.8900078&location=birmingham&searchRadius=2&searchType=ForSale&soldOrLet=false",
	// 	totalPages: 14,
	// 	isRental: false,
	// 	label: "BIRMINGHAM_SALES",
	// },
	// {
	// 	// Greater Manchester / Manchester search (202 properties -> 21 pages)
	// 	urlBase:
	// 		"https://www.purplebricks.co.uk/search/property-for-sale/greater-manchester/manchester?page=1&sortBy=2&betasearch=true&latitude=53.4807593&longitude=-2.2426305&location=manchester&searchRadius=2&searchType=ForSale&soldOrLet=false",
	// 	totalPages: 21,
	// 	isRental: false,
	// 	label: "MANCHESTER_SALES",
	// },
	{
		// Rents (user indicated 11 properties -> 2 pages). Using London rents URL by default.
		urlBase:
			"https://www.purplebricks.co.uk/search/property-to-rent/greater-london/london?page=1&sortBy=2&betasearch=true&latitude=51.5072178&longitude=-0.1275862&location=london&searchRadius=2&searchType=ForRent&soldOrLet=false",
		totalPages: 2,
		isRental: true,
		label: "LONDON_RENTS",
	},
];

async function scrapePurplebricks() {
	console.log(`\n🚀 Starting Purplebricks scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
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

			// Wait a bit for JS-rendered content
			await page.waitForTimeout(1500);

			// Target results inside `data-testid="results-list"`
			const properties = await page.evaluate(() => {
				const list = document.querySelector('[data-testid="results-list"]');
				const items = list ? Array.from(list.querySelectorAll("li")) : [];
				const results = [];
				for (const li of items) {
					const a = li.querySelector(
						'a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]'
					);
					if (!a) continue;

					const priceEl =
						li.querySelector('[data-testid="search-result-price"]') ||
						li.querySelector(".sc-cda42038-7");
					const priceText = priceEl ? priceEl.textContent.trim() : "";
					const priceMatch = priceText.match(/£([\d,]+)/);
					const price = priceMatch ? priceMatch[1].replace(/,/g, "") : "";

					const addrEl =
						li.querySelector('[data-testid="search-result-address"]') ||
						li.querySelector(".sc-cda42038-10");
					const address = addrEl ? addrEl.textContent.trim() : "";

					const bedEl =
						li.querySelector('[data-testid="search-result-bedrooms"]') ||
						li.querySelector('[data-testid="search-result-bedrooms-title"] strong');
					const bedrooms = bedEl ? bedEl.textContent.trim() : "";

					const descEl =
						li.querySelector('[data-testid="search-result-description"]') ||
						li.querySelector(".sc-cda42038-11");
					const description = descEl ? descEl.textContent.trim() : "";

					const href = a.href || a.getAttribute("href");
					const link =
						href && href.startsWith("http")
							? href
							: href
							? new URL(href, window.location.origin).href
							: null;

					if (link) {
						results.push({
							link,
							title: address || a.getAttribute("aria-label") || "",
							price,
							bedrooms,
							description,
						});
					}
				}
				return results;
			});

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			for (const property of properties) {
				if (!property.link) continue;

				totalScraped++;

				// detail extraction
				let latitude = null;
				let longitude = null;

				const detailPage = await page.context().newPage();
				try {
					// Step 1: Open the details page first
					await detailPage.goto(property.link, { waitUntil: "networkidle", timeout: 30000 });
					await detailPage.waitForTimeout(500);
					console.log(`📄 Opened details page: ${property.link}`);

					// Step 1.5: Dismiss dialogs (cookie, popups, etc) if they exist
					try {
						// Aggressively close/hide all dialogs and popups using JavaScript
						await detailPage.evaluate(() => {
							// Hide cookie dialog
							const cookieDialog = document.getElementById("CybotCookiebotDialog");
							if (cookieDialog) {
								cookieDialog.style.display = "none";
								cookieDialog.style.visibility = "hidden";
							}
							const underlay = document.getElementById("CybotCookiebotDialogBodyUnderlay");
							if (underlay) {
								underlay.style.display = "none";
								underlay.style.visibility = "hidden";
							}
							// Hide any modal overlays
							const modals = document.querySelectorAll('[role="dialog"]');
							modals.forEach((m) => {
								m.style.display = "none";
								m.style.visibility = "hidden";
								m.style.pointerEvents = "none";
							});
							// Hide needsclick popups (Klaviyo)
							const needsclick = document.querySelectorAll(".needsclick");
							needsclick.forEach((el) => {
								el.style.display = "none";
								el.style.visibility = "hidden";
								el.style.pointerEvents = "none";
							});
							// Hide any element with aria-modal="true"
							const ariaModals = document.querySelectorAll('[aria-modal="true"]');
							ariaModals.forEach((m) => {
								m.style.display = "none";
								m.style.visibility = "hidden";
								m.style.pointerEvents = "none";
							});
						});
						await detailPage.waitForTimeout(500);
						console.log(`✅ Dialogs hidden`);
					} catch (e) {
						console.log(`ℹ️ Dialog hiding skipped: ${e.message}`);
					}

					// Step 2: Click the Street view tab (with popup-safe retries)
					let mapsHref = null;
					try {
						const streetViewHandle = await detailPage.$('a[href="#/view/streetview"]');
						if (!streetViewHandle) {
							console.log(`ℹ️ No Street view tab found on details page: ${property.link}`);
						} else {
							console.log(`🔎 Clicking Street view tab for: ${property.link}`);

							const maxAttempts = 4;
							for (let attempt = 1; attempt <= maxAttempts; attempt++) {
								// Just before each click, aggressively hide any popups/overlays
								await detailPage.evaluate(() => {
									// Hide cookie dialog
									const cookieDialog = document.getElementById("CybotCookiebotDialog");
									if (cookieDialog) {
										cookieDialog.style.display = "none";
										cookieDialog.style.visibility = "hidden";
										cookieDialog.style.pointerEvents = "none";
									}
									const underlay = document.getElementById("CybotCookiebotDialogBodyUnderlay");
									if (underlay) {
										underlay.style.display = "none";
										underlay.style.visibility = "hidden";
										underlay.style.pointerEvents = "none";
									}
									// Hide any modal overlays
									document.querySelectorAll('[role="dialog"]').forEach((m) => {
										m.style.display = "none";
										m.style.visibility = "hidden";
										m.style.pointerEvents = "none";
									});
									// Hide Klaviyo / "needsclick" popups
									document.querySelectorAll(".needsclick").forEach((el) => {
										el.style.display = "none";
										el.style.visibility = "hidden";
										el.style.pointerEvents = "none";
									});
									// Hide any element with aria-modal="true"
									document.querySelectorAll('[aria-modal="true"]').forEach((m) => {
										m.style.display = "none";
										m.style.visibility = "hidden";
										m.style.pointerEvents = "none";
									});
								});

								await streetViewHandle.scrollIntoViewIfNeeded();
								await detailPage.waitForTimeout(300);

								try {
									await streetViewHandle.click();
									console.log(`✅ Clicked Street view tab (attempt ${attempt})`);
									await detailPage.waitForTimeout(3000);
									break;
								} catch (clickErr) {
									console.log(
										`⚠️ Street view click failed (attempt ${attempt}/${maxAttempts}) for ${property.link}: ${clickErr.message}`
									);
									if (attempt === maxAttempts) {
										throw clickErr;
									}
									await detailPage.waitForTimeout(700);
								}
							}
						}
					} catch (e) {
						console.error(
							`⚠️ Error while trying to click Street view for ${property.link}: ${e.message}`
						);
					}

					// Step 3: Wait for Street View overlay to render and extract maps link
					console.log(`⏳ Waiting for Street View overlay to render...`);

					// Poll for up to 10 seconds for the overlay to appear
					const startWait = Date.now();
					while (Date.now() - startWait < 10000) {
						mapsHref = await detailPage.evaluate(() => {
							// Simple: just get the href from gm-iv-address-link
							const link = document.querySelector(".gm-iv-address-link a");
							if (link) {
								const href = link.getAttribute("href") || link.href;
								if (href && href.includes("google.com/maps")) {
									return href;
								}
							}
							return null;
						});
						if (mapsHref) {
							console.log(`✅ Found Google Maps link`);
							break;
						}
						await detailPage.waitForTimeout(700);
					}

					console.log(
						`📍 mapsHref for ${property.link}: ${mapsHref || "NULL - overlay not found"}`
					);

					if (mapsHref) {
						console.log(`🔍 Attempting to parse: ${mapsHref}`);
						// Try all possible patterns
						let match;

						// Pattern 1: @lat,lng (Street View format: @51.5069887,-0.387836,0a)
						match = mapsHref.match(/@([\-0-9\.]+),([\-0-9\.]+)/);
						if (match) {
							latitude = parseFloat(match[1]);
							longitude = parseFloat(match[2]);
							console.log(`✅ Pattern 1 (@lat,lng): lat=${latitude}, lng=${longitude}`);
						} else {
							// Pattern 2: ll=lat,lng (Maps format: ll=51.506996,-0.387587)
							match = mapsHref.match(/[?&]ll=([\-0-9\.]+),([\-0-9\.]+)/);
							if (match) {
								latitude = parseFloat(match[1]);
								longitude = parseFloat(match[2]);
								console.log(`✅ Pattern 2 (ll=lat,lng): lat=${latitude}, lng=${longitude}`);
							} else {
								// Pattern 3: !3dLAT!4dLNG
								match = mapsHref.match(/!3d([\-0-9\.]+)!4d([\-0-9\.]+)/);
								if (match) {
									latitude = parseFloat(match[1]);
									longitude = parseFloat(match[2]);
									console.log(`✅ Pattern 3 (!3dLAT!4dLNG): lat=${latitude}, lng=${longitude}`);
								}
							}
						}
					}
				} catch (err) {
					console.error(`❌ Failed to extract details for ${property.link}: ${err.message}`);
				} finally {
					await detailPage.close();
				} // Save using updatePriceByPropertyURL helper
				try {
					const priceClean = (property.price || "").replace(/[^0-9]/g, "").trim();
					const bedroomsClean = property.bedrooms
						? property.bedrooms.toString().replace(/[^0-9]/g, "")
						: null;

					await updatePriceByPropertyURL(
						property.link,
						priceClean || null,
						property.title || "",
						bedroomsClean || null,
						AGENT_ID,
						isRental,
						latitude,
						longitude
					);

					totalSaved++;
				} catch (dbErr) {
					console.error(`❌ DB error for ${property.link}: ${dbErr.message}`);
				}

				// Rate limiting: small delay between properties
				await new Promise((resolve) => setTimeout(resolve, 1000));
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
			// Some Purplebricks URLs use `?page=` query param; we replace page=1 with page=pg
			let url = propertyType.urlBase;
			url = url.replace(/page=\d+/, `page=${pg}`);

			requests.push({
				url,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}

		await crawler.addRequests(requests);
		await crawler.run();
	}

	console.log(
		`\n✅ Completed Purplebricks - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
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
		await scrapePurplebricks();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
