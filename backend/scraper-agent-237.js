// Selectiv scraper using Playwright with Crawlee
// Agent ID: 237
// Usage:
// node backend/scraper-agent-237.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 237;

// Updated pagination based on site check
const PROPERTY_TYPES = [
	{
		urlBase: "https://www.selectiv.co.uk/properties/for-sale/hide-completed/page/",
		totalPages: 14, // 164 properties / 12 per page
		isRental: false,
		label: "FOR SALE",
	},
	{
		urlBase: "https://www.selectiv.co.uk/properties/to-rent/hide-completed/page/",
		totalPages: 1, // Only 1 property found
		isRental: true,
		label: "TO RENT",
	},
];

async function scrapeSelectiv() {
	console.log(`\n Starting Selectiv scraper (Agent ${AGENT_ID})...\n`);

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
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

			console.log(` ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(1000);

			// Wait for property cards/links
			await page
				.waitForSelector('a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]', {
					timeout: 20000,
				})
				.catch(() => console.log(` No property links found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const results = [];
					const seenLinks = new Set();

					const links = Array.from(
						document.querySelectorAll(
							'a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]'
						)
					);

					links.forEach((link) => {
						const h3 = link.querySelector("h3");
						const h2 = link.querySelector("h2");

						if (h3 && h2) {
							const href = link.getAttribute("href");
							if (!href || seenLinks.has(href)) return;
							seenLinks.add(href);

							const card = link.closest("div") || link.parentElement;
							if (card && /Under Offer|Sold STC|SSTC/i.test(card.textContent || "")) return;

							const fullLink = href.startsWith("http")
								? href
								: "https://www.selectiv.co.uk" + (href.startsWith("/") ? "" : "/") + href;

							const priceText = h3.textContent.trim();
							const address = h2.textContent.trim();

							let bedrooms = null;
							let debugInfo = "";
							try {
								// Target the specific row we know contains the numbers
								const iconRow = link.querySelector('div[class*="flex"][class*="items-center"]');
								if (iconRow) {
									const numberSpans = Array.from(iconRow.querySelectorAll("span")).filter((s) =>
										/^\d+$/.test(s.textContent.trim())
									);
									if (numberSpans.length > 0) {
										bedrooms = numberSpans[0].textContent.trim();
									} else {
										debugInfo += "[No digit span in iconRow] ";
									}
								} else {
									debugInfo += "[No iconRow found] ";
								}

								if (!bedrooms) {
									const bedSpan = link.querySelector('span[class*="mr-20"], .font-bold.text-17');
									if (bedSpan && /^\d+$/.test(bedSpan.textContent.trim())) {
										bedrooms = bedSpan.textContent.trim();
									}
								}
							} catch (e) {
								debugInfo += `[Err: ${e.message}] `;
							}

							results.push({ link: fullLink, priceText, address, bedrooms, debugInfo });
						}
					});
					return results;
				} catch (err) {
					return [];
				}
			});

			console.log(` Found ${properties.length} properties on page ${pageNum}`);

			// Log properties with missing bedrooms to help debug
			properties.forEach((p) => {
				if (!p.bedrooms) {
					console.log(`  Bedroom Null for: ${p.address} | Info: ${p.debugInfo}`);
				}
			});

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						let lat = null;
						let lon = null;

						const detailPage = await page.context().newPage();
						try {
							await detailPage.goto(property.link, {
								waitUntil: "domcontentloaded",
								timeout: 45000,
							});
							await detailPage.waitForTimeout(500);

							// Extract coords from JSON-LD or scripts
							const coords = await detailPage.evaluate(() => {
								try {
									// 1. Check JSON-LD schema
									const schema = document.querySelector('script[type="application/ld+json"]');
									if (schema) {
										const data = JSON.parse(schema.textContent);
										const geo = data.geo || (data.itemOffered && data.itemOffered.geo);
										if (geo && geo.latitude && geo.longitude) {
											return { lat: parseFloat(geo.latitude), lon: parseFloat(geo.longitude) };
										}
									}

									// 2. Fallback to regex in scripts
									const scripts = Array.from(document.querySelectorAll("script"));
									for (const s of scripts) {
										const txt = s.textContent || "";
										const latM = txt.match(/"latitude"\s*:\s*([-0-9.]+)/);
										const lngM = txt.match(/"longitude"\s*:\s*([-0-9.]+)/);
										if (latM && lngM) {
											return { lat: parseFloat(latM[1]), lon: parseFloat(lngM[1]) };
										}
									}
								} catch (e) {}
								return null;
							});

							if (coords) {
								lat = coords.lat;
								lon = coords.lon;
							}
						} catch (err) {
							// console.log(` Detail page error: ${property.link}`);
						} finally {
							await detailPage.close();
						}

						// Clean price
						const priceMatch = (property.priceText || "").replace(/,/g, "").match(/\d+/);
						const priceClean = priceMatch ? priceMatch[0] : null;

						try {
							await updatePriceByPropertyURL(
								property.link.trim(),
								priceClean,
								property.address,
								property.bedrooms,
								AGENT_ID,
								isRental,
								lat,
								lon
							);
							// Skip logging for clean output unless needed
							// console.log(` Saved: ${property.address}`);
						} catch (dbErr) {
							console.error(` DB error for ${property.link}: ${dbErr.message}`);
						}
					})
				);
			}
		},

		failedRequestHandler({ request }) {
			console.error(` Failed: ${request.url}`);
		},
	});

	// Enqueue all pages
	for (const propertyType of PROPERTY_TYPES) {
		const requests = [];
		for (let pg = 1; pg <= propertyType.totalPages; pg++) {
			requests.push({
				url: `${propertyType.urlBase}${pg}`,
				userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
			});
		}
		console.log(` Enqueuing ${propertyType.label} (${propertyType.totalPages} pages)`);
		await crawler.addRequests(requests);
	}

	await crawler.run();
}

(async () => {
	try {
		await scrapeSelectiv();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
