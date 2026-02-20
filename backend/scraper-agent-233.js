// Gatekeeper scraper using Playwright with Crawlee
// Agent ID: 233
// Website: gatekeeper.co.uk
// Usage:
// node backend/scraper-agent-233.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 233;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
	// {
	// 	url: "https://www.gatekeeper.co.uk/properties",
	// 	isRental: false,
	// 	label: "SALES",
	// 	buttonSelector: "#buyBtn",
	// },
	{
		url: "https://www.gatekeeper.co.uk/properties",
		isRental: true,
		label: "RENTALS",
		buttonSelector: "#rentBtn",
	},
];

async function handleListingPage({ page, request }) {
	const { url, isRental, label, buttonSelector } = request.userData;

	console.log(`\n📋 Starting ${label} scrape...\n`);

	try {
		console.log(`📍 Loading: ${url}`);

		// Navigate to the page
		await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
		await page.waitForTimeout(2000);

		// Wait for properties list to load
		await page.waitForSelector("#properties_list", { timeout: 30000 }).catch(() => {
			console.log(`⚠️ Properties list not found`);
		});

		// Click the appropriate button to show sales or rentals
		const button = await page.$(buttonSelector);
		if (button) {
			console.log(`🔘 Clicking ${label} button...`);
			await button.click();
			await page.waitForTimeout(2000);

			// Trigger search after selecting the tab
			const searchButton =
				(await page.$('button[onclick*="search_properties_form"]')) ||
				(await page.$('#search_properties_form button[type="submit"]')) ||
				(await page.$('button:has-text("Search")'));

			if (searchButton) {
				console.log(`🔘 Clicking Search button...`);
				await Promise.all([
					searchButton.click(),
					page.waitForNavigation({ waitUntil: "domcontentloaded", timeout: 30000 }).catch(() => {}),
				]);
				await page.waitForTimeout(1500);
			} else {
				console.log(`⚠️ Search button not found`);
			}
		}

		// Load all properties by clicking "View More Properties" button
		await loadAllProperties(page);

		// Extract properties from the page
		const properties = await extractPropertiesFromPage(page, isRental);

		console.log(`✅ Found ${properties.length} ${label.toLowerCase()}`);
		stats.totalScraped += properties.length;

		// Save properties to database with details page extraction
		const batchSize = 2;
		for (let i = 0; i < properties.length; i += batchSize) {
			const batch = properties.slice(i, i + batchSize);

			await Promise.all(
				batch.map(async (property) => {
					try {
						const coords = await extractCoordsFromDetailsPage(page.context(), property.url);
						if (coords) {
							property.latitude = coords.latitude;
							property.longitude = coords.longitude;
						}

						const priceNum = property.price
							? parseFloat(property.price.replace(/[^0-9.]/g, ""))
							: null;

						if (priceNum === null) {
							console.log(`⚠️ No price found: ${property.title}`);
							return;
						}

						const result = await updatePriceByPropertyURLOptimized(
							property.url.trim(),
							priceNum,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
						);

						let persisted = !!result.updated;

						if (!result.isExisting) {
							await updatePriceByPropertyURL(
								property.url.trim(),
								formatPriceUk(priceNum),
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								property.latitude,
								property.longitude,
							);
							persisted = true;
						}

						if (persisted) {
							stats.totalSaved++;
						}

						const priceDisplay = formatPriceUk(priceNum);
						console.log(
							`✅ ${property.title} - ${priceDisplay}${
								property.latitude ? ` - (${property.latitude}, ${property.longitude})` : ""
							}`,
						);
						if (persisted) {
							if (isRental) stats.savedRentals++;
							else stats.savedSales++;
						}
					} catch (err) {
						console.error(`❌ Error saving property: ${err.message}`);
					}
				}),
			);

			await page.waitForTimeout(500);
		}
	} catch (error) {
		console.error(`❌ Error in ${label} scrape: ${error.message}`);
	}
}

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			log.error(` Failed listing page: ${request.url}`);
		},
	});
}

async function scrapeGatekeeper() {
	console.log(`\n🚀 Starting Gatekeeper scraper (Agent ${AGENT_ID})...\n`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const requests = PROPERTY_TYPES.map((propertyType) => ({
		url: propertyType.url,
		userData: {
			url: propertyType.url,
			isRental: propertyType.isRental,
			label: propertyType.label,
			buttonSelector: propertyType.buttonSelector,
		},
	}));

	await crawler.addRequests(requests);
	await crawler.run();

	console.log(`\n✅ Scraping complete!`);
	console.log(`Total scraped: ${stats.totalScraped}`);
	console.log(`Total saved: ${stats.totalSaved}`);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}\n`);
}

async function loadAllProperties(page) {
	console.log(`📄 Loading all properties...`);
	let clickCount = 0;
	let previousCount = 0;
	let noChangeCount = 0;

	// Keep clicking until no new properties are added or button disappears
	while (true) {
		try {
			// Get current property count
			const currentCount = await page.evaluate(() => {
				return document.querySelectorAll("#properties_list a[id^='property_']").length;
			});

			console.log(`📊 Current properties: ${currentCount} (previous: ${previousCount})`);

			// Check if button exists and is visible
			const button = await page.$('button[data-turbo-submits-with="Loading More Properties ..."]');
			if (!button) {
				console.log(
					`✅ All properties loaded - button not found (${clickCount} clicks, ${currentCount} total)`,
				);
				break;
			}

			const isVisible = await button.isVisible().catch(() => false);
			if (!isVisible) {
				console.log(
					`✅ All properties loaded - button not visible (${clickCount} clicks, ${currentCount} total)`,
				);
				break;
			}

			// If no new properties were added, stop clicking
			if (currentCount === previousCount) {
				noChangeCount++;
				console.log(`⚠️ No new properties added (${noChangeCount} consecutive checks)`);

				if (noChangeCount >= 3) {
					console.log(
						`✅ All properties loaded - no new properties for 3 checks (${clickCount} clicks, ${currentCount} total)`,
					);
					break;
				}
			} else {
				noChangeCount = 0;
				previousCount = currentCount;
			}

			console.log(`🔄 Clicking "View More Properties" (click ${clickCount + 1})...`);
			await button.click();
			clickCount++;

			// Wait for new properties to load
			await page.waitForTimeout(2500);
		} catch (error) {
			console.log(`⚠️ Error during pagination: ${error.message}`);
			break;
		}
	}
}

async function extractPropertiesFromPage(page, isRental) {
	const properties = await page.evaluate((isRental) => {
		const cards = Array.from(document.querySelectorAll("#properties_list a[id^='property_']"));
		const results = [];

		cards.forEach((card) => {
			try {
				// Skip "Sold STC" properties
				const statusImg = card.querySelector('img[alt="Recently Sold"]');
				if (statusImg) {
					return; // Skip sold properties
				}

				// Extract URL
				const href = card.getAttribute("href");
				const url = href
					? href.startsWith("http")
						? href
						: "https://www.gatekeeper.co.uk" + href
					: null;

				if (!url) return;

				// Extract title (h3)
				const titleEl = card.querySelector("h3");
				const title = titleEl ? titleEl.textContent.trim() : "N/A";

				// Extract location (p with location icon)
				const locationEls = Array.from(card.querySelectorAll("p")).filter(
					(p) =>
						p.textContent.includes("Oxfordshire") ||
						p.textContent.includes("Witney") ||
						p.textContent.includes("Standlake"),
				);
				const location =
					locationEls.length > 0
						? locationEls[0].textContent.trim()
						: card.querySelector("p")?.textContent.trim() || "N/A";

				// Extract price (£ sign text); keep numeric with commas, strip currency/extra chars
				const priceEls = Array.from(card.querySelectorAll("p")).filter((p) =>
					p.textContent.match(/£[\d,]+/),
				);
				const priceRaw = priceEls.length > 0 ? priceEls[0].textContent.trim() : "N/A";
				const priceText = priceRaw
					.replace(/£/g, "")
					.replace(/[^0-9,.-]/g, "")
					.trim();

				// Extract bedrooms, bathrooms, size from the flex items at bottom
				let bedrooms = null;
				let bathrooms = null;
				let size = null;

				const flexItems = Array.from(
					card.querySelectorAll(".flex.flex-col.justify-center.items-center"),
				);
				flexItems.forEach((item) => {
					const text = item.textContent;
					if (text.includes("Beds")) {
						const numMatch = text.match(/(\d+)\s*Beds/);
						bedrooms = numMatch ? parseInt(numMatch[1]) : null;
					} else if (text.includes("Baths")) {
						const numMatch = text.match(/(\d+)\s*Baths/);
						bathrooms = numMatch ? parseInt(numMatch[1]) : null;
					} else if (text.includes("Sqm")) {
						const numMatch = text.match(/(\d+(?:\.\d+)?)\s*Sqm/);
						size = numMatch ? parseFloat(numMatch[1]) : null;
					}
				});

				results.push({
					url,
					title: `${title}, ${location}`,
					price: priceText,
					bedrooms,
					bathrooms,
					size,
					latitude: null, // Will be extracted from details page
					longitude: null,
				});
			} catch (err) {
				console.error(`Error extracting property: ${err.message}`);
			}
		});

		return results;
	}, isRental);

	return properties;
}

async function extractCoordsFromDetailsPage(browserContext, propertyUrl) {
	let detailPage = null;
	const mapRequestUrls = [];
	try {
		console.log(`📍 Extracting coordinates from: ${propertyUrl}`);

		// Wrap entire function in timeout
		return await Promise.race([
			(async () => {
				detailPage = await browserContext.newPage({
					ignoreHTTPSErrors: true,
				});

				detailPage.on("request", (req) => {
					const reqUrl = req.url();
					if (reqUrl.includes("StaticMapService.GetMapImage") || reqUrl.includes("/maps/vt?pb=")) {
						mapRequestUrls.push(reqUrl);
					}
				});

				await detailPage.goto(propertyUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
				await detailPage.waitForTimeout(1500);

				// Look for Location heading and scroll 100px below it
				const locationFound = await detailPage.evaluate(() => {
					const headings = Array.from(document.querySelectorAll("h3"));
					const locationHeading = headings.find((h) => h.textContent.trim() === "Location");
					if (locationHeading) {
						const rect = locationHeading.getBoundingClientRect();
						const scrollY = window.pageYOffset + rect.top;
						window.scrollTo({
							top: scrollY - 200, // Scroll 200px above the heading to ensure it's in view
							behavior: "smooth",
						});
						// Wait a bit then scroll 100px more to reveal content below
						setTimeout(() => {
							window.scrollBy(0, 100);
						}, 500);
						return true;
					}
					return false;
				});

				if (locationFound) {
					console.log(`📍 Scrolled to Location section`);
					await detailPage.waitForTimeout(1000); // Wait for the delayed scroll
				} else {
					console.log(`⚠️ Location section not found, scrolling to bottom`);
					await detailPage.evaluate(() => {
						window.scrollTo(0, document.body.scrollHeight);
					});
				}

				// Wait for maps to load
				await detailPage.waitForTimeout(2000);

				// First attempt: decode coordinates from map network requests
				let coords = extractCoordsFromMapRequests(mapRequestUrls);
				if (coords) {
					console.log(`✓ Found coordinates in map requests`);
					console.log(`✓ Extracted coords: ${coords.latitude}, ${coords.longitude}`);
					return coords;
				}

				// Fallback: try to extract from main page
				coords = await detailPage.evaluate(() => {
					// Try to find the Google Maps link with ll parameter
					const mapLink = document.querySelector('a[href*="maps.google.com"]');
					if (mapLink) {
						const href = mapLink.getAttribute("href");
						if (href) {
							// Extract ll parameter: ll=51.580382,-1.884427
							const llMatch = href.match(/ll=([0-9.]+),(-?[0-9.]+)/);
							if (llMatch) {
								return {
									latitude: parseFloat(llMatch[1]),
									longitude: parseFloat(llMatch[2]),
								};
							}
						}
					}
					return null;
				});

				// If not found in main page, try to extract from iframe
				if (!coords) {
					console.log(`🔍 Checking iframes for Google Maps link...`);
					try {
						const iframes = await detailPage.$$("iframe");
						console.log(`Found ${iframes.length} iframes`);

						for (const iframe of iframes) {
							try {
								const frameHandle = await iframe.contentFrame();
								if (frameHandle) {
									try {
										const iframeCoords = await Promise.race([
											frameHandle.evaluate(() => {
												const mapLink = document.querySelector('a[href*="maps.google.com"]');
												if (mapLink) {
													const href = mapLink.getAttribute("href");
													if (href) {
														const llMatch = href.match(/ll=([0-9.]+),(-?[0-9.]+)/);
														if (llMatch) {
															return {
																latitude: parseFloat(llMatch[1]),
																longitude: parseFloat(llMatch[2]),
															};
														}
													}
												}
												return null;
											}),
											new Promise((_, reject) =>
												setTimeout(() => reject(new Error("iframe evaluation timeout")), 5000),
											),
										]);

										if (iframeCoords) {
											console.log(`✓ Found coordinates in iframe`);
											coords = iframeCoords;
											break;
										}
									} catch (err) {
										console.log(`⚠️ Error evaluating iframe: ${err.message}`);
									}
								}
							} catch (err) {
								console.log(`⚠️ Could not get iframe content: ${err.message}`);
							}
						}
					} catch (err) {
						console.log(`⚠️ Error checking iframes: ${err.message}`);
					}
				}

				// Fallback: Try to extract from script tag with onEmbedLoad
				if (!coords) {
					console.log(`🔍 Checking scripts for embedded coordinates...`);
					coords = await detailPage.evaluate(() => {
						const scripts = Array.from(document.querySelectorAll("script"));
						for (const script of scripts) {
							const text = script.textContent;
							if (text.includes("function onEmbedLoad()") && text.includes("initEmbed")) {
								// Extract coordinate pairs like [52.0574583,-1.3359553]
								const matches = text.match(/\[(\d+\.\d+),(-?\d+\.\d+)\]/g);

								if (matches && matches.length > 0) {
									// Find the one that looks like UK coordinates
									for (const match of matches) {
										const coordsMatch = match.match(/\[(\d+\.\d+),(-?\d+\.\d+)\]/);
										if (coordsMatch) {
											const lat = parseFloat(coordsMatch[1]);
											const lon = parseFloat(coordsMatch[2]);
											// UK coordinates roughly: lat 50-56, lon -8 to 2
											if (lat > 50 && lat < 56 && lon > -8 && lon < 2) {
												return { latitude: lat, longitude: lon };
											}
										}
									}
								}
							}
						}
						return null;
					});
				}
				if (coords) {
					console.log(`✓ Extracted coords: ${coords.latitude}, ${coords.longitude}`);
				} else {
					console.log(`✗ No coords extracted`);
				}

				return coords;
			})(),
			new Promise((_, reject) =>
				setTimeout(
					() => reject(new Error("Coordinate extraction timeout after 20 seconds")),
					20000,
				),
			),
		]);
	} catch (err) {
		console.error(`Error extracting coordinates: ${err.message}`);
		return null;
	} finally {
		if (detailPage) {
			await detailPage.close().catch(() => {});
		}
	}
}

function extractCoordsFromMapRequests(requestUrls) {
	if (!Array.isArray(requestUrls) || requestUrls.length === 0) {
		return null;
	}

	for (const requestUrl of requestUrls) {
		// Pattern 1: Static map request with pixel center at fixed zoom (usually 15)
		const staticMatch = requestUrl.match(/[?&]1i=(\d+).*?[?&]2i=(\d+).*?[?&]3u=(\d+)/);
		if (staticMatch) {
			const pixelX = parseInt(staticMatch[1], 10);
			const pixelY = parseInt(staticMatch[2], 10);
			const zoom = parseInt(staticMatch[3], 10);

			const coords = pixelToLatLng(pixelX, pixelY, zoom);
			if (coords && isUkCoordinate(coords.latitude, coords.longitude)) {
				return coords;
			}
		}

		// Pattern 2: Vector tile request with E7 integers in protobuf-like path
		const vtMatch = requestUrl.match(/!1x(-?\d+)!2x(-?\d+)/);
		if (vtMatch) {
			const latE7 = parseInt(vtMatch[1], 10);
			const lonRaw = parseInt(vtMatch[2], 10);
			const lonE7 = toSigned32(lonRaw);

			const latitude = latE7 / 1e7;
			const longitude = lonE7 / 1e7;

			if (isUkCoordinate(latitude, longitude)) {
				return { latitude, longitude };
			}
		}
	}

	return null;
}

function pixelToLatLng(pixelX, pixelY, zoom) {
	if (!Number.isFinite(pixelX) || !Number.isFinite(pixelY) || !Number.isFinite(zoom)) {
		return null;
	}

	const worldSize = 256 * Math.pow(2, zoom);
	const longitude = (pixelX / worldSize) * 360 - 180;
	const n = Math.PI - (2 * Math.PI * pixelY) / worldSize;
	const latitude = (180 / Math.PI) * Math.atan(Math.sinh(n));

	if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
		return null;
	}

	return { latitude, longitude };
}

function toSigned32(value) {
	if (!Number.isFinite(value)) return value;
	return value > 2147483647 ? value - 4294967296 : value;
}

function isUkCoordinate(latitude, longitude) {
	return latitude > 49 && latitude < 61 && longitude > -11 && longitude < 3;
}

(async () => {
	try {
		await scrapeGatekeeper();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
