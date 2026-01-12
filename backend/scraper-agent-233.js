// Gatekeeper scraper using Playwright with Camoufox
// Agent ID: 233
// Website: gatekeeper.co.uk
// Usage:
// node backend/scraper-agent-233.js

const { firefox } = require("playwright");
const { launchOptions } = require("camoufox-js");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

const AGENT_ID = 233;

const formatPrice = (num) => {
	return "£" + num.toLocaleString("en-GB");
};

let totalScraped = 0;
let totalSaved = 0;

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

async function scrapeGatekeeper() {
	console.log(`\n🚀 Starting Gatekeeper scraper (Agent ${AGENT_ID})...\n`);

	const browser = await firefox.launch(
		await launchOptions({
			headless: true,
		})
	);

	const page = await browser.newPage({
		ignoreHTTPSErrors: true,
	});

	// Create a separate browser instance for detail page extraction
	const detailBrowser = await firefox.launch(
		await launchOptions({
			headless: true,
		})
	);

	for (const propertyType of PROPERTY_TYPES) {
		await scrapePropertyType(propertyType, page, detailBrowser);
	}

	await page.close();
	await browser.close();
	await detailBrowser.close();

	console.log(`\n✅ Scraping complete!`);
	console.log(`Total scraped: ${totalScraped}`);
	console.log(`Total saved: ${totalSaved}\n`);
}

async function scrapePropertyType(propertyType, page, detailBrowser) {
	const { url, isRental, label, buttonSelector } = propertyType;

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
		totalScraped += properties.length;

		// Save properties to database with details page extraction
		const batchSize = 2;
		for (let i = 0; i < properties.length; i += batchSize) {
			const batch = properties.slice(i, i + batchSize);

			await Promise.all(
				batch.map(async (property) => {
					try {
						// Extract coordinates from details page using separate browser
						const coords = await extractCoordsFromDetailsPage(detailBrowser, property.url);
						if (coords) {
							property.latitude = coords.latitude;
							property.longitude = coords.longitude;
						}

						const priceClean = property.price ? property.price.replace(/[^0-9.]/g, "") : null;
						const priceNum = parseFloat(priceClean);

						await updatePriceByPropertyURL(
							property.url,
							priceClean,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental,
							property.latitude,
							property.longitude
						);

						totalSaved++;
						const priceDisplay = isNaN(priceNum) ? "N/A" : formatPrice(priceNum);
						console.log(
							`✅ ${property.title} - ${priceDisplay}${
								property.latitude ? ` - (${property.latitude}, ${property.longitude})` : ""
							}`
						);
					} catch (err) {
						console.error(`❌ Error saving property: ${err.message}`);
					}
				})
			);

			await page.waitForTimeout(500);
		}
	} catch (error) {
		console.error(`❌ Error in ${label} scrape: ${error.message}`);
	}
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
					`✅ All properties loaded - button not found (${clickCount} clicks, ${currentCount} total)`
				);
				break;
			}

			const isVisible = await button.isVisible().catch(() => false);
			if (!isVisible) {
				console.log(
					`✅ All properties loaded - button not visible (${clickCount} clicks, ${currentCount} total)`
				);
				break;
			}

			// If no new properties were added, stop clicking
			if (currentCount === previousCount) {
				noChangeCount++;
				console.log(`⚠️ No new properties added (${noChangeCount} consecutive checks)`);

				if (noChangeCount >= 3) {
					console.log(
						`✅ All properties loaded - no new properties for 3 checks (${clickCount} clicks, ${currentCount} total)`
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
						p.textContent.includes("Standlake")
				);
				const location =
					locationEls.length > 0
						? locationEls[0].textContent.trim()
						: card.querySelector("p")?.textContent.trim() || "N/A";

				// Extract price (£ sign text); keep numeric with commas, strip currency/extra chars
				const priceEls = Array.from(card.querySelectorAll("p")).filter((p) =>
					p.textContent.match(/£[\d,]+/)
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
					card.querySelectorAll(".flex.flex-col.justify-center.items-center")
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

async function extractCoordsFromDetailsPage(browser, propertyUrl) {
	let detailPage = null;
	try {
		console.log(`📍 Extracting coordinates from: ${propertyUrl}`);

		// Wrap entire function in timeout
		return await Promise.race([
			(async () => {
				detailPage = await browser.newPage({
					ignoreHTTPSErrors: true,
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

				// First, try to extract from main page
				let coords = await detailPage.evaluate(() => {
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
												setTimeout(() => reject(new Error("iframe evaluation timeout")), 5000)
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
				setTimeout(() => reject(new Error("Coordinate extraction timeout after 20 seconds")), 20000)
			),
		]);
	} catch (err) {
		console.error(`Error extracting coordinates: ${err.message}`);
		// Ensure page gets closed on error
		try {
			if (detailPage) {
				await detailPage.close().catch(() => {});
			}
		} catch (e) {
			// Ignore errors closing page
		}
		return null;
	}
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
