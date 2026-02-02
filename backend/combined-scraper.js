const { CheerioCrawler, PlaywrightCrawler } = require("crawlee");
const cheerio = require("cheerio");
const { updatePriceByPropertyURL, updateRemoveStatus, promisePool } = require("./db.js");

// Keywords to identify sold properties
const SOLD_KEYWORDS = [
	"sold subject to contract",
	"sold stc",
	"sold",
	"under offer",
	"let agreed",
	"let",
	"withdrawn",
	"off market",
];

// Combined scraper for multiple agents using Crawlee
const AGENTS = [
	{
		id: 4,
		name: "Marsh & Parsons",
		propertyTypes: [
			{
				name: "Sales",
				baseUrl:
					"https://www.marshandparsons.co.uk/properties-for-sale/london/?filters=exclude_sold%2Cexclude_under_offer",
				isRent: false,
				totalPages: 30,
			},
		],
	},
	{
		id: 8,
		name: "Jackie Quinn",
		propertyTypes: [
			{
				name: "Sales",
				baseUrl:
					"https://www.jackiequinn.co.uk/search?category=1&listingtype=5&statusids=1%2C10%2C4%2C16%2C3&obc=Price&obd=Descending",
				isRent: false,
				totalPages: 12,
			},
		],
	},
	{
		id: 12,
		name: "Purplebricks",
		propertyTypes: [
			{
				name: "London Rents",
				baseUrl:
					"https://www.purplebricks.co.uk/search/property-to-rent/greater-london/london?sortBy=2&betasearch=true&latitude=51.5072178&longitude=-0.1275862&location=london&searchRadius=2&searchType=ForRent&soldOrLet=false",
				isRent: true,
				totalPages: 2,
			},
		],
	},
	{
		id: 13,
		name: "Bairstow Eves",
		propertyTypes: [
			{
				name: "Sales",
				baseUrl:
					"https://www.bairstoweves.co.uk/properties/sales/status-available/most-recent-first",
				isRent: false,
				totalPages: 57,
			},
			{
				name: "Lettings",
				baseUrl:
					"https://www.bairstoweves.co.uk/properties/lettings/status-available/most-recent-first",
				isRent: true,
				totalPages: 13,
			},
		],
	},
	{
		id: 14,
		name: "Chestertons",
		propertyTypes: [
			{
				name: "Lettings",
				baseUrl: "https://www.chestertons.co.uk/properties/lettings/status-available",
				isRent: true,
				totalPages: 95,
			},
		],
	},
	{
		id: 15,
		name: "Sequence Home",
		propertyTypes: [
			{
				name: "Rentals",
				baseUrl: "https://www.sequencehome.co.uk/properties/lettings",
				isRent: true,
				totalPages: 191,
			},
		],
	},
];

// Memory monitoring
function logMemoryUsage(label) {
	const used = process.memoryUsage();
	console.log(
		`[${label}] Memory: ${Math.round(used.heapUsed / 1024 / 1024)}MB / ${Math.round(
			used.heapTotal / 1024 / 1024,
		)}MB`,
	);
}

// Check if property is sold based on text content
function isSoldProperty(text) {
	const lowerText = text.toLowerCase();
	return SOLD_KEYWORDS.some((keyword) => lowerText.includes(keyword));
}

// Optimized update function - only updates price for existing properties
async function updatePriceByPropertyURLOptimized(
	link,
	price,
	title,
	bedrooms,
	agent_id,
	is_rent = false,
) {
	try {
		if (link) {
			let tableName = "property_for_sale";
			if (is_rent) {
				tableName = "property_for_rent";
			}

			const linkTrimmed = link.trim();

			// Check if property exists for THIS agent
			const [propertiesUrlRows] = await promisePool.query(
				`SELECT COUNT(*) as count FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
				[linkTrimmed, agent_id],
			);

			if (propertiesUrlRows[0].count > 0) {
				// UPDATE existing property - only price
				const [result] = await promisePool.query(
					`UPDATE ${tableName}
                    SET price = ?, updated_at = NOW()
                    WHERE property_url = ? AND agent_id = ?`,
					[price, linkTrimmed, agent_id],
				);

				if (result.affectedRows > 0) {
					console.log(`✅ Updated price: ${linkTrimmed.substring(0, 50)}... | Price: £${price}`);
				}
				return { isExisting: true, updated: result.affectedRows > 0 };
			} else {
				// For new properties, we'll need coordinates
				return { isExisting: false, updated: false };
			}
		}
		return { isExisting: false, updated: false };
	} catch (error) {
		console.error(`❌ Error checking property ${link}:`, error.message || error);
		console.error(`Full error:`, error);
		// Don't throw - return error state instead to prevent crawler from failing
		return { isExisting: false, updated: false, error: error.message || String(error) };
	}
}

// Extract coordinates from HTML
async function extractCoordinatesFromHTML(html) {
	let latitude = null;
	let longitude = null;

	try {
		const mapsMatch = html.match(/ll=([\d.-]+),([\d.-]+)/);
		const scriptMatch = html.match(/lat:\s*"?([\d.-]+)"?,\s*lng:\s*"?([\d.-]+)"?/);
		const jsonMatch = html.match(
			/"latitude"\s*:\s*"?([\d.-]+)"?,\s*"longitude"\s*:\s*"?([\d.-]+)"?/,
		);
		const latLngMatch = html.match(/"lat"\s*:\s*"?([\d.-]+)"?,\s*"lng"\s*:\s*"?([\d.-]+)"?/);
		const dataAttrMatch = html.match(
			/data-(?:lat|latitude)="([\d.-]+)"[\s\S]*?data-(?:lng|longitude)="([\d.-]+)"/,
		);
		const atMatch = html.match(/@([0-9.-]+),([0-9.-]+),\d+z/);
		const latCommentMatch = html.match(/<!--property-latitude:["']([0-9.-]+)["']-->/);
		const lngCommentMatch = html.match(/<!--property-longitude:["']([0-9.-]+)["']-->/);

		if (mapsMatch) {
			latitude = parseFloat(mapsMatch[1]);
			longitude = parseFloat(mapsMatch[2]);
		} else if (scriptMatch) {
			latitude = parseFloat(scriptMatch[1]);
			longitude = parseFloat(scriptMatch[2]);
		} else if (jsonMatch) {
			latitude = parseFloat(jsonMatch[1]);
			longitude = parseFloat(jsonMatch[2]);
		} else if (latLngMatch) {
			latitude = parseFloat(latLngMatch[1]);
			longitude = parseFloat(latLngMatch[2]);
		} else if (dataAttrMatch) {
			latitude = parseFloat(dataAttrMatch[1]);
			longitude = parseFloat(dataAttrMatch[2]);
		} else if (atMatch) {
			latitude = parseFloat(atMatch[1]);
			longitude = parseFloat(atMatch[2]);
		} else if (latCommentMatch && lngCommentMatch) {
			latitude = parseFloat(latCommentMatch[1]);
			longitude = parseFloat(lngCommentMatch[1]);
		}
	} catch (error) {
		console.error("Error extracting coordinates:", error.message);
	}

	return { latitude, longitude };
}

// Process property with coordinates from detail page
async function processPropertyWithCoordinates(url, price, title, bedrooms, agentId, isRent, html) {
	try {
		const coords = await extractCoordinatesFromHTML(html);

		await updatePriceByPropertyURL(
			url,
			price,
			title,
			bedrooms,
			agentId,
			isRent,
			coords.latitude,
			coords.longitude,
		);

		console.log(
			`✅ New property: ${title} (£${price}) - Coords: ${coords.latitude}, ${coords.longitude}`,
		);
	} catch (error) {
		console.error(`❌ Failed ${url}:`, error.message);
		// Don't throw - just log the error
	}
}

// Generic Cheerio crawler for agents that work with simple HTTP requests
async function scrapeWithCheerio(urls, agentId, isRent) {
	const crawler = new CheerioCrawler({
		requestHandlerTimeoutSecs: 60,
		maxRequestRetries: 2,
		maxConcurrency: 5,
		async requestHandler({ $, request, body }) {
			// Handle detail pages
			if (request.userData?.isDetailPage) {
				const html = body.toString();
				await processPropertyWithCoordinates(
					request.url,
					request.userData.price,
					request.userData.title,
					request.userData.bedrooms,
					request.userData.agentId,
					request.userData.isRent,
					html,
				);
				return;
			}

			console.log(`\n📋 Scraping: ${request.url}`);
			const propertyList = [];

			// Extract properties based on agent
			if (agentId === 8) {
				// Jackie Quinn
				$(".propertyBox").each((index, element) => {
					try {
						const $listing = $(element);
						const linkEl = $listing.find("h2.searchProName a").first();
						const link = linkEl.attr("href");
						const title = linkEl.text();

						const priceEl = $listing.find("h3 div").first();
						const priceText = priceEl.text();

						if (isSoldProperty(priceText)) return;

						const priceMatch = priceText.match(/£([\d,]+)/);
						const priceRaw = priceMatch ? priceMatch[0] : null;

						const descEl = $listing.find(".featuredDescriptions").first();
						const description = descEl.text();
						const bedroomMatch = description.match(/(\d+)\s+BEDROOM/i);
						const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

						if (link && title && priceRaw) {
							propertyList.push({
								url: link.startsWith("http") ? link : "https://www.jackiequinn.co.uk" + link,
								title: title.trim(),
								priceRaw,
								bedrooms,
							});
						}
					} catch (err) {
						console.error(`Error extracting property: ${err.message}`);
					}
				});
			}

			console.log(`Found ${propertyList.length} available properties`);

			// Process each property
			for (const property of propertyList) {
				const { url, title, priceRaw, bedrooms } = property;
				let priceClean = priceRaw.replace(/[£,]/g, "");
				if (isRent && priceClean.includes("p/w")) {
					priceClean = priceClean.replace("p/w", "").trim();
				}
				const price = parseFloat(priceClean);

				// For Bairstow Eves (agent 13), always visit detail page for coordinates
				if (agentId === 13) {
					await crawler.addRequests([
						{
							url,
							userData: {
								isDetailPage: true,
								price,
								title: title.trim(),
								bedrooms,
								isRent,
								agentId,
							},
						},
					]);
				} else {
					// Check if property exists
					const result = await updatePriceByPropertyURLOptimized(
						url,
						price,
						title.trim(),
						bedrooms,
						agentId,
						isRent,
					);

					if (!result.isExisting && !result.error) {
						// Need to fetch coordinates from detail page (only if no error)
						await crawler.addRequests([
							{
								url,
								userData: {
									isDetailPage: true,
									price,
									title: title.trim(),
									bedrooms,
									isRent,
									agentId,
								},
							},
						]);
					}
				}
			}
		},
		failedRequestHandler: async ({ request }) => {
			console.log(`⚠️ Request failed after retries: ${request.url}`);
		},
	});

	await crawler.run(urls);
}

// Generic Playwright crawler for agents that need JavaScript rendering
async function scrapeWithPlaywright(urls, agentId, isRent) {
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;

	console.log(`🌐 Connecting to browserless at: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = new PlaywrightCrawler({
		launchContext: {
			launcher: undefined,
			launchOptions: {
				// connect to remote browserless instance
				browserWSEndpoint,
			},
		},
		requestHandlerTimeoutSecs: 60,
		maxRequestRetries: 2,
		maxConcurrency: agentId === 8 || agentId === 13 ? 1 : 2, // Agent 8 & 13 need sequential requests to avoid 429 errors
		failedRequestHandler: async ({ request }) => {
			console.log(`⚠️ Request failed after retries: ${request.url}`);
		},
		preNavigationHooks: [
			async ({ page }) => {
				await page.route("**/*", (route) => {
					const resourceType = route.request().resourceType();
					if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
						route.abort();
					} else {
						route.continue();
					}
				});

				if (agentId === 4) {
					// Marsh & Parsons needs special headers
					await page.setExtraHTTPHeaders({
						"User-Agent":
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
						"Accept-Language": "en-GB,en;q=0.9",
						Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
					});
				}
			},
		],
		async requestHandler({ page, request }) {
			// Handle detail pages
			if (request.userData?.isDetailPage) {
				const agentId = request.userData.agentId;

				// Special handling for Jackie Quinn (agent 8) - click map link to load coordinates
				if (agentId === 8) {
					try {
						// Wait for the map link and click it
						await page.waitForSelector('a[href*="mapcontainer"]', { timeout: 10000 }).catch(() => {
							console.log(`⚠️ No map link found for ${request.userData.title}`);
						});

						const mapLinkClicked = await page.evaluate(() => {
							const mapLink = document.querySelector('a[href*="mapcontainer"]');
							if (mapLink) {
								mapLink.click();
								return true;
							}
							return false;
						});

						if (mapLinkClicked) {
							// Wait for the map to load
							await page.waitForTimeout(1500);
						}
					} catch (error) {
						console.log(`⚠️ Failed to click map for ${request.userData.title}: ${error.message}`);
					}
				}

				// For Bairstow Eves (agent 13), just wait a moment for page to load
				if (agentId === 13) {
					await page.waitForTimeout(1000);
				}

				const html = await page.content();
				await processPropertyWithCoordinates(
					request.url,
					request.userData.price,
					request.userData.title,
					request.userData.bedrooms,
					request.userData.agentId,
					request.userData.isRent,
					html,
				);
				return;
			}

			console.log(`\n📋 Scraping: ${request.url}`);
			await page.waitForTimeout(2000);
			const htmlContent = await page.content();
			const $ = cheerio.load(htmlContent);
			const propertyList = [];

			// Extract properties based on agent
			switch (agentId) {
				case 4:
					// Marsh & Parsons
					$("div.my-4.shadow-md.rounded-xl").each((index, element) => {
						try {
							const $card = $(element);
							const linkElement = $card.find('a[href*="/property/"]').first();
							const titleElement = $card.find("h3").first();
							const locationElement = $card.find("p").first();

							const textContent = $card.text();
							if (isSoldProperty(textContent)) return;

							const priceMatch = textContent.match(/£[0-9,]+(p\/w)?/);
							const priceRaw = priceMatch ? priceMatch[0] : null;

							const bedImg = $card.find('img[alt="bed"]').first();
							let bedrooms = null;
							if (bedImg.length) {
								const parent = bedImg.parent();
								const bedroomText = parent.text();
								const bedroomMatch = bedroomText.trim().match(/\d+/);
								bedrooms = bedroomMatch ? parseInt(bedroomMatch[0]) : null;
							}

							const url = linkElement.attr("href");
							const title = titleElement.text() || "";
							const location = locationElement.text() || "";

							if (url && priceRaw) {
								const fullUrl = url.startsWith("http")
									? url
									: `https://www.marshandparsons.co.uk${url}`;
								propertyList.push({
									url: fullUrl,
									title: title.trim(),
									location: location.trim(),
									priceRaw,
									bedrooms,
								});
							}
						} catch (err) {
							console.error(`Error extracting property: ${err.message}`);
						}
					});
					break;

				case 8:
					// Jackie Quinn
					$(".propertyBox").each((index, element) => {
						try {
							const $listing = $(element);
							const linkEl = $listing.find("h2.searchProName a").first();
							const link = linkEl.attr("href");
							const title = linkEl.text();

							const priceEl = $listing.find("h3 div").first();
							const priceText = priceEl.text();

							if (isSoldProperty(priceText)) return;

							const priceMatch = priceText.match(/£([\d,]+)/);
							const priceRaw = priceMatch ? priceMatch[0] : null;

							const descEl = $listing.find(".featuredDescriptions").first();
							const description = descEl.text();
							const bedroomMatch = description.match(/(\d+)\s+BEDROOM/i);
							const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

							if (link && title && priceRaw) {
								propertyList.push({
									url: link.startsWith("http") ? link : "https://www.jackiequinn.co.uk" + link,
									title: title.trim(),
									priceRaw,
									bedrooms,
								});
							}
						} catch (err) {
							console.error(`Error extracting property: ${err.message}`);
						}
					});
					break;

				case 12:
					// Purplebricks
					$('[data-testid="results-list"] li').each((index, element) => {
						try {
							const $li = $(element);
							const linkEl = $li
								.find('a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]')
								.first();
							if (!linkEl.length) return;

							const priceEl = $li
								.find('[data-testid="search-result-price"], .sc-cda42038-7')
								.first();
							const priceText = priceEl.text();

							if (isSoldProperty(priceText)) return;

							const priceMatch = priceText.match(/£([\d,]+)/);
							const priceRaw = priceMatch ? priceMatch[0] : "";

							const addrEl = $li
								.find('[data-testid="search-result-address"], .sc-cda42038-10')
								.first();
							const address = addrEl.text();

							const bedEl = $li.find('[data-testid="search-result-bedrooms"]').first();
							const bedrooms = bedEl.text();

							const href = linkEl.attr("href");
							const url =
								href && href.startsWith("http")
									? href
									: href
										? `https://www.purplebricks.co.uk${href}`
										: null;

							if (url && priceRaw) {
								propertyList.push({
									url,
									title: address.trim(),
									location: "",
									priceRaw,
									bedrooms: bedrooms.trim(),
								});
							}
						} catch (err) {
							console.error(`Error extracting property: ${err.message}`);
						}
					});
					break;

				case 13:
					// Bairstow Eves
					$(".card").each((index, element) => {
						try {
							const $card = $(element);
							const linkEl = $card.find("a.card__link").first();
							const link = linkEl.attr("href");

							const titleEl = $card.find(".card__text-content").first();
							const title = titleEl.text();

							const priceEl = $card.find(".card__heading").first();
							const priceText = priceEl.text();

							if (isSoldProperty(priceText)) return;

							const priceMatch = priceText.match(/£[\d,]+/);
							const priceRaw = priceMatch ? priceMatch[0] : null;

							const bedroomsEl = $card.find(".card-content__spec-list-number").first();
							const bedroomsText = bedroomsEl.text();
							const bedroomsMatch = bedroomsText.match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

							if (link && priceRaw && title) {
								propertyList.push({
									url: link.startsWith("http") ? link : `https://www.bairstoweves.co.uk${link}`,
									title: title.trim(),
									location: "",
									priceRaw,
									bedrooms,
								});
							}
						} catch (err) {
							console.error(`Error extracting property: ${err.message}`);
						}
					});
					break;
			}

			console.log(`Found ${propertyList.length} available properties`);

			// Process each property
			for (const property of propertyList) {
				const { url, title, priceRaw, bedrooms } = property;
				let priceClean = priceRaw.replace(/[£,]/g, "");
				if (isRent && priceClean.includes("p/w")) {
					priceClean = priceClean.replace("p/w", "").trim();
				}
				const price = parseFloat(priceClean);

				// For Bairstow Eves (agent 13), always visit detail page for coordinates
				if (agentId === 13) {
					await crawler.addRequests([
						{
							url,
							userData: {
								isDetailPage: true,
								price,
								title: title.trim(),
								bedrooms,
								isRent,
								agentId,
							},
						},
					]);
				} else {
					// Check if property exists
					const result = await updatePriceByPropertyURLOptimized(
						url,
						price,
						title.trim(),
						bedrooms,
						agentId,
						isRent,
					);

					if (!result.isExisting && !result.error) {
						// Need to fetch coordinates from detail page (only if no error)
						await crawler.addRequests([
							{
								url,
								userData: {
									isDetailPage: true,
									price,
									title: title.trim(),
									bedrooms,
									isRent,
									agentId,
								},
							},
						]);
					}
				}
			}
		},
		failedRequestHandler: async ({ request }) => {
			console.log(`⚠️ Request failed after retries: ${request.url}`);
		},
	});

	await crawler.run(urls);
}

// Generic Playwright crawler for agents that need JavaScript rendering
async function scrapeWithPlaywright(urls, agentId, isRent) {
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;

	console.log(`🌐 Connecting to browserless at: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = new PlaywrightCrawler({
		launchContext: {
			launcher: undefined,
			launchOptions: {
				// connect to remote browserless instance
				browserWSEndpoint,
			},
		},
		requestHandlerTimeoutSecs: 60,
		maxRequestRetries: 2,
		maxConcurrency: agentId === 8 ? 1 : 2, // Agent 8 (Jackie Quinn) needs sequential requests to avoid 429 errors
		failedRequestHandler: async ({ request }) => {
			console.log(`⚠️ Request failed after retries: ${request.url}`);
		},
		preNavigationHooks: [
			async ({ page }) => {
				await page.route("**/*", (route) => {
					const resourceType = route.request().resourceType();
					if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
						route.abort();
					} else {
						route.continue();
					}
				});

				if (agentId === 4) {
					// Marsh & Parsons needs special headers
					await page.setExtraHTTPHeaders({
						"User-Agent":
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
						"Accept-Language": "en-GB,en;q=0.9",
						Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
					});
				}
			},
		],
		async requestHandler({ page, request }) {
			// Handle detail pages
			if (request.userData?.isDetailPage) {
				const agentId = request.userData.agentId;

				// Special handling for Jackie Quinn (agent 8) - click map link to load coordinates
				if (agentId === 8) {
					try {
						// Wait for the map link and click it
						await page.waitForSelector('a[href*="mapcontainer"]', { timeout: 10000 }).catch(() => {
							console.log(`⚠️ No map link found for ${request.userData.title}`);
						});

						const mapLinkClicked = await page.evaluate(() => {
							const mapLink = document.querySelector('a[href*="mapcontainer"]');
							if (mapLink) {
								mapLink.click();
								return true;
							}
							return false;
						});

						if (mapLinkClicked) {
							// Wait for the map to load
							await page.waitForTimeout(1500);
						}
					} catch (error) {
						console.log(`⚠️ Failed to click map for ${request.userData.title}: ${error.message}`);
					}
				}

				// For Bairstow Eves (agent 13), just wait a moment for page to load
				if (agentId === 13) {
					await page.waitForTimeout(1000);
				}

				const html = await page.content();
				await processPropertyWithCoordinates(
					request.url,
					request.userData.price,
					request.userData.title,
					request.userData.bedrooms,
					request.userData.agentId,
					request.userData.isRent,
					html,
				);
				return;
			}

			console.log(`\n📋 Scraping: ${request.url}`);
			await page.waitForTimeout(2000);
			const htmlContent = await page.content();
			const $ = cheerio.load(htmlContent);
			const propertyList = [];

			// Extract properties based on agent
			switch (agentId) {
				case 4:
					// Marsh & Parsons
					$("div.my-4.shadow-md.rounded-xl").each((index, element) => {
						try {
							const $card = $(element);
							const linkElement = $card.find('a[href*="/property/"]').first();
							const titleElement = $card.find("h3").first();
							const locationElement = $card.find("p").first();

							const textContent = $card.text();
							if (isSoldProperty(textContent)) return;

							const priceMatch = textContent.match(/£[0-9,]+(p\/w)?/);
							const priceRaw = priceMatch ? priceMatch[0] : null;

							const bedImg = $card.find('img[alt="bed"]').first();
							let bedrooms = null;
							if (bedImg.length) {
								const parent = bedImg.parent();
								const bedroomText = parent.text();
								const bedroomMatch = bedroomText.trim().match(/\d+/);
								bedrooms = bedroomMatch ? parseInt(bedroomMatch[0]) : null;
							}

							const url = linkElement.attr("href");
							const title = titleElement.text() || "";
							const location = locationElement.text() || "";

							if (url && priceRaw) {
								const fullUrl = url.startsWith("http")
									? url
									: `https://www.marshandparsons.co.uk${url}`;
								propertyList.push({
									url: fullUrl,
									title: title.trim(),
									location: location.trim(),
									priceRaw,
									bedrooms,
								});
							}
						} catch (err) {
							console.error(`Error extracting property: ${err.message}`);
						}
					});
					break;
				case 8:
					// Jackie Quinn
					$(".propertyBox").each((index, element) => {
						try {
							const $listing = $(element);
							const linkEl = $listing.find("h2.searchProName a").first();
							const link = linkEl.attr("href");
							const title = linkEl.text();

							const priceEl = $listing.find("h3 div").first();
							const priceText = priceEl.text();

							if (isSoldProperty(priceText)) return;

							const priceMatch = priceText.match(/£([\d,]+)/);
							const priceRaw = priceMatch ? priceMatch[0] : null;

							const descEl = $listing.find(".featuredDescriptions").first();
							const description = descEl.text();
							const bedroomMatch = description.match(/(\d+)\s+BEDROOM/i);
							const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

							if (link && title && priceRaw) {
								propertyList.push({
									url: link.startsWith("http") ? link : "https://www.jackiequinn.co.uk" + link,
									title: title.trim(),
									priceRaw,
									bedrooms,
								});
							}
						} catch (err) {
							console.error(`Error extracting property: ${err.message}`);
						}
					});
					break;

				case 12:
					// Purplebricks
					$('[data-testid="results-list"] li').each((index, element) => {
						try {
							const $li = $(element);
							const linkEl = $li
								.find('a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]')
								.first();
							if (!linkEl.length) return;

							const priceEl = $li
								.find('[data-testid="search-result-price"], .sc-cda42038-7')
								.first();
							const priceText = priceEl.text();

							if (isSoldProperty(priceText)) return;

							const priceMatch = priceText.match(/£([\d,]+)/);
							const priceRaw = priceMatch ? priceMatch[0] : "";

							const addrEl = $li
								.find('[data-testid="search-result-address"], .sc-cda42038-10')
								.first();
							const address = addrEl.text();

							const bedEl = $li.find('[data-testid="search-result-bedrooms"]').first();
							const bedrooms = bedEl.text();

							const href = linkEl.attr("href");
							const url =
								href && href.startsWith("http")
									? href
									: href
										? `https://www.purplebricks.co.uk${href}`
										: null;

							if (url && priceRaw) {
								propertyList.push({
									url,
									title: address.trim(),
									location: "",
									priceRaw,
									bedrooms: bedrooms.trim(),
								});
							}
						} catch (err) {
							console.error(`Error extracting property: ${err.message}`);
						}
					});
					break;

				case 13:
					// Bairstow Eves
					$(".card").each((index, element) => {
						try {
							const $card = $(element);
							const linkEl = $card.find("a.card__link").first();
							const link = linkEl.attr("href");

							const titleEl = $card.find(".card__text-content").first();
							const title = titleEl.text();

							const priceEl = $card.find(".card__heading").first();
							const priceText = priceEl.text();

							if (isSoldProperty(priceText)) return;

							const priceMatch = priceText.match(/£[\d,]+/);
							const priceRaw = priceMatch ? priceMatch[0] : null;

							const bedroomsEl = $card.find(".card-content__spec-list-number").first();
							const bedroomsText = bedroomsEl.text();
							const bedroomsMatch = bedroomsText.match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

							if (link && priceRaw && title) {
								propertyList.push({
									url: link.startsWith("http") ? link : `https://www.bairstoweves.co.uk${link}`,
									title: title.trim(),
									location: "",
									priceRaw,
									bedrooms,
								});
							}
						} catch (err) {
							console.error(`Error extracting property: ${err.message}`);
						}
					});
					break;
			}

			console.log(`Found ${propertyList.length} available properties`);

			// Process each property
			for (const property of propertyList) {
				const { url, title, location, priceRaw, bedrooms } = property;
				let priceClean = priceRaw.replace(/[£,]/g, "");
				if (isRent && priceClean.includes("p/w")) {
					priceClean = priceClean.replace("p/w", "").trim();
				}
				const price = parseFloat(priceClean);
				const fullTitle = location ? `${title}, ${location}` : title;

				// Check if property exists
				const result = await updatePriceByPropertyURLOptimized(
					url,
					price,
					fullTitle,
					bedrooms,
					agentId,
					isRent,
				);

				if (!result.isExisting && !result.error) {
					// Need to fetch coordinates from detail page (only if no error)
					await crawler.addRequests([
						{
							url,
							userData: { isDetailPage: true, price, title: fullTitle, bedrooms, isRent, agentId },
						},
					]);
				}
			}
		},
	});

	await crawler.run(urls);
}

// Main scraping function using Crawlee
async function runOptimizedCombinedScraper(selectedAgentIds = null) {
	// Filter agents if specific ones are selected
	let agentsToProcess = AGENTS;
	if (selectedAgentIds && selectedAgentIds.length > 0) {
		agentsToProcess = AGENTS.filter((a) => selectedAgentIds.includes(a.id));
		console.log(`📝 Running for specific agents: ${selectedAgentIds.join(", ")}`);
	}

	console.log(
		`Starting Optimized Combined Crawlee Scraper for agents: ${agentsToProcess.map((a) => a.id).join(", ")}...`,
	);
	logMemoryUsage("START");

	let totalProcessed = 0;

	try {
		for (const agent of agentsToProcess) {
			console.log(`\n🏢 Processing ${agent.name} (Agent ${agent.id})...`);

			for (const type of agent.propertyTypes) {
				console.log(`\n📦 Processing ${type.name}...`);

				// Build URLs for all pages
				const urls = [];
				for (let pageNum = 1; pageNum <= type.totalPages; pageNum++) {
					let listingUrl;

					switch (agent.id) {
						case 4: // Marsh & Parsons
							listingUrl = `${type.baseUrl}&page=${pageNum}`;
							break;
						case 8: // Jackie Quinn
							listingUrl = `${type.baseUrl}&page=${pageNum}`;
							break;
						case 12: // Purplebricks
							listingUrl = type.baseUrl.replace(/page=\d+/, `page=${pageNum}`);
							break;
						case 13: // Bairstow Eves
							listingUrl = `${type.baseUrl}/page-${pageNum}#/`;
							break;
						case 14: // Chestertons
							listingUrl = pageNum === 1 ? type.baseUrl : `${type.baseUrl}?page=${pageNum}`;
							break;
						case 15: // Sequence Home
							listingUrl = pageNum === 1 ? `${type.baseUrl}/` : `${type.baseUrl}/page-${pageNum}/`;
							break;
					}

					urls.push(listingUrl);
				}

				// Determine which crawler to use based on agent
				if (agent.id === 4 || agent.id === 8 || agent.id === 12 || agent.id === 13) {
					// Use PlaywrightCrawler for agents that need JavaScript
					// Agent 4: Marsh & Parsons (needs JS rendering)
					// Agent 8: Jackie Quinn (needs to click map link for coordinates)
					// Agent 12: Purplebricks (needs JS rendering)
					// Agent 13: Bairstow Eves (needs to visit detail pages for coordinates)
					if (agent.id === 13) {
						// Run each listing page sequentially to avoid 429s
						for (const url of urls) {
							await scrapeWithPlaywright([url], agent.id, type.isRent);
							await new Promise((resolve) => setTimeout(resolve, 500));
						}
					} else {
						await scrapeWithPlaywright(urls, agent.id, type.isRent);
					}
				} else {
					// Use CheerioCrawler for others (faster)
					await scrapeWithCheerio(urls, agent.id, type.isRent);
				}

				totalProcessed += urls.length;
				logMemoryUsage(`After ${agent.name}`);
			}

			// Update remove status for this agent
			await updateRemoveStatus(agent.id);
			console.log(`✅ Completed ${agent.name}`);
		}

		console.log(`\n✅ All scrapers completed.`);
		console.log(`📊 Summary: ${totalProcessed} pages processed`);
		logMemoryUsage("END");
	} catch (error) {
		console.error("❌ Fatal error:", error);
		throw error;
	}
}

// Run the optimized combined scraper
// Parse command-line arguments for agent selection
const args = process.argv.slice(2);
let selectedAgents = null;

if (args.length > 0) {
	if (args[0] === "--from") {
		// Usage: node combined-scraper.js --from 8
		// Scrapes agent 8 and all agents after it
		const fromAgentId = parseInt(args[1]);
		if (!isNaN(fromAgentId)) {
			selectedAgents = AGENTS.filter((a) => a.id >= fromAgentId).map((a) => a.id);
			console.log(`\n▶️  Starting from agent ${fromAgentId}`);
		}
	} else {
		// Usage: node combined-scraper.js 8 12
		// Scrapes only agents 8 and 12
		selectedAgents = args
			.map((arg) => parseInt(arg))
			.filter((id) => !isNaN(id) && AGENTS.some((a) => a.id === id));
		if (selectedAgents.length > 0) {
			console.log(`\n▶️  Scraping specific agents: ${selectedAgents.join(", ")}`);
		}
	}
}

// Show usage info
if (selectedAgents === null) {
	console.log(`\n📖 Usage:`);
	console.log(`  node combined-scraper.js              # Scrape all agents`);
	console.log(`  node combined-scraper.js 8            # Scrape only agent 8`);
	console.log(`  node combined-scraper.js 4 8 12       # Scrape agents 4, 8, and 12`);
	console.log(`  node combined-scraper.js --from 8     # Scrape agent 8 and onwards`);
	console.log(`\n`);
}

runOptimizedCombinedScraper(selectedAgents)
	.then(() => {
		console.log("✅ All done!");
		process.exit(0);
	})
	.catch((err) => {
		console.error("❌ Scraper error:", err);
		process.exit(1);
	});
