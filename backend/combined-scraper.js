const { CheerioCrawler, PlaywrightCrawler } = require("crawlee");
const cheerio = require("cheerio");
const { updatePriceByPropertyURL, updateRemoveStatus, promisePool } = require("./db.js");
const { isSoldProperty } = require("./lib/property-helpers.js");
const {
	logMemoryUsage,
	runAgent13Scraper,
	runAgent14Scraper,
	runAgent15Scraper,
	runAgent16Scraper,
	runAgent18Scraper,
	runAgent19Scraper,
	runAgent22Scraper,
	runAgent24Scraper,
} = require("./lib/scraper-utils.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

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
		id: 18,
		name: "Moveli",
		propertyTypes: [
			{
				name: "Sales",
				baseUrl:
					"https://www.moveli.co.uk/test/properties?category=for-sale&searchKeywords=&status=all&maxPrice=any&minBeds=any&sortOrder=price-desc",
				isRent: false,
				totalPages: 1,
			},
			{
				name: "Lettings",
				baseUrl:
					"https://www.moveli.co.uk/test/properties?category=for-rent&searchKeywords=&status=all&maxPrice=any&minBeds=any&sortOrder=price-desc",
				isRent: true,
				totalPages: 1,
			},
		],
	},
];

// Generic Cheerio crawler for agents that work with simple HTTP requests
async function scrapeWithCheerio(urls, agentId, isRent) {
	// Storage for agent 13 detail page requests
	const agent13DetailPages = [];

	const crawler = new CheerioCrawler({
		requestHandlerTimeoutSecs: 60,
		maxRequestRetries: 2,
		maxConcurrency: agentId === 13 ? 1 : 5, // Sequential for agent 13
		async requestHandler({ $, request, body }) {
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
			} else if (agentId === 13) {
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

				if (agentId === 13) {
					// For agent 13, collect detail pages to process later with Playwright
					agent13DetailPages.push({
						url,
						userData: {
							isDetailPage: true,
							price,
							title: title.trim(),
							bedrooms,
							isRent,
							agentId,
						},
					});
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

	// For agent 13, process detail pages with Playwright sequentially
	if (agentId === 13 && agent13DetailPages.length > 0) {
		console.log(`\n📍 Processing ${agent13DetailPages.length} detail pages for agent 13...`);
		const detailPageCrawler = await createAgent13DetailCrawler();
		await detailPageCrawler.addRequests(agent13DetailPages);
		await detailPageCrawler.run();
	}
}

// Create a dedicated Playwright crawler for agent 13 detail pages
async function createAgent13DetailCrawler() {
	const browserWSEndpoint =
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;

	console.log(`🌐 Connecting to browserless for detail pages: ${browserWSEndpoint.split("?")[0]}`);

	return new PlaywrightCrawler({
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},
		requestHandlerTimeoutSecs: 60,
		maxRequestRetries: 2,
		maxConcurrency: 1, // Sequential processing
		preNavigationHooks: [
			async ({ page }) => {
				// Block unnecessary resources
				await page.route("**/*", (route) => {
					const resourceType = route.request().resourceType();
					if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
						route.abort();
					} else {
						route.continue();
					}
				});
			},
		],
		async requestHandler({ page, request }) {
			const { price, title, bedrooms, isRent, agentId } = request.userData;

			// Wait for page to load
			await page.waitForTimeout(1500);

			// Extract HTML and coordinates
			const html = await page.content();
			const coords = await extractCoordinatesFromHTML(html);

			// Save to database
			await updatePriceByPropertyURL(
				request.url,
				price,
				title,
				bedrooms,
				agentId,
				isRent,
				coords.latitude,
				coords.longitude,
			);

			if (coords.latitude && coords.longitude) {
				console.log(
					`✅ Created: ${request.url.substring(0, 50)}... | Price: £${price} | Coords: ${coords.latitude}, ${coords.longitude}`,
				);
				console.log(
					`✅ New property: ${title.substring(0, 40)}... (£${price}) - Coords: ${coords.latitude}, ${coords.longitude}`,
				);
			} else {
				console.log(
					`✅ Created: ${request.url.substring(0, 50)}... | Price: £${price} | No coords`,
				);
				console.log(`⚠️ New property: ${title.substring(0, 40)}... (£${price}) - No coords found`);
			}

			// Add delay between detail page requests to prevent 429
			await new Promise((resolve) => setTimeout(resolve, 1200));
		},
		failedRequestHandler: async ({ request }) => {
			console.log(`⚠️ Detail page failed: ${request.url}`);
		},
	});
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
		requestHandlerTimeoutSecs: agentId === 13 ? 180 : 60, // Agent 13 needs more time for pages with many properties
		maxRequestRetries: 2,
		maxConcurrency: agentId === 8 || agentId === 13 ? 1 : 2, // Agents 8 & 13 need sequential requests to avoid 429 errors
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
					// Add to queue for detail page processing
					await crawler.addRequests(
						[
							{
								url,
								userData: {
									isDetailPage: true,
									price,
									title: fullTitle,
									bedrooms,
									isRent,
									agentId,
								},
							},
						],
						{ waitForAllRequestsToBeAdded: true },
					);
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
					}

					urls.push(listingUrl);
				}

				// Determine which crawler to use based on agent
				if (agent.id === 4 || agent.id === 8 || agent.id === 12) {
					// Use PlaywrightCrawler for agents that need JavaScript
					// Agent 4: Marsh & Parsons (needs JS rendering)
					// Agent 8: Jackie Quinn (needs to click map link for coordinates)
					// Agent 12: Purplebricks (needs JS rendering)
					await scrapeWithPlaywright(urls, agent.id, type.isRent);
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
let agent13StartPage = 1;
let agent14StartPage = 1;
let agent15StartPage = 1;
let agent16StartPage = 1;
let agent18StartPage = 1;
let agent19StartPage = 1;

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
		// Check if first argument is 13 or 14 (special handling)
		const firstAgentId = parseInt(args[0]);
		if (firstAgentId === 13) {
			// Usage: node combined-scraper.js 13 [startPage]
			// If second argument exists and is a number, use it as start page
			if (args.length > 1) {
				const startPage = parseInt(args[1]);
				if (!isNaN(startPage) && startPage > 0) {
					agent13StartPage = startPage;
					console.log(`\n▶️  Running Agent 13 from page ${agent13StartPage}`);
				}
			} else {
				console.log(`\n▶️  Running Agent 13 from page 1`);
			}
			selectedAgents = [13];
		} else if (firstAgentId === 14) {
			// Usage: node combined-scraper.js 14 [startPage]
			if (args.length > 1) {
				const startPage = parseInt(args[1]);
				if (!isNaN(startPage) && startPage > 0) {
					agent14StartPage = startPage;
					console.log(`\n▶️  Running Agent 14 from page ${agent14StartPage}`);
				}
			} else {
				console.log(`\n▶️  Running Agent 14 from page 1`);
			}
			selectedAgents = [14];
		} else if (firstAgentId === 15) {
			// Usage: node combined-scraper.js 15 [startPage]
			if (args.length > 1) {
				const startPage = parseInt(args[1]);
				if (!isNaN(startPage) && startPage > 0) {
					agent15StartPage = startPage;
					console.log(`\n▶️  Running Agent 15 from page ${agent15StartPage}`);
				}
			} else {
				console.log(`\n▶️  Running Agent 15 from page 1`);
			}
			selectedAgents = [15];
		} else if (firstAgentId === 16) {
			// Usage: node combined-scraper.js 16 [startPage]
			if (args.length > 1) {
				const startPage = parseInt(args[1]);
				if (!isNaN(startPage) && startPage > 0) {
					agent16StartPage = startPage;
					console.log(`\n▶️  Running Agent 16 from page ${agent16StartPage}`);
				}
			} else {
				console.log(`\n▶️  Running Agent 16 from page 1`);
			}
			selectedAgents = [16];
		} else if (firstAgentId === 18) {
			// Usage: node combined-scraper.js 18
			console.log(`\n▶️  Running Agent 18`);
			selectedAgents = [18];
		} else if (firstAgentId === 19) {
			// Usage: node combined-scraper.js 19
			console.log(`\n▶️  Running Agent 19`);
			selectedAgents = [19];
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
}

// Show usage info
if (selectedAgents === null) {
	console.log(`\n📖 Usage:`);
	console.log(`  node combined-scraper.js              # Scrape all agents`);
	console.log(`  node combined-scraper.js 8            # Scrape only agent 8`);
	console.log(`  node combined-scraper.js 4 8 12       # Scrape agents 4, 8, and 12`);
	console.log(`  node combined-scraper.js 13 20        # Scrape agent 13 starting from page 20`);
	console.log(`  node combined-scraper.js 14 10        # Scrape agent 14 starting from page 10`);
	console.log(`  node combined-scraper.js 15 50        # Scrape agent 15 starting from page 50`);
	console.log(`  node combined-scraper.js 16 30        # Scrape agent 16 starting from page 30`);
	console.log(`  node combined-scraper.js 18           # Scrape agent 18 (Moveli)`);
	console.log(`  node combined-scraper.js 19           # Scrape agent 19 (Snellers)`);
	console.log(`  node combined-scraper.js --from 8     # Scrape agent 8 and onwards`);
	console.log(`\n`);
}

// Handle agents 13 and 14 separately by spawning their dedicated scripts
if (selectedAgents && selectedAgents.length === 1 && selectedAgents[0] === 13) {
	runAgent13Scraper(agent13StartPage)
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
} else if (selectedAgents && selectedAgents.length === 1 && selectedAgents[0] === 14) {
	runAgent14Scraper(agent14StartPage)
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
} else if (selectedAgents && selectedAgents.length === 1 && selectedAgents[0] === 15) {
	runAgent15Scraper(agent15StartPage)
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
} else if (selectedAgents && selectedAgents.length === 1 && selectedAgents[0] === 16) {
	runAgent16Scraper(agent16StartPage)
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
} else if (selectedAgents && selectedAgents.length === 1 && selectedAgents[0] === 18) {
	runAgent18Scraper()
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
} else if (selectedAgents && selectedAgents.length === 1 && selectedAgents[0] === 19) {
	runAgent19Scraper()
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
} else if (selectedAgents && selectedAgents.length === 1 && selectedAgents[0] === 22) {
	runAgent22Scraper()
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
} else if (selectedAgents && selectedAgents.length === 1 && selectedAgents[0] === 24) {
	runAgent24Scraper()
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
} else {
	// Run normal combined scraper
	runOptimizedCombinedScraper(selectedAgents)
		.then(() => {
			console.log("✅ All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("❌ Scraper error:", err);
			process.exit(1);
		});
}
