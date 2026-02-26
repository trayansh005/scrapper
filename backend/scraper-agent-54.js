// Leaders scraper using Playwright with Crawlee
// Agent ID: 54
// Usage:
// node backend/scraper-agent-54.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 54;
let totalScraped = 0;
let totalSaved = 0;
const processedUrls = new Set();

function formatPrice(num) {
	if (!num || isNaN(num)) return "£0";
	return "£" + Number(num).toLocaleString("en-GB");
}

// Configuration for Leaders
// 211 pages sales, 344 pages rent, total ~1686 sales + 2750 rent properties
const PROPERTY_TYPES = [
	// {
	// 	// Sales
	// 	urlBase: "https://www.leaders.co.uk/properties/for-sale",
	// 	totalPages: 211,
	// 	recordsPerPage: 8,
	// 	isRental: false,
	// 	label: "SALES",
	// },
	{
		// Rentals
		urlBase: "https://www.leaders.co.uk/properties/to-rent",
		totalPages: 344,
		recordsPerPage: 8,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapeLeaders() {
	console.log(`\n🚀 Starting Leaders scraper (Agent ${AGENT_ID})...\n`);

	function getBrowserlessEndpoint() {
		return (
			process.env.BROWSERLESS_WS_ENDPOINT ||
			`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
		);
	}


	async function scrapePropertyDetail(browserContext, property) {
		await new Promise((r) => setTimeout(r, 700));

		const detailPage = await browserContext.newPage();

		try {
			await detailPage.route("**/*", (route) => {
				const resourceType = route.request().resourceType();
				if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
					route.abort();
				} else {
					route.continue();
				}
			});

			await detailPage.goto(property.link, {
				waitUntil: "domcontentloaded",
				timeout: 90000,
			});

			await detailPage.waitForTimeout(1500);

			const htmlContent = await detailPage.content();
			const coords = await extractCoordinatesFromHTML(htmlContent);

			return {
				coords: {
					latitude: coords.latitude || null,
					longitude: coords.longitude || null,
				},
			};
		} catch (error) {
			return null;
		} finally {
			await detailPage.close();
		}
	}

	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint: getBrowserlessEndpoint(),
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},

		async requestHandler({ page, request }) {
			const { pageNum, isRental, label } = request.userData;

			console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

			await page.waitForTimeout(700);

			await page
				.waitForSelector(".property-card-wrapper", { timeout: 20000 })
				.catch(() => console.log(`⚠️ No property cards found on page ${pageNum}`));

			const properties = await page.evaluate(() => {
				try {
					const items = Array.from(document.querySelectorAll(".property-card-wrapper"));
					return items
						.map((el) => {
							const linkEl = el.querySelector("a[href]");
							const href = linkEl ? linkEl.getAttribute("href") : null;
							const link = href
								? href.startsWith("http")
									? href
									: href.startsWith("/")
										? "https://www.leaders.co.uk" + href
										: "https://www.leaders.co.uk/" + href
								: null;

							const title = el.querySelector(".property-title h2")?.textContent?.trim() || "";
							const price = el.querySelector(".property-price")?.textContent?.trim() || "";

							let bedrooms = null;
							const bedroomsEls = el.querySelectorAll("li.list-inline-item");
							if (bedroomsEls.length > 1) {
								bedrooms = bedroomsEls[1].textContent.trim();
							}

							const statusText = el.innerText || "";

							return { link, price, title, bedrooms, statusText };
						})
						.filter((p) => p.link);
				} catch (e) {
					return [];
				}
			});

			// Fix links for rentals - no need to replace, as it's correct
			// if (isRental) {
			// 	properties.forEach((p) => {
			// 		if (p.link) {
			// 			p.link = p.link.replace("properties-to-rent", "properties-for-rent");
			// 		}
			// 	});
			// }

			console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

			const batchSize = 5;
			for (let i = 0; i < properties.length; i += batchSize) {
				const batch = properties.slice(i, i + batchSize);

				await Promise.all(
					batch.map(async (property) => {
						if (!property.link) return;

						if (processedUrls.has(property.link)) return;
						processedUrls.add(property.link);
						if (isSoldProperty(property.statusText || "")) return;

						// let coords = { latitude: property.lat || null, longitude: property.lng || null };

						// if (!coords.latitude || !coords.longitude) {
						// 	const detailPage = await page.context().newPage();
						// 	try {
						// 		await detailPage.goto(property.link, {
						// 			waitUntil: "domcontentloaded",
						// 			timeout: 30000,
						// 		});
						// 		await detailPage.waitForTimeout(400);

						// 		const detailCoords = await detailPage.evaluate(() => {
						// 			try {
						// 				// Get all script tags and search for latitude/longitude
						// 				const scripts = Array.from(document.querySelectorAll("script:not([src])"));

						// 				for (const script of scripts) {
						// 					const text = script.textContent;

						// 					// Try multiple patterns for latitude/longitude
						// 					const patterns = [
						// 						{
						// 							lat: /"latitude"\s*:\s*([0-9.+-]+)/,
						// 							lng: /"longitude"\s*:\s*([0-9.+-]+)/,
						// 						},
						// 						{
						// 							lat: /"latitude"\s*:\s*"([0-9.+-]+)"/,
						// 							lng: /"longitude"\s*:\s*"([0-9.+-]+)"/,
						// 						},
						// 						{
						// 							lat: /\\"latitude\\"\s*:\s*([0-9.+-]+)/,
						// 							lng: /\\"longitude\\"\s*:\s*([0-9.+-]+)/,
						// 						},
						// 						{
						// 							lat: /\\"latitude\\"\s*:\s*\\"([0-9.+-]+)\\"/,
						// 							lng: /\\"longitude\\"\s*:\s*\\"([0-9.+-]+)\\"/,
						// 						},
						// 					];

						// 					for (const pattern of patterns) {
						// 						const latMatch = text.match(pattern.lat);
						// 						const lngMatch = text.match(pattern.lng);

						// 						if (latMatch && lngMatch) {
						// 							const lat = parseFloat(latMatch[1]);
						// 							const lng = parseFloat(lngMatch[1]);
						// 							if (!isNaN(lat) && !isNaN(lng)) {
						// 								return { lat, lng };
						// 							}
						// 						}
						// 					}
						// 				}

						// 				return null;
						// 			} catch (e) {
						// 				return null;
						// 			}
						// 		});

						// 		console.log(`Coords for ${property.link}: ${JSON.stringify(detailCoords)}`);

						// 		if (detailCoords) {
						// 			let lat = detailCoords.lat;
						// 			let lng = detailCoords.lng;
						// 			// Heuristic for inverted coordinates (UK region)
						// 			if (
						// 				Math.abs(lat) <= 10 &&
						// 				lng >= 49 &&
						// 				lng <= 61 &&
						// 				!(lat >= 49 && lat <= 61 && Math.abs(lng) <= 10)
						// 			) {
						// 				const t = lat;
						// 				lat = lng;
						// 				lng = t;
						// 			}
						// 			coords.latitude = lat;
						// 			coords.longitude = lng;
						// 		}
						// 	} catch (err) {
						// 		// ignore detail page errors
						// 	} finally {
						// 		await detailPage.close();
						// 	}
						// }

						const price = formatPriceUk(property.price);
						if (!price) return;

						const result = await updatePriceByPropertyURLOptimized(
							property.link,
							price,
							property.title,
							property.bedrooms,
							AGENT_ID,
							isRental
						);

						if (result.updated) {
							totalSaved++;
						}

						if (!result.isExisting && !result.error) {
							const detail = await scrapePropertyDetail(page.context(), property);

							await updatePriceByPropertyURL(
								property.link.trim(),
								price,
								property.title,
								property.bedrooms,
								AGENT_ID,
								isRental,
								detail?.coords?.latitude || null,
								detail?.coords?.longitude || null
							);

							totalSaved++;
							totalScraped++;
						}
					})
				);

				await new Promise((resolve) => setTimeout(resolve, 300));
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
		`\n✅ Completed Leaders - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`
	);
}

(async () => {
	try {
		await scrapeLeaders();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
