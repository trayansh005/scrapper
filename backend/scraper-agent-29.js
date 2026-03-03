// Allday & Miller scraper using Playwright with Crawlee
// Agent ID: 29
// Homeflow based - using JSON extraction from script tag
//
// Usage:
// node backend/scraper-agent-29.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { processPropertyWithCoordinates } = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

// Reduce logging noise
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 29;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label, totalPages } = request.userData;
	logger.page(pageNum, label, `Processing ${request.url}`, totalPages || null);

	try {
		// Accept cookies if present
		const cookieButton = await page.$("#cookieconsent:active button[aria-label='allow cookies']");
		if (cookieButton) {
			await cookieButton.click().catch(() => {});
			await page.waitForTimeout(1000);
		}

		// Homeflow sites often put property data in a script tag
		const properties = await page.evaluate(() => {
			if (typeof Ctesius === "undefined" || !Ctesius.getConfig) {
				// Fallback to manual extraction if Ctesius is not ready
				const scripts = Array.from(document.querySelectorAll("script"));
				const propScript = scripts.find((s) =>
					s.innerText.includes("Ctesius.addConfig('properties'"),
				);
				if (!propScript) return [];

				try {
					const content = propScript.innerText;
					const match = content.match(/Ctesius\.addConfig\('properties',\s*(\{[\s\S]*?\})\)/);
					if (match && match[1]) {
						// The site sometimes has .properties after the object, which breaks JSON.parse
						// We'll try to parse just the object
						const data = JSON.parse(match[1]);
						return (data.properties || []).map((p) => ({
							link: p.property_url.startsWith("http")
								? p.property_url
								: window.location.origin + p.property_url,
							title: p.display_address || "Property",
							priceRaw: p.price,
							bedrooms: p.bedrooms ? parseInt(p.bedrooms, 10) : null,
							lat: p.lat,
							lng: p.lng,
							status: p.status,
						}));
					}
				} catch (e) {}
				return [];
			}

			const props = Ctesius.getConfig("properties");
			if (!props || !Array.isArray(props)) return [];

			return props.map((p) => ({
				link: p.property_url.startsWith("http")
					? p.property_url
					: window.location.origin + p.property_url,
				title: p.display_address || "Property",
				priceRaw: p.price,
				bedrooms: p.bedrooms ? parseInt(p.bedrooms, 10) : null,
				lat: p.lat,
				lng: p.lng,
				status: p.status,
			}));
		});

		logger.page(
			pageNum,
			label,
			`Found ${properties.length} properties via JSON`,
			totalPages || null,
		);

		for (const property of properties) {
			if (processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			if (isSoldProperty(property.status || "")) {
				logger.property(
					pageNum,
					label,
					property.title,
					property.priceRaw,
					property.link,
					isRental,
					totalPages || null,
					"UNCHANGED",
				);
				continue;
			}

			const price = parsePrice(property.priceRaw);
			if (!price) {
				logger.property(
					pageNum,
					label,
					property.title,
					"N/A",
					property.link,
					isRental,
					totalPages || null,
					"ERROR",
				);
				continue;
			}

			// Since we have coordinates in the JSON, we can use processPropertyWithCoordinates directly
			// This saves us a navigation to the detail page
			await processPropertyWithCoordinates(
				property.link,
				price,
				property.title,
				property.bedrooms,
				AGENT_ID,
				isRental,
				null, // HTML not needed if we have coords
				property.lat,
				property.lng,
			);

			counts.totalScraped++;
			counts.totalSaved++;
			if (isRental) counts.savedRentals++;
			else counts.savedSales++;

			logger.property(
				pageNum,
				label,
				property.title,
				`£${price}`,
				property.link,
				isRental,
				totalPages || null,
				"CREATED",
			);
		}

		// Throttle requests
		await new Promise((r) => setTimeout(r, 2000));
	} catch (error) {
		logger.error("Error in handleListingPage", error, pageNum, label);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler() {
	return new PlaywrightCrawler({
		maxConcurrency: 1, // Polite mode
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],
		launchContext: {
			launchOptions: {
				headless: true,
				args: [
					"--no-sandbox",
					"--disable-setuid-sandbox",
					"--disable-dev-shm-usage",
					"--disable-accelerated-2d-canvas",
					"--disable-gpu",
				],
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			logger.error(`Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeAlldayMiller() {
	logger.step(`Starting Allday & Miller scraper (Agent ${AGENT_ID})`);
	const startPageArg = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	const startPage = Number.isFinite(startPageArg) && startPageArg > 0 ? startPageArg : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	const crawler = createCrawler();

	const PROPERTY_TYPES = [
		{
			urlBase: "https://www.alldayandmiller.co.uk/properties/sales",
			isRental: false,
			label: "SALES",
			totalPages: 18, // Total props / 12 (approx 212/12 = 17.6)
		},
		{
			urlBase: "https://www.alldayandmiller.co.uk/properties/lettings",
			isRental: true,
			label: "RENTALS",
			totalPages: 2, // Usually smaller for lettings
		},
	];

	const allRequests = [];
	for (const type of PROPERTY_TYPES) {
		for (let p = Math.max(1, startPage); p <= type.totalPages; p++) {
			allRequests.push({
				url: p === 1 ? type.urlBase : `${type.urlBase}/page-${p}`,
				userData: {
					pageNum: p,
					isRental: type.isRental,
					label: type.label,
					totalPages: type.totalPages,
				},
			});
		}
	}

	await crawler.run(allRequests);

	logger.step(
		`Finished Allday & Miller - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, Sales: ${counts.savedSales}, Rentals: ${counts.savedRentals}`,
	);

	if (!isPartialRun) {
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn(`Partial run detected (startPage=${startPage}). Skipping updateRemoveStatus.`);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeAlldayMiller();
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
