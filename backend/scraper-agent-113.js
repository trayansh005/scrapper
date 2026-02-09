// Carter Jonas scraper using Playwright with Crawlee
// Agent ID: 113
//
// Usage:
// node backend/scraper-agent-113.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 113;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function parsePrice(priceText) {
	if (!priceText) return null;

	// Extract everything before non-numeric text like "Asking price" or "Offers in excess of"
	const match = priceText.match(/[£€$]\s*([\d,]+)/);
	if (!match) {
		const fallbackMatch = priceText.match(/([\d,]{4,})/);
		if (fallbackMatch) {
			const priceClean = fallbackMatch[1].replace(/,/g, "");
			return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
		}
		return null;
	}

	const priceClean = match[1].replace(/,/g, "");
	return priceClean.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

function parsePropertyCard($, element) {
	try {
		const $li = $(element);

		// Check for "Sold" status in any badge or text
		// Using the user-provided snippet indicator (bg-plum)
		const soldBadgeText = $li.find(".bg-plum").text().trim();
		if (soldBadgeText.toLowerCase().includes("sold")) return null;

		// General sold keyword check on the card
		if (isSoldProperty($li.text())) return null;

		const h3 = $li.find("h3");
		const h4 = $li.find("h4");
		
		if (!h3.length || !h4.length) return null;

		const title = h3.text().trim();
		const priceAttr = h4.text().trim();
		const price = parsePrice(priceAttr);

		const linkRel = h3.find("a").attr("href") || $li.find("a").attr("href");
		if (!linkRel) return null;
		const link = linkRel.startsWith("http") ? linkRel : `https://www.carterjonas.co.uk${linkRel}`;

		// Specs (Bedrooms)
		const specs = [];
		$li.find("ul li").each((j, specEl) => {
			specs.push($(specEl).text().trim());
		});
		const bedsMatch = specs.find((s) => /^\d+$/.test(s));
		const bedrooms = bedsMatch || null;

		if (link && title) {
			return { link, title, price, bedrooms };
		}
		return null;
	} catch (error) {
		return null;
	}
}

function parseListingPage(htmlContent) {
	const $ = cheerio.load(htmlContent);
	const properties = [];

	$("li").each((index, element) => {
		const property = parsePropertyCard($, element);
		if (property) {
			properties.push(property);
		}
	});

	return properties;
}

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
	await sleep(1000);

	const detailPage = await browserContext.newPage();

	try {
		// Block unnecessary resources
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
			timeout: 35000,
		});

		const htmlContent = await detailPage.content();
		
		const coords = await detailPage.evaluate(() => {
			try {
				// Check meta tags first
				const latMeta = document.querySelector('meta[property="place:location:latitude"]');
				const lonMeta = document.querySelector('meta[property="place:location:longitude"]');
				if (latMeta && lonMeta) {
					return { lat: parseFloat(latMeta.content), lon: parseFloat(lonMeta.content) };
				}

				// Fallback to script data or regex
				const html = document.documentElement.innerHTML;
				const latMatch = html.match(/"latitude":\s*(-?\d+\.\d+)/i);
				const lonMatch = html.match(/"longitude":\s*(-?\d+\.\d+)/i);
				if (latMatch && lonMatch) {
					return { lat: parseFloat(latMatch[1]), lon: parseFloat(lonMatch[1]) };
				}
			} catch (e) {}
			return null;
		});

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			htmlContent,
			coords ? coords.lat : null,
			coords ? coords.lon : null
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (error) {
		console.error(`❌ Error scraping detail page ${property.link}:`, error.message);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(`📋 [${label}] Page ${pageNum} - ${request.url}`);

	// Handle cookie dismissal if present
	const cookieButton = page.getByRole("button", { name: "Accept All Cookies" });
	if (await cookieButton.isVisible()) {
		await cookieButton.click();
		await page.waitForTimeout(1000);
	}

	await page.waitForTimeout(2000);
	await page.waitForSelector("li h3", { timeout: 30000 }).catch(() => {});

	const htmlContent = await page.content();
	const properties = parseListingPage(htmlContent);

	console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

	for (const property of properties) {
		// Skip properties with no price
		if (!property.price || property.price === "0") {
			console.log(`⏩ Skipping property (no price): ${property.title}`);
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
		);

		if (result.updated) {
			stats.totalSaved++;
		}

		if (!result.isExisting && !result.error) {
			console.log(`🆕 Scraping detail for new property: ${property.title}`);
			await scrapePropertyDetail(page.context(), property, isRental);
		}
	}

	// Dynamic Pagination
	const nextButton = page.getByRole("button", { name: "Next" });
	if (await nextButton.isVisible() && !await nextButton.isDisabled()) {
		const currentUrl = new URL(request.url);
		let nextP = pageNum + 1;
		currentUrl.searchParams.set("page", nextP.toString());
		
		// Optional: Check total pages to avoid infinite loop if button stays visible
		const paginationInfo = await page.evaluate(() => {
			const el = document.querySelector('nav[aria-label="Pagination"] + div');
			if (el) {
				const match = el.innerText.match(/of\s+(\d+)/);
				return match ? parseInt(match[1]) : 0;
			}
			return 0;
		});

		const totalResults = paginationInfo;
		const totalPages = Math.ceil(totalResults / 12); // Carter Jonas uses 12 per page

		if (nextP <= totalPages) {
			await crawler.addRequests([{
				url: currentUrl.toString(),
				userData: { pageNum: nextP, isRental, label }
			}]);
		}
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		requestHandlerTimeoutSecs: 360,
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
			},
		},
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			console.error(`❌ Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeCarterJonas() {
	console.log(`\n🚀 Starting Carter Jonas scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [
		{
			url: `https://www.carterjonas.co.uk/property-search?division=Homes&area=GreaterLondon&toBuy=true&sortOrder=HighestPriceFirst&page=${startPage}`,
			userData: { pageNum: startPage, isRental: false, label: "SALES" },
		},
		{
			url: `https://www.carterjonas.co.uk/property-search?division=Homes&area=GreaterLondon&toBuy=false&sortOrder=HighestPriceFirst&page=${startPage}`,
			userData: { pageNum: startPage, isRental: true, label: "RENTALS" },
		}
	];

	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(`\n✅ Finished - Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`);
}

(async () => {
	try {
		await scrapeCarterJonas();
		await updateRemoveStatus(AGENT_ID);
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err);
		process.exit(1);
	}
})();
