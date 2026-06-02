// REDAC Strattons Property scraper using Playwright with Crawlee
// Agent ID: 262
// Updated 2026-04-18: Fixed Sales + Rentals both scraping + robust bedroom extraction

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 262;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://redacstrattons.com/property-for-sale/?location&office&type&min_price&max_price&min_beds&exclude_sold=true",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://redacstrattons.com/property-to-rent/?location&office&type&min_price&max_price&min_beds&exclude_let=true",
		isRental: true,
		label: "RENTALS",
	},
];

const counts = { totalScraped: 0, totalSaved: 0, savedSales: 0, savedRentals: 0 };
const processedUrls = new Set();

// ============================================================================
// UTILITY
// ============================================================================

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		if (["image", "font", "media"].includes(route.request().resourceType())) return route.abort();
		return route.continue();
	});
}

function isSkippableProperty(statusText) {
	if (!statusText) return false;
	const t = statusText.toLowerCase();
	return /under offer|sold|let|rented/.test(t);
}

function normalizePropertyUrl(url) {
	try {
		const u = new URL(url);
		const parts = u.pathname.split("/").filter(Boolean);

		if (parts.length >= 2 && parts[0] === "property") {
			const slug = parts[1].replace(/-\d+$/, "");
			u.pathname = `/property/${slug}`;
		}

		return u.toString();
	} catch {
		return url;
	}
}

// ============================================================================
// LISTING HANDLER - IMPROVED VERSION
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, baseUrl, totalPages } = request.userData;

	logger.page(pageNum, label, request.url, totalPages);

	try {
		// 1. Initial load
		await page.waitForLoadState("domcontentloaded", { timeout: 20000 });

		// 2. Better waiting for content to load (critical for Nuxt SSR + client-side rendering)
		await Promise.race([
			page.waitForSelector("h4:contains('properties found')", { timeout: 15000 }),
			page.waitForSelector("article, .property-card, .listing-item", { timeout: 15000 }),
			page.waitForTimeout(10000)
		]).catch(() => { });

		// 3. Optional: Trigger search / refresh if needed
		try {
			const submitBtn = await page.locator("input[type='submit'], button[type='submit'], .button--primary").first();
			if (await submitBtn.isVisible({ timeout: 3000 })) {
				await submitBtn.click();
				await page.waitForTimeout(4000);
			}
		} catch (e) { }

		// 4. Wait a bit more for dynamic content
		await page.waitForTimeout(3000);

		// 5. Extract properties
		const properties = await page.evaluate(() => {
			const results = [];
			const seen = new Set();

			// Broader and more reliable selectors
			const cardSelectors = [
				"article",
				"div[class*='property']",
				"div[class*='listing']",
				"li[class*='property']",
				".card",
				".listing-item"
			];

			let cards = [];
			for (const sel of cardSelectors) {
				cards = Array.from(document.querySelectorAll(sel));
				if (cards.length > 0) break;
			}

			for (const card of cards) {
				const linkEl = card.querySelector("a[href*='/property/']");
				if (!linkEl) continue;

				const href = linkEl.getAttribute("href");
				if (!href || seen.has(href)) continue;
				seen.add(href);

				const fullLink = href.startsWith("http")
					? href
					: new URL(href, location.origin).href;

				const rawText = (card.textContent || "").replace(/\s+/g, " ").trim();

				const titleEl = card.querySelector("h2, h3, .title, .address, .property-title");
				const title = (titleEl?.textContent || "Property").trim();

				// Status
				let statusText = "";
				const statusEl = card.querySelector("[class*='status'], [class*='badge'], [class*='tag'], .sold, .let, .under-offer");
				if (statusEl) statusText = statusEl.textContent.trim();

				// Price
				const priceMatch = rawText.match(/£[\d,]+(?:\.\d+)?/);
				const priceRaw = priceMatch ? priceMatch[0] : "";

				// Bedrooms - improved regex
				let bedText = "";
				const bedPatterns = [
					/(\d+)\s*(?:bed|beds|bedroom|bedrooms)\b/i,
					/bed(?:room)?s?\s*[:=]?\s*(\d+)/i,
					/\b(\d+)\s*[xX]\s*\d+/i,
					/\b(studio)\b/i
				];

				for (const re of bedPatterns) {
					const match = rawText.match(re);
					if (match) {
						bedText = match[0];
						break;
					}
				}

				results.push({
					link: fullLink,
					title,
					priceRaw,
					bedText,
					statusText,
					rawText: rawText.substring(0, 300) // for debugging
				});
			}

			return results;
		});

		logger.page(pageNum, label, `Found ${properties.length} properties on page ${pageNum}`);

		// 6. Process each property
		for (const prop of properties) {
			const normalizedUrl = normalizePropertyUrl(prop.link);

			if (!prop.link || processedUrls.has(normalizedUrl)) continue;
			processedUrls.add(normalizedUrl);

			// Skip sold / under offer / let properties
			if (isSkippableProperty(prop.statusText)) {
				logger.property(pageNum, label, prop.title.substring(0, 60), "N/A", prop.link, isRental, totalPages, "SKIPPED");
				continue;
			}

			const price = parsePrice(prop.priceRaw);
			if (!price) {
				logger.property(pageNum, label, prop.title.substring(0, 60), "INVALID PRICE", prop.link, isRental, totalPages, "SKIPPED");
				continue;
			}

			// Parse bedrooms
			let bedrooms = null;
			if (prop.bedText) {
				const lower = prop.bedText.toLowerCase();
				if (lower.includes("studio")) bedrooms = 0;
				else {
					const numMatch = prop.bedText.match(/\d+/);
					if (numMatch) bedrooms = parseInt(numMatch[0], 10);
				}
			}

			// Save/Update in DB
			const result = await updatePriceByPropertyURLOptimized(
				prop.link, price, prop.title, bedrooms, AGENT_ID, isRental
			);

			let action = "EXISTING";
			if (result.updated) action = "UPDATED";
			else if (!result.isExisting) action = "CREATED";

			if (!result.isExisting) {
				try {
					const detailPage = await page.context().newPage();
					await blockNonEssentialResources(detailPage);
					await detailPage.goto(prop.link, { waitUntil: "networkidle", timeout: 45000 });

					const html = await detailPage.content();
					await processPropertyWithCoordinates(
						prop.link, price, prop.title, bedrooms, AGENT_ID, isRental, html
					);

					await detailPage.close().catch(() => { });
					action = "CREATED";
					counts.totalSaved++;
					if (isRental) counts.savedRentals++;
					else counts.savedSales++;
				} catch (e) {
					logger.error(`Detail scraping failed for ${prop.link}`, e.message);
				}
			}

			logger.property(
				pageNum,
				label,
				`${prop.title.substring(0, 55)}${bedrooms !== null ? ` (${bedrooms} bed)` : ""}`,
				formatPriceDisplay(price, isRental),
				prop.link,
				isRental,
				totalPages,
				action
			);

			await sleep(action === "CREATED" ? 2500 : 800);
		}

		// 7. Pagination
		const hasNext = await page.evaluate(() => {
			return !!document.querySelector("a[rel='next'], a.next, .pagination__next, .next-page, button.next");
		});

		if (hasNext && pageNum < 30) {  // safety limit
			const nextPageNum = pageNum + 1;
			const nextUrl = baseUrl.includes("?")
				? `${baseUrl}&page=${nextPageNum}`
				: `${baseUrl}?page=${nextPageNum}`;

			await crawler.addRequests([{
				url: nextUrl,
				userData: { ...request.userData, pageNum: nextPageNum }
			}]);
		}

	} catch (error) {
		logger.error(`Error on page ${pageNum} (${label})`, error.message);
	}
}

// ============================================================================
// CRAWLER + MAIN
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 240,
		preNavigationHooks: [({ page }) => blockNonEssentialResources(page)],
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1280, height: 900 }
			}
		},
		requestHandler: handleListingPage,
	});
}

async function scrapeRedacStrattons() {
	logger.step("Starting REDAC Strattons Scraper (Sales + Rentals) - v2026-04-18");

	const args = process.argv.slice(2);
	const startPage = args.length ? parseInt(args[0]) || 1 : 1;
	const browserWSEndpoint = process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	const estimatedPages = 12;

	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} start page`);
		allRequests.push({
			url: type.baseUrl + (startPage > 1 ? `&page=${startPage}` : ""),
			userData: {
				pageNum: startPage,
				isRental: type.isRental,
				label: type.label,
				baseUrl: type.baseUrl,
				totalPages: 20 // Initial guess, will be updated dynamically
			}
		});
	}

	await crawler.run(allRequests);

	logger.step(`Run finished → Total Saved: ${counts.totalSaved} | Sales: ${counts.savedSales} | Rentals: ${counts.savedRentals}`);
	if (startPage === 1) {
		await updateRemoveStatus(AGENT_ID, new Date());
	}
}

(async () => {
	try {
		await scrapeRedacStrattons();
		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();