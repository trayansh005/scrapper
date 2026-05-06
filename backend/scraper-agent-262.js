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
// LISTING HANDLER (unchanged - good as is)
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	const { pageNum, isRental, label, baseUrl, totalPages } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	await page.waitForLoadState("networkidle", { timeout: 30000 }).catch(() => null);

	const properties = await page.evaluate(() => {
		const results = [];
		const seen = new Set();

		const links = Array.from(
			document.querySelectorAll("a[href*='/property/']")
		).filter(a => {
			const href = a.getAttribute("href");
			return href && href.includes("/property/") && !href.includes("#");
		});

		for (const link of links) {
			let card = link.closest("article, li, div");

			let depth = 0;
			while (card && depth < 8) {
				const txt = (card.textContent || "").trim();

				if (txt.length > 100 && txt.length < 8000) break;

				card = card.parentElement;
				depth++;
			}

			if (!card) continue;

			const href = link.getAttribute("href");
			if (!href || seen.has(href)) continue;
			seen.add(href);

			const fullLink = href.startsWith("http")
				? href
				: new URL(href, location.origin).href;

			const rawText = (card.textContent || "").replace(/\s+/g, " ");

			const titleEl = card.querySelector(
				"h2, h3, [class*='title'], [class*='address'], .property-title"
			);

			const title = (titleEl?.textContent || "Property").trim();
			let statusText = "";
			const statusEl = card.querySelector(
				"[class*='badge'], [class*='status'], [class*='tag']"
			);

			if (statusEl) statusText = statusEl.textContent.trim();

			if (!statusText) {
				statusText = rawText.match(/under offer|sold|let|rented/i)?.[0] || "";
			}

			const priceMatch = rawText.match(/£[\d,]+(?:[.,]\d+)?/);
			const priceRaw = priceMatch ? priceMatch[0] : "";

			let bedText = "";

			const textPatterns = [
				/(\d+)\s*(?:bed|beds|bedroom|bedrooms)\b/i,
				/bed(?:room)?s?\s*[:=]?\s*(\d+)/i,
				/\b(studio)\b/i
			];

			for (const re of textPatterns) {
				const m = rawText.match(re);
				if (m) {
					bedText = m[0];
					break;
				}
			}

			if (!bedText) {
				const xMatch = rawText.match(/\b[xX]\s*(\d+)\b/i);
				if (xMatch) bedText = "x" + xMatch[1];
			}

			if (!bedText) {
				const earlyNums = rawText.match(/\b([1-5])\b/);
				if (earlyNums) bedText = earlyNums[1];
			}

			results.push({
				link: fullLink,
				title,
				priceRaw,
				bedText,
				statusText
			});
		}

		return results;
	});

	logger.page(pageNum, label, `Found ${properties.length} properties on page ${pageNum}`);

	for (const prop of properties) {
		const normalizedUrl = normalizePropertyUrl(prop.link);

		if (!prop.link || processedUrls.has(normalizedUrl)) continue;
		processedUrls.add(normalizedUrl);

		if (isSkippableProperty(prop.statusText)) {
			logger.property(pageNum, label, prop.title.substring(0, 50), "N/A", prop.link, isRental, totalPages, "SKIPPED");
			continue;
		}

		const price = parsePrice(prop.priceRaw);
		if (!price) continue;

		let bedrooms = null;
		if (prop.bedText) {
			const lower = prop.bedText.toLowerCase().trim();
			if (lower.includes("studio") || lower === "x1" || lower === "1") {
				bedrooms = 0;
			} else {
				const numMatch = prop.bedText.match(/\d+/);
				if (numMatch) bedrooms = parseInt(numMatch[0], 10);
			}
		}

		logger.page(pageNum, label, `DEBUG | Title: "${prop.title}" | bedText: "${prop.bedText}" | bedrooms: ${bedrooms ?? 'null'} | Price: ${price}`);

		const result = await updatePriceByPropertyURLOptimized(
			prop.link, price, prop.title, bedrooms, AGENT_ID, isRental
		);

		let action = "EXISTING";
		if (result.updated) action = "UPDATED";
		else if (!result.isExisting) action = "CREATED";

		if (!result.isExisting && !result.error) {
			try {
				const detailPage = await page.context().newPage();
				await blockNonEssentialResources(detailPage);
				await detailPage.goto(prop.link.trim(), { waitUntil: "networkidle", timeout: 45000 });
				const html = await detailPage.content();
				await processPropertyWithCoordinates(prop.link, price, prop.title, bedrooms, AGENT_ID, isRental, html, null, null);
				await detailPage.close().catch(() => { });
				action = "CREATED";
				counts.totalScraped++;
				counts.totalSaved++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			} catch (e) {
				logger.error(`Detail page failed for ${prop.link}`, e.message);
			}
		}

		logger.property(
			pageNum, label,
			prop.title.substring(0, 50),
			formatPriceDisplay(price, isRental),
			prop.link,
			isRental,
			totalPages,
			action,
			bedrooms !== null ? `${bedrooms} bed` : "NULL"
		);

		await sleep(action === "CREATED" || action === "UPDATED" ? 800 : 300);
	}

	// Pagination
	const hasNext = await page.evaluate(() => !!document.querySelector("a[rel='next'], a.next, .pagination a:last-of-type"));
	if (hasNext && pageNum < totalPages) {
		const nextPageNum = pageNum + 1;
		const nextUrl = baseUrl.includes("?")
			? `${baseUrl}&page=${nextPageNum}`
			: `${baseUrl}?page=${nextPageNum}`;
		await crawler.addRequests([{
			url: nextUrl,
			userData: { ...request.userData, pageNum: nextPageNum }
		}]);
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
		logger.step(`Queueing ${type.label} listings`);
		for (let p = Math.max(1, startPage); p <= estimatedPages; p++) {
			const pageParam = p > 1 ? `&page=${p}` : "";
			allRequests.push({
				url: type.baseUrl + pageParam,
				userData: {
					pageNum: p,
					isRental: type.isRental,
					label: type.label,
					baseUrl: type.baseUrl,
					totalPages: estimatedPages
				}
			});
		}
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