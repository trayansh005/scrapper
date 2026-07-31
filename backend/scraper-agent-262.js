// REDAC Strattons Property scraper using Playwright with Crawlee
// Agent ID: 262
// Updated 2026-06-02: Fixed for Nuxt.js dynamic loading + better card detection

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 262;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
	{
		baseUrl: "https://redacstrattons.com/property-for-sale/",
		isRental: false,
		label: "SALES",
	},
	{
		baseUrl: "https://redacstrattons.com/property-to-rent/",
		isRental: true,
		label: "RENTALS",
	},
];

const counts = { totalScraped: 0, totalSaved: 0, savedSales: 0, savedRentals: 0 };
const processedUrls = new Set();

// ============================================================================
// UTILITIES
// ============================================================================

function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const type = route.request().resourceType();
		if (["image", "font", "media", "stylesheet"].includes(type)) return route.abort();
		return route.continue();
	});
}

function shouldExcludeProperty(statusText, rawText, title) {
    const status = (statusText || "").toLowerCase();
    const text = (rawText + " " + title || "").toLowerCase();

    if (["sold", "under offer", "let agreed", "reserved", "rented", "sold stc", "sstc"].some(k => status.includes(k))) {
        return true;
    }

    const keywordsRegex = /\b(under offer|under-offer|sold|sold stc|sstc|reserved|let agreed|rented)\b/i;
    return keywordsRegex.test(text);
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
// LISTING HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
    const { pageNum, isRental, label, baseUrl } = request.userData;
    
    logger.page(pageNum, label, request.url);

    try {
        await page.waitForLoadState("networkidle", { timeout: 60000 });

        // Long wait for Nuxt.js to load listings
        await page.waitForTimeout(8000);

        // Scroll to trigger lazy loading
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await page.waitForTimeout(3000);

        const properties = await page.evaluate(() => {
            const results = [];
            const seen = new Set();

            // Target exact listing card containers
            const cards = document.querySelectorAll(".listing__inner, article");

            for (const card of cards) {
                const linkEl = card.querySelector('a[href*="/property/"]');
                if (!linkEl) continue;

                const href = linkEl.getAttribute("href");
                if (!href || seen.has(href)) continue;
                seen.add(href);

                const fullLink = href.startsWith("http") ? href : new URL(href, location.origin).href;

                const rawText = (card.textContent || "").replace(/\s+/g, " ").trim();

                // Title extraction targeting .listing__title or heading tags
                const titleEl = card.querySelector(".listing__title a, .listing__title, h1, h2, h3");
                const title = (titleEl?.textContent || "Property").replace(/\s+/g, " ").trim();

                // Status label extraction (.listing__label__container or badge elements)
                let statusText = "";
                const labelEl = card.querySelector(".listing__label__container, .listing__label, [class*='label'], [class*='status'], [class*='badge']");
                if (labelEl) {
                    statusText = labelEl.textContent.replace(/\s+/g, " ").trim();
                }

                // Price extraction
                const priceMatch = rawText.match(/£[\d,]+(?:\.\d+)?/);
                const priceRaw = priceMatch ? priceMatch[0] : "";

                // Bedroom count extraction
                let bedText = "";
                const bedEl = Array.from(card.querySelectorAll(".icons__item__text, .icons__item")).find(el => el.textContent.toLowerCase().includes("bed"));
                if (bedEl) {
                    bedText = bedEl.textContent.trim();
                } else {
                    const bedMatch = rawText.match(/(\d+)\s*(?:bed|beds|bedroom|bedrooms)/i);
                    if (bedMatch) bedText = bedMatch[0];
                }

                results.push({
                    link: fullLink,
                    title,
                    priceRaw,
                    bedText,
                    statusText,
                    rawText: rawText.substring(0, 600)
                });
            }

            return results;
        });

        logger.page(pageNum, label, `Found ${properties.length} properties`);

        let excluded = 0;

        for (const prop of properties) {
            const normalizedUrl = normalizePropertyUrl(prop.link);
            if (processedUrls.has(normalizedUrl)) continue;
            processedUrls.add(normalizedUrl);

            if (shouldExcludeProperty(prop.statusText, prop.rawText, prop.title)) {
                excluded++;
                logger.property(pageNum, label, prop.title.substring(0, 50), "N/A", prop.link, isRental, 20, "EXCLUDED");
                continue;
            }

            const price = parsePrice(prop.priceRaw);
            if (!price) continue;

            let bedrooms = null;
            if (prop.bedText) {
                const num = prop.bedText.match(/\d+/);
                if (num) bedrooms = parseInt(num[0]);
            }

            const result = await updatePriceByPropertyURLOptimized(
                prop.link, price, prop.title, bedrooms, AGENT_ID, isRental
            );

            let action = result.updated ? "UPDATED" : (!result.isExisting ? "CREATED" : "EXISTING");

            if (!result.isExisting) {
                try {
                    const detailPage = await page.context().newPage();
                    await blockNonEssentialResources(detailPage);
                    await detailPage.goto(prop.link, { waitUntil: "networkidle", timeout: 60000 });
                    const html = await detailPage.content();
                    await processPropertyWithCoordinates(prop.link, price, prop.title, bedrooms, AGENT_ID, isRental, html);
                    await detailPage.close().catch(() => {});

                    counts.totalSaved++;
                    if (isRental) counts.savedRentals++;
                    else counts.savedSales++;
                } catch (e) {
                    logger.error(`Detail failed: ${prop.link}`, e.message);
                }
            }

            logger.property(pageNum, label,
                `${prop.title.substring(0, 55)}${bedrooms ? ` (${bedrooms} bed)` : ""}`,
                formatPriceDisplay(price, isRental),
                prop.link, isRental, 20, action
            );

            await sleep(1000);
        }

        logger.page(pageNum, label, `Excluded ${excluded} properties this page`);

        // Try pagination
        const hasNext = await page.evaluate(() => 
            !!document.querySelector("a[rel='next'], a.next, button.next, .pagination a:last-child")
        );

        if (hasNext && pageNum < 25) {
            const nextUrl = `${baseUrl}?page=${pageNum + 1}`;
            await crawler.addRequests([{
                url: nextUrl,
                userData: { ...request.userData, pageNum: pageNum + 1 }
            }]);
        }

    } catch (error) {
        logger.error(`Page ${pageNum} error`, error.message);
    }
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 3,
		navigationTimeoutSecs: 120,
		requestHandlerTimeoutSecs: 300,
		preNavigationHooks: [({ page }) => blockNonEssentialResources(page)],
		launchContext: {
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1366, height: 1000 }
			}
		},
		requestHandler: handleListingPage,
	});
}

async function scrapeRedacStrattons() {
	logger.step("Starting REDAC Strattons Scraper - Nuxt Optimized v2026-06-02");

	const args = process.argv.slice(2);
	const startPage = args.length ? parseInt(args[0]) || 1 : 1;

	const browserWSEndpoint = process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label}`);
		allRequests.push({
			url: type.baseUrl,
			userData: {
				pageNum: startPage,
				isRental: type.isRental,
				label: type.label,
				baseUrl: type.baseUrl
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