// REDAC Strattons Property scraper using Playwright with Crawlee
// Agent ID: 262
// Updated 2026-06-02: Stronger Under Offer / Sold / Let exclusion logic

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
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) { 
    return new Promise(r => setTimeout(r, ms)); 
}

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		if (["image", "font", "media"].includes(route.request().resourceType())) 
            return route.abort();
		return route.continue();
	});
}

// === STRONG EXCLUSION LOGIC ===
function shouldExcludeProperty(statusText, rawText, title) {
    const combinedText = (statusText + " " + rawText + " " + title || "").toLowerCase();
    
    const excludeKeywords = [
        "under offer", "under-offer", "sold", "sold stc", 
        "reserved", "let agreed", "let", "rented", "agreed",
        "under offer", "sstc"
    ];

    return excludeKeywords.some(keyword => combinedText.includes(keyword));
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
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
    const { pageNum, isRental, label, baseUrl, totalPages = 20 } = request.userData;
    
    logger.page(pageNum, label, request.url, totalPages);

    try {
        await page.waitForLoadState("domcontentloaded", { timeout: 30000 });

        // Wait for property cards
        await Promise.race([
            page.waitForSelector("a[href*='/property/']", { timeout: 25000 }),
            page.waitForTimeout(12000)
        ]).catch(() => {});

        // Extra time for Nuxt hydration
        await page.waitForTimeout(4500);

        const properties = await page.evaluate(() => {
            const results = [];
            const seen = new Set();

            const cardElements = document.querySelectorAll(`
                article, 
                div[class*="property"], 
                div[class*="listing"], 
                li[class*="property"], 
                .card, 
                .property-card,
                [data-property]
            `);

            for (const card of cardElements) {
                const linkEl = card.querySelector('a[href*="/property/"]');
                if (!linkEl) continue;

                const href = linkEl.getAttribute("href");
                if (!href || seen.has(href)) continue;
                seen.add(href);

                const fullLink = href.startsWith("http") ? href : new URL(href, location.origin).href;

                const rawText = (card.textContent || "").replace(/\s+/g, " ").trim();
                const titleEl = card.querySelector("h2, h3, .title, .address, .property-title, strong");
                const title = (titleEl?.textContent || "Property").trim();

                // Status detection - multiple attempts
                let statusText = "";
                const statusSelectors = [
                    "[class*='status']", "[class*='badge']", "[class*='tag']", 
                    ".sold", ".let", ".offer", ".under", ".stc", "[class*='flag']"
                ];
                
                for (const sel of statusSelectors) {
                    const el = card.querySelector(sel);
                    if (el) {
                        statusText = (el.textContent || "").trim();
                        break;
                    }
                }

                // Price
                const priceMatch = rawText.match(/£[\d,]+(?:\.\d+)?/);
                const priceRaw = priceMatch ? priceMatch[0] : "";

                // Bedrooms
                let bedText = "";
                const bedRegex = /(\d+)\s*(?:bed|beds|bedroom|bedrooms)/i;
                const bedMatch = rawText.match(bedRegex);
                if (bedMatch) bedText = bedMatch[0];

                results.push({
                    link: fullLink,
                    title,
                    priceRaw,
                    bedText,
                    statusText,
                    rawText: rawText.substring(0, 500)
                });
            }

            return results;
        });

        logger.page(pageNum, label, `Found ${properties.length} properties on page ${pageNum}`);

        // Process each property
        for (const prop of properties) {
            const normalizedUrl = normalizePropertyUrl(prop.link);
            if (!prop.link || processedUrls.has(normalizedUrl)) continue;
            processedUrls.add(normalizedUrl);

            // === EXCLUSION CHECK ===
            if (shouldExcludeProperty(prop.statusText, prop.rawText, prop.title)) {
                logger.property(pageNum, label, prop.title.substring(0, 60), "N/A", prop.link, isRental, totalPages, "EXCLUDED");
                continue;
            }

            const price = parsePrice(prop.priceRaw);
            if (!price) continue;

            let bedrooms = null;
            if (prop.bedText) {
                const numMatch = prop.bedText.match(/\d+/);
                if (numMatch) bedrooms = parseInt(numMatch[0]);
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
                    logger.error(`Detail page failed: ${prop.link}`, e.message);
                }
            }

            logger.property(
                pageNum, 
                label, 
                `${prop.title.substring(0, 55)}${bedrooms ? ` (${bedrooms} bed)` : ""}`,
                formatPriceDisplay(price, isRental), 
                prop.link, 
                isRental, 
                totalPages, 
                action
            );

            await sleep(action.includes("CREATE") ? 2500 : 800);
        }

        // Pagination
        const hasNext = await page.evaluate(() => 
            !!document.querySelector("a[rel='next'], a.next, .pagination a:last-child, button.next")
        );

        if (hasNext && pageNum < 30) {
            const nextUrl = baseUrl.includes("?") 
                ? `${baseUrl}&page=${pageNum + 1}` 
                : `${baseUrl}?page=${pageNum + 1}`;
            
            await crawler.addRequests([{
                url: nextUrl,
                userData: { ...request.userData, pageNum: pageNum + 1 }
            }]);
        }

    } catch (error) {
        logger.error(`Page ${pageNum} error (${label})`, error.message);
    }
}

// ============================================================================
// CRAWLER SETUP
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

// ============================================================================
// MAIN FUNCTION
// ============================================================================

async function scrapeRedacStrattons() {
	logger.step("Starting REDAC Strattons Scraper (Sales + Rentals) - v2026-06-02");

	const args = process.argv.slice(2);
	const startPage = args.length ? parseInt(args[0]) || 1 : 1;
	
	const browserWSEndpoint = process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`;

	const crawler = createCrawler(browserWSEndpoint);
	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		logger.step(`Queueing ${type.label} start page`);
		allRequests.push({
			url: type.baseUrl + (startPage > 1 ? `&page=${startPage}` : ""),
			userData: {
				pageNum: startPage,
				isRental: type.isRental,
				label: type.label,
				baseUrl: type.baseUrl,
				totalPages: 20
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