// Abbey Sales and Lettings Group scraper using Playwright with Crawlee
// Agent ID: 227
// Website: abbeysalesandlettingsgroup.co.uk
// Usage:
// node backend/scraper-agent-227.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty, extractBedroomsFromHTML } = require("./lib/property-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 227;
const stats = {
    totalScraped: 0,
    totalSaved: 0,
    savedSales: 0,
    savedRentals: 0,
};

const recentPageSignatures = new Map();
const processedUrls = new Set();

function getBrowserlessEndpoint() {
    return (
        process.env.BROWSERLESS_WS_ENDPOINT ||
        `ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
    );
}

// Configuration
const PROPERTY_TYPES = [
    {
        urlBase: "https://abbeysalesandlettingsgroup.co.uk/search/page/",
        totalPages: 17,
        isRental: false,
        label: "SALES",
        suffix: "/?address_keyword&department=residential-sales&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&officeID&availability=2",
    },
    {
        urlBase: "https://abbeysalesandlettingsgroup.co.uk/search/page/",
        totalPages: 7,
        isRental: true,
        label: "RENTALS",
        suffix: "/?address_keyword=&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=&officeID=&availability=6",
    },
];

function createCrawler(browserWSEndpoint) {
    return new PlaywrightCrawler({
        maxConcurrency: 1,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 300,
        launchContext: {
            launcher: undefined,
            launchOptions: {
                browserWSEndpoint,
                args: ["--no-sandbox", "--disable-setuid-sandbox"],
            },
        },
        requestHandler: handleListingPage,
        failedRequestHandler({ request }) {
            log.error(`Failed listing page: ${request.url}`);
        },
    });
}

async function handleListingPage({ page, request }) {
    const { pageNum, isRental, label } = request.userData;

    console.log(` [${label}] Page ${pageNum} - ${request.url}`);

    // Wait for AJAX content to fully render
    await page.waitForLoadState("networkidle", { timeout: 20000 }).catch(() => { });

    try {
        await page.waitForSelector("a.grid-box-card", { timeout: 20000 }).catch(() => {
            console.log(` Listing container not found on page ${pageNum}`);
        });

                const properties = await page.evaluate(() => {
            try {
                console.log("→ Starting property extraction on listing page");

                // Give a tiny extra moment for any lazy-loaded content (helps on this site)
                // Note: We already waited outside, but this helps in evaluate context

                // === AGGRESSIVE PROPERTY CARD DETECTION ===
                let cardElements = [];

                // Strategy 1: Direct property links (most reliable)
                cardElements = Array.from(document.querySelectorAll('a[href*="/property/"]'));

                // Strategy 2: If no direct links, look inside common card containers
                if (cardElements.length === 0) {
                    const containers = document.querySelectorAll(
                        'div[class*="card"], article[class*="property"], div[class*="listing"], ' +
                        'div[class*="item"], section, .property-card, .grid-item'
                    );
                    cardElements = Array.from(containers).filter(container => 
                        container.querySelector('a[href*="/property/"]')
                    );
                }

                console.log(`→ Found ${cardElements.length} potential property elements`);

                const results = [];

                for (const el of cardElements) {
                    // Get the actual link element
                    const linkEl = el.tagName === 'A' ? el : el.querySelector('a[href*="/property/"]');
                    if (!linkEl) continue;

                    const link = linkEl.href.trim();
                    if (!link || !link.includes("/property/")) continue;

                    // Title
                    let title = "";
                    const titleSelectors = "h1, h2, h3, h4, h5, .title, [class*='title'], strong, .property-name";
                    const titleEl = el.querySelector(titleSelectors);
                    if (titleEl) {
                        title = titleEl.innerText.trim();
                    } else {
                        // Fallback: first line of text that looks like an address
                        title = (el.innerText || "").split('\n')[0].trim();
                    }

                    // Price
                    let rawPrice = "";
                    const priceEl = el.querySelector("[class*='price'], h5, h6, .amount, .cost");
                    if (priceEl) {
                        rawPrice = priceEl.innerText.trim();
                    } else {
                        // Fallback regex for £ symbol
                        const priceMatch = (el.innerText || "").match(/£[0-9,]+(?:\.[0-9]+)?/);
                        if (priceMatch) rawPrice = priceMatch[0];
                    }

                    // === BEDROOMS EXTRACTION (Improved) ===
                    let bedrooms = null;
                    const fullText = (el.innerText || el.textContent || "").toLowerCase();

                    // Primary regex - most common on estate sites
                    let match = fullText.match(/(\d+)\s*(?:bed|beds|bedroom|bedrooms)/i);
                    if (match) {
                        bedrooms = match[1];
                    }

                    // Secondary fallback (for icons or separate spans)
                    if (!bedrooms) {
                        const bedElements = el.querySelectorAll('[class*="bed"], [class*="room"], .icon');
                        for (const bedEl of bedElements) {
                            const txt = (bedEl.innerText || "").trim();
                            const numMatch = txt.match(/(\d+)/);
                            if (numMatch) {
                                bedrooms = numMatch[1];
                                break;
                            }
                        }
                    }

                    // Final fallback - look for number followed by "bed" anywhere
                    if (!bedrooms) {
                        match = (el.innerText || "").match(/(\d+)\s*bed/i);
                        if (match) bedrooms = match[1];
                    }

                    const statusText = el.innerText || "";

                    results.push({
                        link,
                        title,
                        rawPrice,
                        bedrooms,
                        statusText
                    });
                }

                console.log(`→ Successfully extracted ${results.length} properties with details`);
                return results;

            } catch (err) {
                console.error("Evaluate error:", err.message);
                return [];
            }
        });

        console.log(` Found ${properties.length} properties on page ${pageNum}`);

        const pageSignature = properties.map((p) => p.link).slice(0, 5).join("|");
        const signatureKey = isRental ? "RENTALS" : "SALES";
        const previousSignature = recentPageSignatures.get(signatureKey);
        if (pageSignature && previousSignature === pageSignature) {
            console.log(` Warning: ${signatureKey} page ${pageNum} has the same leading links as previous page.`);
        }
        recentPageSignatures.set(signatureKey, pageSignature);

        const batchSize = 2;
        for (let i = 0; i < properties.length; i += batchSize) {
            const batch = properties.slice(i, i + batchSize);

            await Promise.all(
                batch.map(async (property) => {
                    if (!property.link) return;

                    if (isSoldProperty(property.statusText || "")) return;

                    if (processedUrls.has(property.link)) return;
                    processedUrls.add(property.link);

                    const priceNum = property.rawPrice
                        ? parseFloat(property.rawPrice.replace(/[^0-9.]/g, ""))
                        : null;

                    const formattedPrice = priceNum
                        ? parseInt(priceNum, 10).toLocaleString("en-GB")
                        : null;
                    if (priceNum === null) {
                        console.log(` Skipping update (no price found): ${property.link}`);
                        return;
                    }

                    const bedrooms = property.bedrooms || extractBedroomsFromHTML(property.title || "");

                    const result = await updatePriceByPropertyURLOptimized(
                        property.link.trim(),
                        priceNum,
                        property.title,
                        bedrooms,
                        AGENT_ID,
                        isRental
                    );

                    if (result.updated) stats.totalSaved++;

                    let latitude = null;
                    let longitude = null;

                    const detail = await scrapePropertyDetail(page.context(), property);
                    latitude = detail?.coords?.latitude || null;
                    longitude = detail?.coords?.longitude || null;

                    if (!result.isExisting && !result.error) {
                        stats.totalScraped++;
                        if (isRental) stats.savedRentals++;
                        else stats.savedSales++;
                    }

                    // ✅ ALWAYS update DB
                    await updatePriceByPropertyURL(
                        property.link.trim(),
                        formattedPrice,
                        property.title,
                        bedrooms,
                        AGENT_ID,
                        isRental,
                        latitude,
                        longitude,
                    );

                    stats.totalSaved++;

                    const categoryLabel = isRental ? "RENTALS" : "SALES";
                    console.log(
                        `✅ [${categoryLabel}]`,
                        "\n Title:     ", property.title,
                        "\n PriceNum:  ", priceNum,
                        "\n PriceText: ", formattedPrice,
                        "\n Bedrooms:  ", bedrooms,
                        "\n Link:      ", property.link,
                        "\n----------------------------------------------"
                    );
                }),
            );

            await page.waitForTimeout(500);
        }
    } catch (error) {
        console.error(` Error in ${label} page ${pageNum}: ${error.message}`);
    }
}

// Detail page scraping using a light-weight page (blocked resources)
async function scrapePropertyDetail(browserContext, property) {
    await sleep(700);

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

        await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 90000 });
        await detailPage.waitForTimeout(1200);
        const html = await detailPage.content();
        const coords = await extractCoordinatesFromHTML(html);
        console.log("Checking coords for:", property.link);
        console.log("Extracted coords:", coords);

        return {
            coords: { latitude: coords.latitude || null, longitude: coords.longitude || null },
        };
    } catch (err) {
        console.log(` Error scraping detail page ${property.link}: ${err.message}`);
        return null;
    } finally {
        await detailPage.close();
    }
}

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function scrapeAbbeySalesLettings() {
    console.log(`\n Starting Abbey Sales and Lettings Group scraper (Agent ${AGENT_ID})...\n`);

    const browserWSEndpoint = getBrowserlessEndpoint();

    for (const propertyType of PROPERTY_TYPES) {
        console.log(`\n Processing ${propertyType.label} (${propertyType.totalPages} pages)`);
        const crawler = createCrawler(browserWSEndpoint);
        const requests = [];
        for (let pg = 1; pg <= propertyType.totalPages; pg++) {
            requests.push({
                url: `${propertyType.urlBase}${pg}${propertyType.suffix}`,
                userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
            });
        }
        await crawler.addRequests(requests);
        await crawler.run();

    }

    console.log(`\n Scraping complete!`);
    console.log(`Total scraped: ${stats.totalScraped}`);
    console.log(`Total saved: ${stats.totalSaved}`);
}

(async () => {
    try {
        await scrapeAbbeySalesLettings();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n All done!");
        process.exit(0);
    } catch (err) {
        console.error(" Fatal error:", err?.message || err);
        process.exit(1);
    }
})();
