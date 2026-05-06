// Jacksons (Rainham) Scraper 
// Agent ID: 257
// Playwright scraper with Cloudflare bypass

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
    updatePriceByPropertyURLOptimized,
    processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { parsePrice, formatPriceDisplay } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 257;
const logger = createAgentLogger(AGENT_ID);

const stats = {
    totalScraped: 0,
    totalSaved: 0,
    savedSales: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================================================
// BROWSERLESS ENDPOINT
// ============================================================================

function getBrowserlessEndpoint() {
    return (
        process.env.BROWSERLESS_WS_ENDPOINT ||
        `ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
    );
}

const SALES_URL = "https://jacksonsproperty.co.uk/sales/property-for-sale?is_available=true&sort=suggested";

// ============================================================================
// REQUEST HANDLER
// ============================================================================

const cheerio = require("cheerio");
const https = require("https");

// ============================================================================
// GEOCODING FALLBACK
// ============================================================================
async function geocodeAddress(address) {
    if (!address) return { latitude: null, longitude: null };
    const query = encodeURIComponent(address.trim() + ", UK");
    const url = `https://nominatim.openstreetmap.org/search?q=${query}&format=json&limit=1`;
    return new Promise((resolve) => {
        https.get(url, { headers: { 'User-Agent': 'JacksonsScraper/1.0' } }, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                try {
                    const results = JSON.parse(data);
                    if (results && results.length > 0) {
                        resolve({ latitude: parseFloat(results[0].lat), longitude: parseFloat(results[0].lon) });
                    } else {
                        resolve({ latitude: null, longitude: null });
                    }
                } catch (e) { resolve({ latitude: null, longitude: null }); }
            });
        }).on('error', () => resolve({ latitude: null, longitude: null }));
    });
}

// ============================================================================
// EXTRACT COORDINATES FROM DETAIL PAGE
// ============================================================================
async function extractCoordinatesFromDetailPage(page, title) {
    try {
        const html = await page.content();

        let latitude = null;
        let longitude = null;

        // Method 1: JSON-LD geo (company coords - skip, use NUXT data instead)
        // Method 2: __NUXT_DATA__ - property address has lat/long as strings
        try {
            const nuxtDataMatch = html.match(/<script[^>]*id="__NUXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
            if (nuxtDataMatch) {
                const nuxtText = nuxtDataMatch[1];
                // UK lat pattern: "51.xxxxxx"
                const latMatch = nuxtText.match(/"(5[0-9]\.[0-9]{4,8})"/);
                // UK lng pattern: "0.xxxxxx" or "-0.xxxxxx" or "1.xxxxxx"
                const lngMatch = nuxtText.match(/"(-?[0-2]\.[0-9]{4,8})"/);
                if (latMatch && lngMatch) {
                    latitude = parseFloat(latMatch[1]);
                    longitude = parseFloat(lngMatch[1]);
                    logger.step(`✅ NUXT coords: ${latitude}, ${longitude}`);
                }
            }
        } catch (_) {}

        // Method 3: "lat":"51.xxx" string pattern
        if (!latitude || !longitude) {
            const latMatch = html.match(/"lat"\s*:\s*"([0-9.-]+)"/);
            const lonMatch = html.match(/"long"\s*:\s*"([0-9.-]+)"/i) ||
                             html.match(/"lon"\s*:\s*"([0-9.-]+)"/i) ||
                             html.match(/"lng"\s*:\s*"([0-9.-]+)"/i);
            if (latMatch) latitude = parseFloat(latMatch[1]);
            if (lonMatch) longitude = parseFloat(lonMatch[1]);
        }

        // Method 4: latitude/longitude numeric patterns
        if (!latitude || !longitude) {
            const latMatch = html.match(/"?latitude"?\s*[:=]\s*([0-9.-]+)/i);
            const lngMatch = html.match(/"?longitude"?\s*[:=]\s*([0-9.-]+)/i);
            if (latMatch) latitude = parseFloat(latMatch[1]);
            if (lngMatch) longitude = parseFloat(lngMatch[1]);
        }

        // Validate UK coords
        if (latitude && (latitude < 49 || latitude > 61)) {
            latitude = null;
            longitude = null;
        }

        // Geocoding fallback
        if (!latitude || !longitude) {
            logger.step(`Geocoding fallback for: ${title}`);
            const coords = await geocodeAddress(title);
            latitude = coords.latitude;
            longitude = coords.longitude;
        }

        return { latitude, longitude };
    } catch (err) {
        logger.error(`Coordinate extraction failed: ${err.message}`);
        return { latitude: null, longitude: null };
    }
}

// ============================================================================
// IMPROVED REQUEST HANDLER - FIXED
// ============================================================================
async function handleListingPage({ page, request, crawler }) {
    const { pageNum, label, totalPages = 1 } = request.userData;
    logger.page(pageNum, label, request.url, totalPages);

    let allProperties = [];

    try {
        // Initial load
        await page.waitForSelector('a[href*="/properties/"]', { timeout: 20000 }).catch(() => {});
        await page.waitForTimeout(2000);

        // === ACCEPT COOKIES ===
        try {
            const acceptBtn = await page.getByText('Accept', { exact: true }).first();
            if (await acceptBtn.isVisible()) {
                logger.step("Accepting cookies...");
                await acceptBtn.click();
                await page.waitForTimeout(2000);
            }
        } catch (e) {
            // Ignore if not found
        }

        let previousCount = 0;
        let attempts = 0;
        const maxAttempts = 25; // Increased to ensure we get all 41 results

        logger.step(`Starting Load More loop (max ${maxAttempts} attempts)...`);

        while (attempts < maxAttempts) {
            attempts++;

            const properties = await page.evaluate(() => {
                const props = [];
                const seen = new Set();

                document.querySelectorAll('a[href*="/properties/"]').forEach(card => {
                    const href = card.getAttribute('href');
                    if (!href || href === '/properties' || seen.has(href)) return;
                    seen.add(href);

                    const h5s = card.querySelectorAll('h5');
                    if (h5s.length < 2) return;

                    const title = h5s[0].innerText.trim();
                    const priceText = h5s[1].innerText.trim();
                    const price = parseFloat(priceText.replace(/[^\d.]/g, ''));

                    if (!title || !price) return;

                    const addressEl = card.querySelector('p, .address, .location, .property-address, .text-gray');
                    const address = addressEl ? addressEl.innerText.trim() : '';

                    const bedroomsMatch = card.innerText.match(/(\d+)\s*(?:bed|bedroom|bedrooms)/i);
                    const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[1]) : null;

                    const link = href.startsWith('http') 
                        ? href 
                        : `https://jacksonsproperty.co.uk${href}`;

                    props.push({ title, link, price, bedrooms, address });
                });

                return props;
            });

            const currentCount = properties.length;
            logger.page(pageNum, label, `Attempt ${attempts}: Found ${currentCount} properties`);

            // Add to collection
            allProperties = [...allProperties, ...properties];

            // Stop if no new properties are loading and we have a reasonable amount
            if (currentCount === previousCount && currentCount >= 40) {
                logger.step(`✅ Stopping - No more properties loading. Total: ${currentCount}`);
                break;
            }

            previousCount = currentCount;

            // === CLICK "LOAD MORE" BUTTON (Improved) ===
            let loadMoreClicked = false;
            try {
                const possibleTexts = ["LOAD MORE", "Load More", "load more", "Show More", "View More", "LOAD MORE PROPERTIES"];
                
                for (const text of possibleTexts) {
                    const btn = page.locator(`button:has-text("${text}"), a:has-text("${text}"), div:has-text("${text}")`).last();
                    
                    if (await btn.isVisible()) {
                        await btn.scrollIntoViewIfNeeded();
                        await btn.click({ force: true });
                        loadMoreClicked = true;
                        break;
                    }
                }
            } catch (err) {
                logger.step(`Load More click failed: ${err.message}`);
            }

            if (loadMoreClicked) {
                logger.step(`Clicked "Load More" button (attempt ${attempts})`);
            } else {
                // Fallback: Scroll to bottom
                await page.evaluate(() => {
                    window.scrollBy(0, window.innerHeight * 1.5);
                });
                await page.waitForTimeout(1000);
            }

            await page.waitForTimeout(3500); // Wait for new content
        }

        // Remove duplicates
        const uniqueProps = [...new Map(allProperties.map(p => [p.link, p])).values()];

        logger.step(`✅ Final unique properties collected: ${uniqueProps.length}`);

        // Process properties
        for (let i = 0; i < uniqueProps.length; i++) {
            const prop = uniqueProps[i];

            const result = await updatePriceByPropertyURLOptimized(
                prop.link, prop.price, prop.title, prop.bedrooms, AGENT_ID, false
            );

            let action = "UNCHANGED";
            let latitude = null;
            let longitude = null;

            try {
                logger.step(`Fetching coords: ${prop.title.substring(0, 45)}...`);
                const detailPage = await page.context().newPage();

                try {
                    await detailPage.goto(prop.link, { 
                        waitUntil: 'domcontentloaded', 
                        timeout: 30000 
                    });
                    await detailPage.waitForTimeout(2500);

                    const coords = await extractCoordinatesFromDetailPage(detailPage, prop.title);
                    latitude = coords.latitude;
                    longitude = coords.longitude;
                } finally {
                    await detailPage.close().catch(() => {});
                }
            } catch (err) {
                logger.error(`Detail page failed: ${err.message}`);
                const coords = await geocodeAddress(prop.address || prop.title);
                latitude = coords.latitude;
                longitude = coords.longitude;
            }

            // Save logic (unchanged)
            if (!result.isExisting) {
                action = "CREATED";
                await processPropertyWithCoordinates(prop.link, prop.price, prop.title, prop.bedrooms, AGENT_ID, false, null, latitude, longitude);
                stats.totalSaved++;
                stats.totalScraped++;
            } else if (result.updated) {
                action = "UPDATED";
                await processPropertyWithCoordinates(prop.link, prop.price, prop.title, prop.bedrooms, AGENT_ID, false, null, latitude, longitude);
                stats.totalSaved++;
                stats.totalScraped++;
            } else if (result.isExisting) {
                await processPropertyWithCoordinates(prop.link, prop.price, prop.title, prop.bedrooms, AGENT_ID, false, null, latitude, longitude);
                stats.totalScraped++;
            }

            logger.property(
                i + 1, label, prop.title.substring(0, 40),
                formatPriceDisplay(prop.price, false),
                prop.link, false, totalPages, action
            );

            await sleep(700);
        }

    } catch (error) {
        logger.error(`Error processing listing page ${pageNum}`, error);
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
        requestHandlerTimeoutSecs: 300,
        preNavigationHooks: [async ({ page }) => await blockNonEssentialResources(page)],
        launchContext: {
            launcher: undefined,
            launchOptions: {
                browserWSEndpoint,
                args: ["--no-sandbox", "--disable-setuid-sandbox"],
            },
        },
        requestHandler: handleListingPage,
        failedRequestHandler({ request }) {
            logger.error(`Failed: ${request.url}`);
        },
    });
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

async function scrapeJacksonsRainham() {
    const scrapeStartTime = new Date();
    logger.step(`Starting Jacksons (Rainham) scraper (Agent ${AGENT_ID}) at ${scrapeStartTime.toISOString()}`);

    const args = process.argv.slice(2);
    const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;

    const browserWSEndpoint = getBrowserlessEndpoint();
    logger.step(`Using browserless: ${browserWSEndpoint.split("?")[0]}`);

    const crawler = createCrawler(browserWSEndpoint);

    // Main listings page (assume single page initially, add pagination later if needed)
    const requests = [{
        url: SALES_URL,
        userData: {
            pageNum: 1,
            label: "JACKSONS_SALES",
            totalPages: 1
        }
    }];

    logger.step(`Queueing ${requests.length} pages starting from page ${startPage}`);
    await crawler.run(requests);

    logger.step(
        `Completed - Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved} sales`
    );

    logger.step("Updating remove status...");
    await updateRemoveStatus(AGENT_ID, scrapeStartTime);

    logger.step("✅ Jacksons scraper finished successfully!");
}

(async () => {
    try {
        await scrapeJacksonsRainham();
        process.exit(0);
    } catch (err) {
        logger.error("Fatal error:", err);
        process.exit(1);
    }
})();

