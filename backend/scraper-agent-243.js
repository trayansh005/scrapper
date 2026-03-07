// Dixons Estate Agents scraper using Playwright with Crawlee
// Agent ID: 243

const { PlaywrightCrawler, log } = require("crawlee");

const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");
const {
    formatPriceUk,
    updatePriceByPropertyURLOptimized,
} = require("./lib/db-helpers.js");

const { isSoldProperty } = require("./lib/property-helpers.js");
const { blockNonEssentialResources, sleep } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 243;
const logger = createAgentLogger(AGENT_ID);

const stats = {
    totalScraped: 0,
    totalSaved: 0,
    savedSales: 0,
    savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// BROWSERLESS
// ============================================================================

function getBrowserlessEndpoint() {
    return (
        process.env.BROWSERLESS_WS_ENDPOINT ||
        "ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
    );
}

// ============================================================================
// DETAIL SCRAPER
// ============================================================================

async function scrapePropertyDetail(browserContext, property) {

    await sleep(500);

    const detailPage = await browserContext.newPage();

    try {

        await detailPage.route("**/*", (route) => {
            const type = route.request().resourceType();

            if (["image", "font", "stylesheet", "media"].includes(type)) {
                route.abort();
            } else {
                route.continue();
            }
        });

        await detailPage.goto(property.link, {
            waitUntil: "domcontentloaded",
            timeout: 60000,
        });

        const coords = await detailPage.evaluate(() => {

            const html = document.documentElement.innerHTML;

            const lat = html.match(/"latitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
            const lon = html.match(/"longitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);

            return {
                lat: lat ? parseFloat(lat[1]) : null,
                lon: lon ? parseFloat(lon[1]) : null,
            };
        });

        return {
            latitude: coords.lat,
            longitude: coords.lon,
        };

    } catch (err) {

        logger.error(`Detail scrape failed: ${property.link}`, err.message);
        return null;

    } finally {

        await detailPage.close();
    }
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {

    const { pageNum, isRental, label } = request.userData;

    logger.page(pageNum, label, "Processing listing page...");

    await page.waitForTimeout(1500);

    await page
        .waitForSelector(".card", { timeout: 20000 })
        .catch(() => logger.warn("No properties found", pageNum, label));

    const properties = await page.evaluate(() => {

        const cards = document.querySelectorAll(".card");
        const results = [];
        const baseUrl = window.location.origin;

        cards.forEach((card) => {

            const priceText = card.querySelector(".card__heading")?.innerText || "";
            const title = card.querySelector(".card__text-content")?.innerText || "Property";

            const rel = card.querySelector("a.card__link")?.getAttribute("href");
            if (!rel) return;

            const link = rel.startsWith("http") ? rel : baseUrl + rel;

            const statusText = card.innerText || "";

            let bedrooms = null;

            const specs = card.querySelectorAll(".card-content__spec-list-item");

            specs.forEach((spec) => {

                if (spec.querySelector(".icon-bedroom")) {

                    const val = spec.querySelector(".card-content__spec-list-number")?.innerText;

                    if (val) bedrooms = parseInt(val, 10);
                }
            });

            results.push({
                link,
                title,
                statusText,
                priceText,
                bedrooms,
            });
        });

        return results;
    });

    logger.step(`Found ${properties.length} properties`, pageNum, label);

    const batchSize = 5;

    for (let i = 0; i < properties.length; i += batchSize) {

        const batch = properties.slice(i, i + batchSize);

        await Promise.all(

            batch.map(async (property) => {

                if (!property.link) return;

                if (isSoldProperty(property.statusText || "")) return;

                if (processedUrls.has(property.link)) {
                    logger.warn("Skipping duplicate", pageNum, label);
                    return;
                }

                processedUrls.add(property.link);

                try {

                    let actionTaken = "UNCHANGED";

                    const price = formatPriceUk(property.priceText);

                    if (!price) {
                        logger.warn("No price found", pageNum, label);
                        return;
                    }

                    const result = await updatePriceByPropertyURLOptimized(
                        property.link,
                        price,
                        property.title,
                        property.bedrooms,
                        AGENT_ID,
                        isRental
                    );

                    if (result.updated) {
                        stats.totalSaved++;
                        actionTaken = "UPDATED";
                    }

                    if (!result.isExisting && !result.error) {

                        const detail = await scrapePropertyDetail(
                            page.context(),
                            property
                        );

                        await updatePriceByPropertyURL(
                            property.link.trim(),
                            price,
                            property.title,
                            property.bedrooms,
                            AGENT_ID,
                            isRental,
                            detail?.latitude || null,
                            detail?.longitude || null
                        );

                        stats.totalSaved++;
                        stats.totalScraped++;

                        if (isRental) stats.savedRentals++;
                        else stats.savedSales++;

                        actionTaken = "CREATED";
                    }

                    logger.property(
                        pageNum,
                        label,
                        property.title,
                        price,
                        property.link,
                        isRental,
                        null,
                        actionTaken
                    );

                    // STEP 7 — Conditional Sleep
                    if (actionTaken === "CREATED") {
                        await sleep(500);
                    }

                } catch (err) {

                    logger.error("DB error", err, pageNum, label);
                }
            })
        );

        await sleep(200);
    }

    // Pagination

    if (properties.length > 0) {

        const nextPage = pageNum + 1;

        const type = isRental ? "lettings" : "sales";

        const nextUrl =
            `https://www.dixonsestateagents.co.uk/properties/${type}/status-available/most-recent-first/page-${nextPage}#/`;

        await crawler.addRequests([
            {
                url: nextUrl,
                userData: {
                    pageNum: nextPage,
                    isRental,
                    label,
                },
            },
        ]);
    }
}

// ============================================================================
// CRAWLER
// ============================================================================

function createCrawler(browserWSEndpoint) {

    return new PlaywrightCrawler({

        maxConcurrency: 2,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 300,

        preNavigationHooks: [
            async ({ page }) => {
                await blockNonEssentialResources(page);
            },
        ],

        launchContext: {
            launchOptions: {
                browserWSEndpoint,
            },
        },

        requestHandler: handleListingPage,

        failedRequestHandler({ request }) {
            logger.error(`Failed request: ${request.url}`);
        },
    });
}

// ============================================================================
// MAIN
// ============================================================================

async function scrapeDixons() {

    const scrapeStartTime = new Date();

    logger.step("Starting Dixons scraper");

    const browserWSEndpoint = getBrowserlessEndpoint();

    const crawler = createCrawler(browserWSEndpoint);

    await crawler.addRequests([
        {
            url: `https://www.dixonsestateagents.co.uk/properties/lettings/status-available/most-recent-first/page-1#/`,
            userData: {
                pageNum: 1,
                isRental: true,
                label: "LETTINGS",
            },
        },
    ]);

    await crawler.run();

    logger.step(
        `Completed - Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`
    );

    logger.step(
        `Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`
    );

    await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

// ============================================================================
// EXECUTION
// ============================================================================

(async () => {

    try {

        await scrapeDixons();

        logger.step("All done!");

        process.exit(0);

    } catch (err) {

        logger.error("Fatal error:", err);

        process.exit(1);
    }
})();