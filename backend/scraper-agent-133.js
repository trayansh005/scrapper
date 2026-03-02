// Countrywide Scotland scraper using Playwright with Crawlee
// Agent ID: 133
// Usage:
// node backend/scraper-agent-133.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { updatePriceByPropertyURLOptimized, processPropertyWithCoordinates, } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty, parsePrice, formatPriceDisplay, } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 133;
const logger = createAgentLogger(AGENT_ID);

const counts = {
    totalScraped: 0,
    totalSaved: 0,
    savedSales: 0,
    savedRentals: 0,
};

// Based on the URL structure, pagination appears to be handled dynamically
// Initial estimate of page counts; crawler will handle pagination
const PROPERTY_TYPES = [
    {
        channel: "sales",
        baseUrl:
            "https://www.countrywidescotland.co.uk/properties/sales/status-available/most-recent-first",
        isRental: false,
        label: "SALES",
        totalRecords: 138,
        recordsPerPage: 10,
    },
    {
        channel: "lettings",
        baseUrl:
            "https://www.countrywidescotland.co.uk/properties/lettings/status-available/most-recent-first",
        isRental: true,
        label: "LETTINGS",
        totalRecords: 41,
        recordsPerPage: 10,
    },
];

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// using shared blockNonEssentialResources from lib/scraper-utils.js

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

async function scrapePropertyDetail(browserContext, property) {
    await sleep(700);
    const detailPage = await browserContext.newPage();
    try {
        // await blockNonEssentialResources(detailPage);
        await detailPage.goto(property.link, {
            waitUntil: "networkidle",
            timeout: 90000,
        });
        await detailPage.waitForTimeout(800);
        const htmlContent = await detailPage.content();
        const coords = await extractCoordinatesFromHTML(htmlContent);
        return {
            coords: {
                latitude: coords.latitude || null,
                longitude: coords.longitude || null,
            },
        };
    } catch (error) {
        logger.error(`Error scraping detail page ${property.link}`, error);
        return null;
    } finally {
        await detailPage.close();
    }
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
    const { pageNum, isRental, label, totalPages } = request.userData;
    logger.page(pageNum, label, request.url, totalPages);

    try {
        // Wait for property cards to load
        await page.waitForLoadState("networkidle");
        await page.waitForTimeout(3000);
        const html = await page.content();
        console.log(html.substring(0, 5000));
    } catch (e) {
        logger.warn("No properties found on page", pageNum, label);
    }

    const properties = await page.evaluate(() => {
        try {
            const results = [];
            const seen = new Set();

            const cards = document.querySelectorAll("article");

            for (const card of cards) {
                const linkElem = card.querySelector("a.card__link");
                if (!linkElem) continue;

                const href = linkElem.getAttribute("href");
                if (!href) continue;

                const fullUrl = href.startsWith("http")
                    ? href
                    : new URL(href, window.location.origin).href;

                if (seen.has(fullUrl)) continue;
                seen.add(fullUrl);

                const priceElem = card.querySelector("[class*='price']");
                const priceRaw = priceElem ? priceElem.textContent.trim() : "";
                if (!priceRaw) continue;

                const titleElem = card.querySelector("h2, h3");
                const title = titleElem ? titleElem.textContent.trim() : "";

                let bedrooms = null;
                const bedMatch = card.innerText.match(/(\d+)\s*bed/i);
                if (bedMatch) bedrooms = parseInt(bedMatch[1]);

                const statusElem = card.querySelector("[class*='status']");
                const statusText = statusElem ? statusElem.textContent.trim() : "";

                results.push({
                    link: fullUrl,
                    title,
                    priceRaw,
                    bedrooms,
                    statusText,
                });
            }

            return results;
        } catch (err) {
            console.log("Extraction error:", err.message);
            return [];
        }
    });

    logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

    for (const property of properties) {
        if (!property.link) continue;

        if (isSoldProperty(property.statusText || "")) continue;

        if (processedUrls.has(property.link)) {
            logger.page(
                pageNum,
                label,
                `Skipping duplicate URL: ${property.link.substring(0, 60)}...`,
                totalPages,
            );
            continue;
        }
        processedUrls.add(property.link);

        // Extract price and bedrooms
        const price = parsePrice(property.priceRaw);
        const bedrooms = property.bedrooms;

        if (!price) {
            logger.page(pageNum, label, `Skipping update (no price found): ${property.link}`, totalPages);
            continue;
        }

        const result = await updatePriceByPropertyURLOptimized(
            property.link,
            price,
            property.title,
            bedrooms,
            AGENT_ID,
            isRental,
        );

        let propertyAction = "UNCHANGED";

        if (result.updated) {
            counts.totalSaved++;
            propertyAction = "UPDATED";
        }

        if (!result.isExisting && !result.error) {
            const detail = await scrapePropertyDetail(page.context(), property);
            await processPropertyWithCoordinates(
                property.link.trim(),
                price,
                property.title,
                bedrooms,
                AGENT_ID,
                isRental,
                null, // HTML not needed if we have coords
                detail?.coords?.latitude || null,
                detail?.coords?.longitude || null,
            );
            counts.totalSaved++;
            counts.totalScraped++;
            if (isRental) counts.savedRentals++;
            else counts.savedSales++;
            propertyAction = "CREATED";
        } else if (result.isExisting && result.updated) {
            counts.totalScraped++;
            if (isRental) counts.savedRentals++;
            else counts.savedSales++;
        } else if (result.error) {
            propertyAction = "ERROR";
        }

        logger.property(
            pageNum,
            label,
            property.title.substring(0, 40),
            formatPriceDisplay(price, isRental),
            property.link,
            isRental,
            totalPages,
            propertyAction,
        );

        if (propertyAction !== "UNCHANGED") {
            await sleep(500);
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
        navigationTimeoutSecs: 90,
        requestHandlerTimeoutSecs: 300,
        preNavigationHooks: [
            async ({ page }) => {

                // 1️⃣ Set real user agent
                await page.setExtraHTTPHeaders({
                    "accept-language": "en-GB,en;q=0.9",
                });

                await page.setUserAgent(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                );

                // 2️⃣ Remove webdriver flag
                await page.addInitScript(() => {
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined,
                    });
                });
            },
        ],
        launchContext: {
            launcher: undefined,
            launchOptions: {
                browserWSEndpoint,
                args: ["--no-sandbox", "--disable-setuid-sandbox"],
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

async function scrapeCountrywideScotland() {
    logger.step("Starting Countrywide Scotland scraper...");

    const args = process.argv.slice(2);
    const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
    const isPartialRun = startPage > 1;
    const scrapeStartTime = new Date();

    const browserWSEndpoint = getBrowserlessEndpoint();
    logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

    const crawler = createCrawler(browserWSEndpoint);

    const allRequests = [];
    for (const type of PROPERTY_TYPES) {
        const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
        logger.step(`Queueing ${type.label} (${totalPages} pages)`);

        for (let pg = Math.max(1, startPage); pg <= totalPages; pg++) {			// For Countrywide, pagination may be handled via query params
            // Adjust URL structure based on actual site behavior
            const url =
                pg === 1
                    ? type.baseUrl
                    : `${type.baseUrl}?page=${pg}`;

            allRequests.push({
                url,
                userData: {
                    pageNum: pg,
                    isRental: type.isRental,
                    label: type.label,
                    totalPages,
                },
            });
        }
    }

    if (allRequests.length > 0) {
        await crawler.run(allRequests);
    } else {
        logger.warn("No requests to process.");
    }

    logger.step(
        `Completed Countrywide Scotland - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New lettings: ${counts.savedRentals}`,
    );

    if (!isPartialRun) {
        logger.step("Updating remove status...");
        await updateRemoveStatus(AGENT_ID, scrapeStartTime);
    } else {
        logger.warn("Partial run detected. Skipping updateRemoveStatus.");
    }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

scrapeCountrywideScotland()
    .then(() => {
        logger.step("All done!");
        process.exit(0);
    })
    .catch((error) => {
        logger.error("Unhandled scraper error", error);
        process.exit(1);
    });
