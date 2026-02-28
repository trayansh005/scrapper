// Andrew Craig scraper using Playwright with Crawlee
// Agent ID: 221
// Website: andrewcraig.co.uk
// Usage:
// node backend/scraper-agent-221.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty, extractBedroomsFromHTML } = require("./lib/property-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 221;
const stats = {
    totalScraped: 0,
    totalSaved: 0,
    savedSales: 0,
    savedRentals: 0,
};

const recentPageSignatures = new Map();
const processedUrls = new Set();

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function getBrowserlessEndpoint() {
    return (
        process.env.BROWSERLESS_WS_ENDPOINT ||
        `ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
    );
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
    {
        urlBase: "https://andrewcraig.co.uk/property-for-sale/property/any-bed/all-location",
        totalPages: 12, // 282 properties / 24 per page = 12 pages
        isRental: false,
        label: "SALES",
    },
    {
        urlBase: "https://andrewcraig.co.uk/property-to-rent/property/any-bed/all-location",
        totalPages: 2, // 34 properties / 24 per page = 2 pages
        isRental: true,
        label: "RENTALS",
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

    try {
        await page.waitForTimeout(700);
        await page.waitForSelector(".card[data-id]", { timeout: 20000 }).catch(() => {
            console.log(` Listing container not found on page ${pageNum}`);
        });

        // Extract properties from the DOM
        const properties = await page.evaluate(() => {
            try {
                const cards = Array.from(document.querySelectorAll(".card[data-id]"));
                return cards
                    .map((card) => {
                        const linkEl = card.querySelector("a.card-image-container");
                        const href = linkEl ? linkEl.getAttribute("href") : null;
                        const link = href
                            ? href.startsWith("http")
                                ? href
                                : "https://andrewcraig.co.uk" + href
                            : null;
                        const titleEl = card.querySelector(".card-content > a");
                        const title = titleEl ? titleEl.textContent.trim() : "";
                        const priceEl = card.querySelector("span.price-value");
                        const price = priceEl ? priceEl.textContent.trim() : "";

                        const detailLeft = card.querySelector(".card-content__detail__left");
                        let bedrooms = null;
                        if (detailLeft) {
                            const numbers = Array.from(detailLeft.querySelectorAll(".number"));
                            if (numbers.length >= 1) {
                                bedrooms = numbers[0].textContent.trim();
                            }
                        }

                        const statusText = card.innerText || "";
                        return { link, title, priceRaw: price, bedrooms, statusText };
                    })
                    .filter((p) => p.link);
            } catch (e) {
                console.log("Error extracting properties:", e);
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

                    const priceNum = property.priceRaw
                        ? parseFloat(property.priceRaw.replace(/[^0-9.]/g, ""))
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
                        isRental,
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

// Detail page scraping helper (lightweight, blocked resources)
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
        return { coords: { latitude: coords.latitude || null, longitude: coords.longitude || null } };
    } catch (err) {
        console.log(` Error scraping detail page ${property.link}: ${err.message}`);
        return null;
    } finally {
        await detailPage.close();
    }
}

async function scrapeAndrewCraig() {
    console.log(`\n Starting Andrew Craig scraper (Agent ${AGENT_ID})...\n`);

    const browserWSEndpoint = getBrowserlessEndpoint();
    console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

    for (const propertyType of PROPERTY_TYPES) {
        console.log(`\n Processing ${propertyType.label} (${propertyType.totalPages} pages)`);

        const crawler = createCrawler(browserWSEndpoint);
        const requests = [];
        for (let pg = 1; pg <= propertyType.totalPages; pg++) {
            const url =
                pg === 1
                    ? `${propertyType.urlBase}?exclude=1`
                    : `${propertyType.urlBase}?exclude=1&page=${pg}`;
            requests.push({
                url,
                userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
            });
        }

        await crawler.addRequests(requests);
        await crawler.run();
    }

    console.log(`\n Scraping complete!`);
    console.log(`Total scraped: ${stats.totalScraped}`);
    console.log(`Total saved: ${stats.totalSaved}`);
    console.log(` Breakdown - SALES: ${stats.savedSales}, RENTALS: ${stats.savedRentals}\n`);
}

(async () => {
    try {
        await scrapeAndrewCraig();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n All done!");
        process.exit(0);
    } catch (err) {
        console.error(" Fatal error:", err?.message || err);
        process.exit(1);
    }
})();
