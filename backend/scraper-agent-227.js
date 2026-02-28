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
        totalPages: 14,
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

    try {
        await page.waitForTimeout(700);
        await page.waitForSelector(".properties-block .grid-box", { timeout: 15000 }).catch(() => {
            console.log(` Listing container not found on page ${pageNum}`);
        });

        const properties = await page.evaluate(() => {
            try {
                const items = Array.from(document.querySelectorAll(".properties-block .grid-box"));
                const results = [];
                for (const el of items) {
                    const linkEl = el.querySelector("a[href*='/property/']");
                    const link = linkEl ? linkEl.href : null;
                    if (!link) continue;
                    const title = el.querySelector("h4")?.innerText.trim() || "";
                    const rawPrice = el.querySelector("h5.property-archive-price")?.innerText.trim() || "";
                    const typeItems = Array.from(el.querySelectorAll("ul.property-types li"));
                    let bedrooms = null;
                    typeItems.forEach((li) => {
                        const span = li.querySelector("span");
                        const icon = li.querySelector("i");
                        if (icon && icon.classList.contains("fa-bed")) {
                            bedrooms = span ? span.innerText.trim() : null;
                        }
                    });
                    const statusText = el.innerText || "";
                    results.push({ link, title, rawPrice, bedrooms, statusText });
                }
                return results;
            } catch (err) {
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
        for (let pg = 38; pg <= propertyType.totalPages; pg++) {
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
