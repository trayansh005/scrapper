// Able Estates scraper using CheerioCrawler
// Agent ID: 101
// Usage: node backend/scraper-agent-101.js [startPage]

const { CheerioCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
    updatePriceByPropertyURLOptimized,
    processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay, formatPriceUk, extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 101;
const logger = createAgentLogger(AGENT_ID);

const counts = {
    totalFound: 0,
    totalScraped: 0,
    totalSaved: 0,
    totalSkipped: 0,
};

const scrapeStartTime = new Date();

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
    const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
    if (!Number.isFinite(value) || value < 1) return 1;
    return Math.floor(value);
}

const PROPERTY_TYPES = [
    {
        urlTemplate: (page) =>
            `https://www.ableestates.com/properties.aspx?Mode=0&PropertyTypeGroup=0&PriceMin=0&PriceMax=0&Bedrooms=0&Statusid=0&searchstatus=1&ShowSearch=1&Page=${page}`,
        label: "SALES",
        isRental: false,
    },
    {
        urlTemplate: (page) =>
            `https://www.ableestates.com/properties.aspx?Mode=1&PropertyTypeGroup=0&PriceMin=0&PriceMax=0&Bedrooms=0&Statusid=0&searchstatus=1&ShowSearch=1&Page=${page}`,
        label: "LETTINGS",
        isRental: true,
    },
];

async function handleListingPage({ $, request, crawler }) {
    const { pageNum, label, isRental } = request.userData;

    logger.page(pageNum, label, `Processing ${request.url}`, null);

    const propertiesFound = $(".item");

    if (propertiesFound.length === 0) {
        logger.page(pageNum, label, `No properties found. End of pagination.`, null);
        return;
    }

    // Just directly paginate up if there are items
    if (propertiesFound.length > 0) {
        const typeConfig = PROPERTY_TYPES[isRental ? 1 : 0]; // Access from definitions directly
        const nextUrl = typeConfig.urlTemplate(pageNum + 1);
        await crawler.addRequests([
            {
                url: nextUrl,
                userData: {
                    ...request.userData,
                    pageNum: pageNum + 1,
                },
            },
        ]);
    }

    logger.page(pageNum, label, `Found ${propertiesFound.length} properties on page ${pageNum}`, null);

    const propertyList = [];
    propertiesFound.each((index, element) => {
        try {
            const $element = $(element);

            const href = $element.find("a").first().attr("href");
            if (!href) return;
            // E.g: "property-for-sale-longworth-close-thamesmead-london-pi-able10008523.htm"
            if (!href.startsWith("property-")) return;

            const link = href.startsWith("http") ? href : `https://www.ableestates.com/${href.startsWith("/") ? href.slice(1) : href}`;

            let title = $element.find("h3, h4, .address, .title").text().trim();
            if (!title) {
                // Secondary fallback
                title = $element.find("img").attr("alt") || "Property";
            }

            let bedrooms = null;
            $element.find("li").each((_, li) => {
                const text = $(li).text().trim();
                const iconClass = $(li).find("i, span").attr("class") || "";
                if (text.toLowerCase().includes("bed") || iconClass.toLowerCase().includes("bed")) {
                    const match = text.match(/(\d+)/);
                    if (match) bedrooms = parseInt(match[1], 10);
                }
            });
            if (!bedrooms) {
                const bedsMatch = $element.text().match(/(\d+)\s*Bed/i);
                if (bedsMatch) bedrooms = parseInt(bedsMatch[1], 10);
            }

            const priceMatch = $element.text().match(/£[\d,]+/);
            let priceText = priceMatch ? priceMatch[0] : "";
            const price = parsePrice(priceText);

            propertyList.push({ link, title, bedrooms, price });
        } catch (error) {
            logger.error(`Error extracting property details on page ${pageNum}`, error);
        }
    });

    for (const property of propertyList) {
        if (!property.link || !property.price) {
            counts.totalSkipped++;
            continue;
        }

        counts.totalFound++;

        const result = await updatePriceByPropertyURLOptimized(
            property.link,
            property.price,
            property.title,
            property.bedrooms,
            AGENT_ID,
            isRental
        );

        let action = "UNCHANGED";

        if (result.updated) {
            action = "UPDATED";
            counts.totalSaved++;
            counts.totalScraped++;
        }

        if (!result.isExisting && !result.error) {

            // Fetch property coordinates from the details page
            let latitude = null;
            let longitude = null;
            try {
                const response = await fetch(property.link, {
                    headers: { 'User-Agent': 'Mozilla/5.0' }
                });
                if (response.ok) {
                    const html = await response.text();
                    const coords = await extractCoordinatesFromHTML(html);
                    latitude = coords.latitude;
                    longitude = coords.longitude;
                }
            } catch (err) {
                logger.error(`Error fetching coordinates for ${property.link}`, err);
            }

            await processPropertyWithCoordinates(
                property.link,
                property.price,
                property.title,
                property.bedrooms,
                AGENT_ID,
                isRental,
                null,
                latitude,
                longitude
            );

            action = "CREATED";
            counts.totalSaved++;
            counts.totalScraped++;
        } else if (result.error) {
            action = "ERROR";
            counts.totalSkipped++;
        }

        logger.property(
            pageNum,
            label,
            property.title.substring(0, 60),
            formatPriceDisplay(property.price, isRental),
            property.link,
            isRental,
            null,
            action
        );

        if (action !== "UNCHANGED") {
            await sleep(500);
        }
    }
}

function createCrawler() {
    return new CheerioCrawler({
        maxConcurrency: 1,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 60,
        requestHandler: handleListingPage,
        failedRequestHandler({ request, error }) {
            const { pageNum, label } = request.userData || {};
            logger.error(`Failed API page: ${request.url}`, error || null, pageNum, label);
        },
    });
}

async function scrapeAbleEstates() {
    logger.step(`Starting Able Estates scraper (Agent ${AGENT_ID})...`);

    const startPage = getStartPage();
    const isPartialRun = startPage > 1;

    if (isPartialRun) {
        logger.step(
            `Partial run detected (startPage=${startPage}). Remove status update will be skipped.`
        );
    }

    const initialRequests = [];
    for (const typeConfig of PROPERTY_TYPES) {
        initialRequests.push({
            url: typeConfig.urlTemplate(startPage),
            userData: {
                pageNum: startPage,
                startPage,
                isRental: typeConfig.isRental,
                label: typeConfig.label,
                typeConfig,
            },
        });
    }

    if (initialRequests.length === 0) {
        logger.step("No pages to scrape with current arguments.");
        return;
    }

    const crawler = createCrawler();
    await crawler.run(initialRequests);

    if (!isPartialRun) {
        await updateRemoveStatus(AGENT_ID, scrapeStartTime);
    } else {
        logger.step("Skipping remove status update (Partial run)");
    }

    logger.step(
        `Completed Able Estates - Found: ${counts.totalFound}, Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Skipped: ${counts.totalSkipped}`
    );
}

scrapeAbleEstates()
    .then(() => {
        logger.step("All done!");
        process.exit(0);
    })
    .catch((error) => {
        logger.error("Fatal error", error);
        process.exit(1);
    });
