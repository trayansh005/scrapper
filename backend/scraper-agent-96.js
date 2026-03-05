// Cow & Co scraper using CheerioCrawler
// Agent ID: 96
// Usage: node backend/scraper-agent-96.js [startPage]

const { CheerioCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
    updatePriceByPropertyURLOptimized,
    processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { parsePrice, formatPriceDisplay, formatPriceUk, extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 96;
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
            `https://cowandco-london.com/property-search/page/${page}/?orderby&instruction_type=sale&address_keyword&min_bedrooms&minprice&maxprice&property_type&showstc=off`,
        label: "SALES",
        isRental: false,
    },
    {
        urlTemplate: (page) =>
            `https://cowandco-london.com/property-search/page/${page}/?orderby&instruction_type=letting&address_keyword&min_bedrooms&minprice&maxprice&property_type&showstc=off`,
        label: "LETTINGS",
        isRental: true,
    },
];

async function handleListingPage({ $, request, crawler }) {
    const { pageNum, label, isRental, startPage } = request.userData;

    logger.page(pageNum, label, `Processing ${request.url}`, null);

    const propertiesFound = $(".property-grid");

    if (propertiesFound.length === 0) {
        logger.page(pageNum, label, `No properties found. End of pagination.`, null);
        return;
    }

    // Just directly check for the next page URL
    if (propertiesFound.length > 0) {
        const typeConfig = PROPERTY_TYPES[isRental ? 1 : 0];
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

            const href = $element.find("a").attr("href");
            if (!href) return;

            const link = href.startsWith("http") ? href : `https://cowandco-london.com${href.startsWith("/") ? "" : "/"}${href}`;

            const title = $element.find(".property-grid__meta h4").text().trim() || "Property";

            let bedrooms = null;
            const bedroomsText = $element.find(".property-grid__meta h5").text().trim();
            if (bedroomsText) {
                const bedroomsMatch = bedroomsText.match(/\d+/);
                bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[0], 10) : null;
            }

            const priceText = $element.find("h6 span").text().trim() || $element.find("h6").text().trim();
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

async function scrapeCowAndCo() {
    logger.step(`Starting Cow & Co scraper (Agent ${AGENT_ID})...`);

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
        `Completed Cow & Co - Found: ${counts.totalFound}, Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Skipped: ${counts.totalSkipped}`
    );
}

scrapeCowAndCo()
    .then(() => {
        logger.step("All done!");
        process.exit(0);
    })
    .catch((error) => {
        logger.error("Fatal error", error);
        process.exit(1);
    });
