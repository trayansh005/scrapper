// William Charles Group XML Scraper
// Agent ID: 255

const axios = require("axios");
const cheerio = require("cheerio");

const { updateRemoveStatus } = require("./db.js");
const {
    updatePriceByPropertyURLOptimized,
    processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");

const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 255;
const logger = createAgentLogger(AGENT_ID);

const XML_FEED_URL =
    "https://www.williamcharlesgroup.com/export/xml2u/b9el9qJ1aN3H1xAZ";

const stats = {
    totalScraped: 0,
    totalSaved: 0,
    savedSales: 0,
    savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// HELPERS
// ============================================================================

function formatPriceDisplay(price, isRental) {
    return isRental ? `£${price} pcm` : `£${price}`;
}

function getText(prop, selector) {
    return prop.find(selector).first().text().trim();
}

// ============================================================================
// PROCESS FUNCTION
// ============================================================================

async function processProperty(data, index, total) {
    const { link, title, price, bedrooms, isRental, latitude, longitude } = data;

    if (!price || !link) return;

    const result = await updatePriceByPropertyURLOptimized(
        link,
        price,
        title,
        bedrooms,
        AGENT_ID,
        isRental
    );

    let action = "UNCHANGED";

    // Count all valid
    if (!result.error) {
        stats.totalScraped++;
        if (isRental) stats.savedRentals++;
        else stats.savedSales++;
    }

    if (result.updated) {
        stats.totalSaved++;
        action = "UPDATED";
    }

    if (!result.isExisting && !result.error) {
        await processPropertyWithCoordinates(
            link,
            price,
            title,
            bedrooms,
            AGENT_ID,
            isRental,
            null,
            latitude,
            longitude
        );

        stats.totalSaved++;
        action = "CREATED";
    }

    logger.property(
        index,
        "XML_FEED",
        title.substring(0, 40),
        formatPriceDisplay(price, isRental),
        link,
        isRental,
        total,
        action
    );
}

// ============================================================================
// MAIN SCRAPER
// ============================================================================

async function scrapeWilliamCharles() {
    const scrapeStartTime = new Date();

    logger.step(`Starting scraper at ${scrapeStartTime.toISOString()}`);

    try {
        const response = await axios.get(XML_FEED_URL);
        const $ = cheerio.load(response.data, { xmlMode: true });

        const properties = $("Property");

        logger.step(`Found ${properties.length} properties`);

        const sales = [];
        const rentals = [];

        // ====================================================================
        // CLASSIFY (IMPORTANT)
        // ====================================================================

        for (let i = 0; i < properties.length; i++) {
            const prop = $(properties[i]);

            const reference =
                getText(prop, "reference") ||
                getText(prop, "PropertyID") ||
                `prop-${i}`;

            const link = `https://www.williamcharlesgroup.com/properties/${reference}`;

            if (processedUrls.has(link)) continue;
            processedUrls.add(link);

            let title =
                getText(prop, "Description > name") ||
                getText(prop, "Address > street") ||
                `Property ${reference}`;

            const priceRaw =
                getText(prop, "Price > price") ||
                getText(prop, "price");

            const price = parseFloat(priceRaw.replace(/[^\d.]/g, ""));

            const category = getText(prop, "category").toLowerCase();
            const frequency = getText(prop, "Price > frequency").toLowerCase();

            // ✅ STRONG RENT DETECTION
            const isRental =
                category.includes("let") ||
                category.includes("rent") ||
                frequency.includes("month") ||
                priceRaw.toLowerCase().includes("pcm");

            const bedrooms = parseInt(getText(prop, "bedrooms")) || null;

            const latitude = parseFloat(getText(prop, "latitude")) || null;
            const longitude = parseFloat(getText(prop, "longitude")) || null;

            if (!price) continue;

            const data = {
                link,
                title,
                price,
                bedrooms,
                isRental,
                latitude,
                longitude,
            };

            if (isRental) rentals.push(data);
            else sales.push(data);
        }

        // ====================================================================
        // SALES FIRST
        // ====================================================================
        logger.step(`Processing SALES (${sales.length})`);

        for (let i = 0; i < sales.length; i++) {
            await processProperty(sales[i], i + 1, sales.length);
        }

        // ====================================================================
        // RENTALS SECOND
        // ====================================================================
        logger.step(`Processing RENTALS (${rentals.length})`);

        for (let i = 0; i < rentals.length; i++) {
            await processProperty(rentals[i], i + 1, rentals.length);
        }

        logger.step(
            `Done → Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved} (Sales: ${stats.savedSales}, Rentals: ${stats.savedRentals})`
        );

        return { scrapeStartTime };
    } catch (err) {
        logger.error(`Error: ${err.message}`);
        throw err;
    }
}

// ============================================================================
// RUN
// ============================================================================

(async () => {
    try {
        const { scrapeStartTime } = await scrapeWilliamCharles();

        await updateRemoveStatus(AGENT_ID, scrapeStartTime);

        logger.step("Finished successfully");
        process.exit(0);
    } catch (err) {
        logger.error("Fatal:", err);
        process.exit(1);
    }
})();