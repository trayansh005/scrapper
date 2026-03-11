// Connells Master Scraper using Playwright with Crawlee
// Agent ID: 46
// Implementation: RSC extraction & incremental pagination
// node backend/scraper-agent-46.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
    updatePriceByPropertyURLOptimized,
    processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
    parsePrice,
    formatPriceUk,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 46;
const logger = createAgentLogger(AGENT_ID);

const BASE_URL = "https://www.connells.co.uk/properties/sales";

const stats = {
    totalSaved: 0,
    created: 0,
    updated: 0,
};

const processedUrls = new Set();

function normalizeRscText(text) {
    let normalized = text;

    for (let index = 0; index < 2; index += 1) {
        normalized = normalized.replace(/\\"/g, '"').replace(/\\\\/g, "\\");
    }

    return normalized;
}

function extractJsonBlock(text, anchor, openingChar, closingChar) {
    const anchorIndex = text.indexOf(anchor);
    if (anchorIndex === -1) {
        return null;
    }

    const startIndex = text.indexOf(openingChar, anchorIndex + anchor.length);
    if (startIndex === -1) {
        return null;
    }

    let depth = 0;
    let inString = false;
    let escaped = false;

    for (let index = startIndex; index < text.length; index += 1) {
        const char = text[index];

        if (escaped) {
            escaped = false;
            continue;
        }

        if (char === "\\") {
            escaped = true;
            continue;
        }

        if (char === '"') {
            inString = !inString;
            continue;
        }

        if (inString) {
            continue;
        }

        if (char === openingChar) {
            depth += 1;
        } else if (char === closingChar) {
            depth -= 1;
            if (depth === 0) {
                return text.slice(startIndex, index + 1);
            }
        }
    }

    return null;
}

function extractListingData(text) {
    const normalizedText = normalizeRscText(text);
    const properties = [];
    let searchIndex = 0;
    let hasNextPage = false;

    while (searchIndex < normalizedText.length) {
        const matchIndex = normalizedText.indexOf('"initialProperties":', searchIndex);
        if (matchIndex === -1) {
            break;
        }

        const slice = normalizedText.slice(matchIndex);
        const propertiesJson = extractJsonBlock(slice, '"initialProperties":', "[", "]");
        const paginationJson = extractJsonBlock(slice, '"initialPagination":', "{", "}");

        if (propertiesJson) {
            try {
                const parsedProperties = JSON.parse(propertiesJson);
                if (Array.isArray(parsedProperties)) {
                    properties.push(...parsedProperties);
                }
            } catch (error) {}
        }

        if (paginationJson) {
            try {
                const pagination = JSON.parse(paginationJson);
                if (pagination && pagination.hasNextPage) {
                    hasNextPage = true;
                }
            } catch (error) {}
        }

        searchIndex = matchIndex + '"initialProperties":'.length;
    }

    return { properties, hasNextPage };
}

const crawler = new PlaywrightCrawler({
    maxRequestRetries: 3,
    requestHandlerTimeoutSecs: 180,
    navigationTimeoutSecs: 120,
    preNavigationHooks: [
        async ({ page }) => {
            await blockNonEssentialResources(page);
        },
    ],
    launchContext: {
        launchOptions: {
            headless: true,
        },
    },
    async requestHandler({ page, request, crawler }) {
        const { url, userData } = request;
        const pageNum = userData.pageNum || 1;

        logger.page(pageNum, "SALES", url);

        const response = await page.goto(url, {
            waitUntil: "domcontentloaded",
            timeout: 90000,
        });

        if (!response) {
            throw new Error(`Failed to load ${url}`);
        }

        await page.waitForTimeout(1500);

        const scriptText = await page.evaluate(() => {
            return Array.from(document.querySelectorAll("script"))
                .map((script) => script.textContent || "")
                .join("\n");
        });

        const { properties, hasNextPage } = extractListingData(scriptText);

        const uniqueProps = Array.from(new Map(properties.map(p => [p.id, p])).values());
        
        logger.page(pageNum, "SALES", `Found ${uniqueProps.length} properties`);

        for (const prop of uniqueProps) {
            const title = prop.displayAddress || prop.roadName || "Property";
            const priceDisplay = prop.price || prop.formattedPrice || "";
            const price = parsePrice(priceDisplay);
            const linkPath = prop.urlLabelWithKeyword || prop.urlLabel;
            const link = linkPath ? `https://www.connells.co.uk${linkPath}` : null;

            if (!link || !price) {
                continue;
            }

            if (processedUrls.has(link)) {
                continue;
            }
            processedUrls.add(link);

            const result = await updatePriceByPropertyURLOptimized(
                link,
                price,
                title,
                prop.bedrooms,
                AGENT_ID,
                false,
            );

            let action = "UNCHANGED";

            if (result.updated) {
                action = "UPDATED";
                stats.totalSaved += 1;
                stats.updated += 1;
            }

            if (!result.isExisting && !result.error) {
                await processPropertyWithCoordinates(
                    link,
                    price,
                    title,
                    prop.bedrooms,
                    AGENT_ID,
                    false,
                    null,
                    prop.lat || null,
                    prop.lng || null,
                );

                action = "CREATED";
                stats.totalSaved += 1;
                stats.created += 1;
            }

            logger.property(
                pageNum,
                "SALES",
                title.substring(0, 40),
                priceDisplay || formatPriceUk(price),
                link,
                false,
                null,
                action,
                prop.lat || null,
                prop.lng || null,
            );
        }
        
        if (hasNextPage && pageNum < 450) { 
            const nextLink = `${BASE_URL}?page=${pageNum + 1}`;
            await crawler.addRequests([{
                url: nextLink,
                userData: { pageNum: pageNum + 1 }
            }]);
        }
    },

    async failedRequestHandler({ request }) {
        logger.error(`FAILED: ${request.url}`);
    },
});

async function run() {
    const scrapeStartTime = new Date();

    logger.step("Starting Connells Scraper...");
    await crawler.run([{
        url: BASE_URL,
        userData: { pageNum: 1 }
    }]);
    await updateRemoveStatus(AGENT_ID, scrapeStartTime);
    logger.step(`Scraper finished. Saved ${stats.totalSaved} properties (${stats.created} created, ${stats.updated} updated).`);
}

run();