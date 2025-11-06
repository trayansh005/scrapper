// Simple Crawlee scraper with Cloudflare handling
// Uses minimal approach to avoid detection
// 
// Usage: 
// AGENT_ID=70 node debug-crawlee.js
// AGENT_ID=116 node debug-crawlee.js
// AGENT_ID=118 node debug-crawlee.js
// AGENT_ID=134 node debug-crawlee.js
// AGENT_ID=135 node debug-crawlee.js
// AGENT_ID=116 START_PAGE=10 node debug-crawlee.js

const { PlaywrightCrawler } = require("crawlee");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

let totalScraped = 0;
let totalSaved = 0;

// Simple user agents
const USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0",
];

// Agent configurations
const AGENT_CONFIGS = {
    70: {
        name: "Fine & Country",
        baseUrl: "https://www.fineandcountry.co.uk",
        selectors: {
            property: ".card-property",
            link: ".property-title-link",
            title: ".property-title-link span",
            price: ".property-price",
            bedrooms: ".card__list-rooms li p",
        },
        coordinateExtractor: "map-data-attr", // Uses data-lat and data-lang from #locrating-map
        parallelDetailPages: 10, // Process 4 detail pages in parallel for faster scraping
        scrapeConfigs: [
            {
                label: "SALES",
                is_rent: false,
                totalRecords: 6009,
                recordsPerPage: 10,
                urlBuilder: (p) =>
                    `https://www.fineandcountry.co.uk/sales/property-for-sale/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${p}`,
            },
            {
                label: "LETTINGS",
                is_rent: true,
                totalRecords: 208,
                recordsPerPage: 10,
                urlBuilder: (p) =>
                    `https://www.fineandcountry.co.uk/lettings/property-to-rent/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${p}`,
            },
        ],
    },
    116: {
        name: "Gascoigne Pees",
        baseUrl: "https://www.gpees.co.uk",
        selectors: {
            property: ".hf-property-results .card",
            link: "a",
            title: ".card__text-content",
            price: ".card__heading",
            bedrooms: ".card-content__spec-list-number",
        },
        scrapeConfigs: [
            // {
            //     label: "SALES",
            //     is_rent: false,
            //     totalRecords: 512,
            //     recordsPerPage: 10,
            //     urlBuilder: (p) =>
            //         `https://www.gpees.co.uk/properties/sales/status-available/most-recent-first/page-${p}#/`,
            // },
            {
                label: "LETTINGS",
                is_rent: true,
                totalRecords: 70,
                recordsPerPage: 10,
                urlBuilder: (p) =>
                    `https://www.gpees.co.uk/properties/lettings/status-available/most-recent-first/page-${p}#/`,
            },
        ],
    },
    118: {
        name: "Mann Countrywide",
        baseUrl: "https://www.manncountrywide.co.uk",
        selectors: {
            property: ".hf-property-results .card",
            link: "a",
            title: ".card__text-content",
            price: ".card__heading",
            bedrooms: ".card-content__spec-list-number",
        },
        scrapeConfigs: [
            {
                label: "SALES",
                is_rent: false,
                totalRecords: 843,
                recordsPerPage: 10,
                urlBuilder: (p) =>
                    `https://www.manncountrywide.co.uk/properties/sales/status-available/most-recent-first/page-${p}#/`,
            },
            {
                label: "LETTINGS",
                is_rent: true,
                totalRecords: 209,
                recordsPerPage: 10,
                urlBuilder: (p) =>
                    `https://www.manncountrywide.co.uk/properties/lettings/status-available/most-recent-first/page-${p}#/`,
            },
        ],
    },
    134: {
        name: "Stratton Creber",
        baseUrl: "https://www.strattoncreber.co.uk",
        selectors: {
            property: ".hf-property-results .card",
            link: "a",
            title: ".card__text-content",
            price: ".card__heading",
            bedrooms: ".card-content__spec-list-number",
        },
        scrapeConfigs: [
            {
                label: "SALES",
                is_rent: false,
                totalRecords: 473,
                recordsPerPage: 10,
                urlBuilder: (p) =>
                    `https://www.strattoncreber.co.uk/properties/sales/status-available/most-recent-first/page-${p}#/`,
            },
            // {
            //     label: "LETTINGS",
            //     is_rent: true,
            //     totalRecords: 9,
            //     recordsPerPage: 10,
            //     urlBuilder: (p) =>
            //         `https://www.strattoncreber.co.uk/properties/lettings/status-available/most-recent-first/page-${p}#/`,
            // },
        ],
    },
    135: {
        name: "Taylors",
        baseUrl: "https://www.taylorsestateagents.co.uk",
        selectors: {
            property: ".hf-property-results .card",
            link: "a",
            title: ".card__text-content",
            price: ".card__heading",
            bedrooms: ".card-content__spec-list-number",
        },
        scrapeConfigs: [
            // {
            //     label: "SALES",
            //     is_rent: false,
            //     totalRecords: 1280,
            //     recordsPerPage: 10,
            //     urlBuilder: (p) =>
            //         `https://www.taylorsestateagents.co.uk/properties/sales/status-available/most-recent-first/page-${p}#/`,
            // },
            {
                label: "LETTINGS",
                is_rent: true,
                totalRecords: 214,
                recordsPerPage: 10,
                urlBuilder: (p) =>
                    `https://www.taylorsestateagents.co.uk/properties/lettings/status-available/most-recent-first/page-${p}#/`,
            },
        ],
    },
};

// Get agent config from environment
const AGENT_ID = parseInt(process.env.AGENT_ID || "70", 10);
const AGENT_CONFIG = AGENT_CONFIGS[AGENT_ID];

if (!AGENT_CONFIG) {
    console.error(`❌ Unknown AGENT_ID: ${AGENT_ID}`);
    process.exit(1);
}

function extractDataFromComments(html) {
    // Extract all property data from HTML comments
    const latMatch = html.match(/<!--property-latitude:"([0-9.-]+)"-->/);
    const lngMatch = html.match(/<!--property-longitude:"([0-9.-]+)"-->/);
    const bedroomsMatch = html.match(/<!--property-bedrooms:"([0-9]+)"-->/);
    const priceMatch = html.match(/<!--property-price:"([0-9]+)"-->/);
    const addressMatch = html.match(/<!--property-address:"([^"]+)"-->/);
    const typeMatch = html.match(/<!--property-type:"([^"]+)"-->/);

    return {
        latitude: latMatch ? parseFloat(latMatch[1]) : null,
        longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
        bedrooms: bedroomsMatch ? bedroomsMatch[1] : null,
        price: priceMatch ? priceMatch[1] : null,
        address: addressMatch ? addressMatch[1] : null,
        propertyType: typeMatch ? typeMatch[1] : null,
    };
}

async function extractCoordinatesFromMapElement(page) {
    // Extract coordinates from map element data attributes (for Fine & Country)
    try {
        const coords = await page.evaluate(() => {
            const mapEl = document.querySelector("#locrating-map");
            if (mapEl) {
                const lat = mapEl.getAttribute("data-lat");
                const lng = mapEl.getAttribute("data-lang");
                if (lat && lng) {
                    return {
                        latitude: parseFloat(lat),
                        longitude: parseFloat(lng),
                    };
                }
            }
            return { latitude: null, longitude: null };
        });
        return coords;
    } catch (error) {
        return { latitude: null, longitude: null };
    }
}

async function scrapeAgent() {
    console.log(`\n🚀 Starting ${AGENT_CONFIG.name} scraper...\n`);

    for (const scrapeConfig of AGENT_CONFIG.scrapeConfigs) {
        await scrapePropertyType(scrapeConfig);
    }

    console.log(`\n✅ Completed ${AGENT_CONFIG.name} — Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

async function scrapePropertyType(typeConfig) {
    const totalPages = Math.ceil(typeConfig.totalRecords / typeConfig.recordsPerPage);
    const startPage = parseInt(process.env.START_PAGE || "1", 10);

    console.log(`\n🏠 Scraping ${typeConfig.label} properties (${typeConfig.totalRecords} total) -> ${totalPages} pages`);
    console.log(`📋 Starting from page ${startPage} of ${totalPages}`);

    // Process pages one by one with long delays
    for (let pageNum = startPage; pageNum <= totalPages; pageNum++) {
        console.log(`\n📋 === Processing Page ${pageNum}/${totalPages} ===`);

        try {
            await scrapePage(pageNum, typeConfig);
        } catch (error) {
            console.error(`❌ Error on page ${pageNum}:`, error.message);

            // If blocked, wait longer
            if (error.message.includes('429') || error.message.includes('1015') || error.message.includes('blocked')) {
                console.log(`🚫 Detected blocking - waiting 60 seconds...`);
                await new Promise(resolve => setTimeout(resolve, 60000));
            }
        }

        // Wait between pages to avoid rate limiting
        if (pageNum < totalPages) {
            const delay = 1000 + Math.random() * 2000; // 1-3 seconds
            console.log(`⏳ Waiting ${Math.round(delay / 1000)}s before next page...`);
            await new Promise(resolve => setTimeout(resolve, delay));
        }
    }
}

async function scrapePage(pageNum, typeConfig) {
    const listingUrl = typeConfig.urlBuilder(pageNum);
    console.log(`📄 Fetching: ${listingUrl}`);

    const crawler = new PlaywrightCrawler({
        // Very conservative settings
        maxConcurrency: 1,
        maxRequestRetries: 0, // Don't retry to avoid getting blocked more
        requestHandlerTimeoutSecs: 600, // 10 minutes to process all properties on page

        // Stealth browser settings
        headless: true,
        launchContext: {
            launchOptions: {
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--disable-features=VizDisplayCompositor',
                    '--window-size=1920,1080',
                ],
            },
        },

        async requestHandler({ page, request }) {
            console.log(`🌐 Loading: ${request.loadedUrl}`);

            // Set stealth properties
            await page.addInitScript(() => {
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                window.chrome = { runtime: {} };
            });

            // Set user agent
            const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
            await page.setExtraHTTPHeaders({ 'User-Agent': ua });

            // Wait for content to load
            await page.waitForTimeout(2000); // Wait 2 seconds for JS to load
            await page.waitForSelector(AGENT_CONFIG.selectors.property, { timeout: 30000 }).catch(() => {
                console.log(`⚠️ No properties found on page`);
            });

            // Extract property links
            const baseUrl = AGENT_CONFIG.baseUrl;
            const linkSelector = AGENT_CONFIG.selectors.link;
            const properties = await page.$$eval(
                AGENT_CONFIG.selectors.property,
                (cards, { baseUrl, linkSelector }) => {
                    return cards.map(card => {
                        try {
                            const link = card.querySelector(linkSelector);
                            const href = link ? link.getAttribute('href') : null;

                            if (href && !href.includes('javascript:')) {
                                return href.startsWith('http') ? href : baseUrl + href;
                            }
                        } catch (e) { }
                        return null;
                    }).filter(Boolean);
                },
                { baseUrl, linkSelector }
            );

            console.log(`🔗 Found ${properties.length} properties`);

            // Check if parallel processing is enabled
            const parallelLimit = AGENT_CONFIG.parallelDetailPages || 1;

            if (parallelLimit > 1) {
                // Process properties in parallel batches
                console.log(`⚡ Processing ${parallelLimit} properties in parallel`);

                for (let i = 0; i < properties.length; i += parallelLimit) {
                    const batch = properties.slice(i, i + parallelLimit);
                    const batchPromises = batch.map(async (propUrl, batchIndex) => {
                        const propertyIndex = i + batchIndex;
                        const detailPage = await page.context().newPage();

                        try {
                            console.log(`🏠 Processing property ${propertyIndex + 1}/${properties.length}: ${propUrl}`);

                            await detailPage.goto(propUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

                            const htmlContent = await detailPage.content();
                            const commentData = extractDataFromComments(htmlContent);

                            let title = commentData.address || null;
                            let price = commentData.price || null;
                            let bedrooms = commentData.bedrooms || null;

                            if (!title) {
                                title = await detailPage.locator(AGENT_CONFIG.selectors.title).first().textContent().catch(() => null);
                            }

                            if (!price) {
                                const priceText = await detailPage.locator(AGENT_CONFIG.selectors.price).first().textContent().catch(() => null);
                                if (priceText) {
                                    const priceMatch = priceText.match(/£([\d,]+)/);
                                    if (priceMatch) price = priceMatch[1].replace(/,/g, '');
                                }
                            }

                            if (!bedrooms) {
                                const bedroomsText = await detailPage.locator(AGENT_CONFIG.selectors.bedrooms).first().textContent().catch(() => null);
                                if (bedroomsText) {
                                    const bedroomsMatch = bedroomsText.match(/\d+/);
                                    if (bedroomsMatch) bedrooms = bedroomsMatch[0];
                                }
                            }

                            let coords = {
                                latitude: commentData.latitude,
                                longitude: commentData.longitude
                            };

                            if (AGENT_CONFIG.coordinateExtractor === 'map-data-attr') {
                                const mapCoords = await extractCoordinatesFromMapElement(detailPage);
                                coords.latitude = mapCoords.latitude;
                                coords.longitude = mapCoords.longitude;
                            }

                            await updatePriceByPropertyURL(
                                propUrl,
                                price,
                                title ? title.trim() : null,
                                bedrooms,
                                AGENT_ID,
                                typeConfig.is_rent,
                                coords.latitude,
                                coords.longitude
                            );

                            totalSaved++;
                            totalScraped++;
                            console.log(`✅ Successfully saved property ${propertyIndex + 1}/${properties.length}`);
                            if (coords.latitude && coords.longitude) {
                                console.log(`📍 Coordinates: ${coords.latitude}, ${coords.longitude}`);
                            } else {
                                console.log(`⚠️  No coordinates found`);
                            }
                        } catch (propError) {
                            console.error(`❌ Error processing property ${propertyIndex + 1}: ${propError.message}`);
                        } finally {
                            await detailPage.close();
                        }
                    });

                    await Promise.all(batchPromises);
                    console.log(`✅ Batch ${Math.floor(i / parallelLimit) + 1} completed`);
                }
            } else {
                // Process properties sequentially (original behavior)
                for (let i = 0; i < properties.length; i++) {
                    const propUrl = properties[i];
                    console.log(`🏠 Processing property ${i + 1}/${properties.length}: ${propUrl}`);

                    try {
                        await page.goto(propUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

                        const htmlContent = await page.content();
                        const commentData = extractDataFromComments(htmlContent);

                        let title = commentData.address || null;
                        let price = commentData.price || null;
                        let bedrooms = commentData.bedrooms || null;

                        if (!title) {
                            title = await page.locator(AGENT_CONFIG.selectors.title).first().textContent().catch(() => null);
                        }

                        if (!price) {
                            const priceText = await page.locator(AGENT_CONFIG.selectors.price).first().textContent().catch(() => null);
                            if (priceText) {
                                const priceMatch = priceText.match(/£([\d,]+)/);
                                if (priceMatch) price = priceMatch[1].replace(/,/g, '');
                            }
                        }

                        if (!bedrooms) {
                            const bedroomsText = await page.locator(AGENT_CONFIG.selectors.bedrooms).first().textContent().catch(() => null);
                            if (bedroomsText) {
                                const bedroomsMatch = bedroomsText.match(/\d+/);
                                if (bedroomsMatch) bedrooms = bedroomsMatch[0];
                            }
                        }

                        let coords = {
                            latitude: commentData.latitude,
                            longitude: commentData.longitude
                        };

                        if (AGENT_CONFIG.coordinateExtractor === 'map-data-attr') {
                            const mapCoords = await extractCoordinatesFromMapElement(page);
                            coords.latitude = mapCoords.latitude;
                            coords.longitude = mapCoords.longitude;
                        }

                        await updatePriceByPropertyURL(
                            propUrl,
                            price,
                            title ? title.trim() : null,
                            bedrooms,
                            AGENT_ID,
                            typeConfig.is_rent,
                            coords.latitude,
                            coords.longitude
                        );

                        totalSaved++;
                        totalScraped++;
                        console.log(`✅ Successfully saved property ${i + 1}/${properties.length}`);
                        if (coords.latitude && coords.longitude) {
                            console.log(`📍 Coordinates: ${coords.latitude}, ${coords.longitude}`);
                        } else {
                            console.log(`⚠️  No coordinates found`);
                        }

                    } catch (propError) {
                        console.error(`❌ Error processing property: ${propError.message}`);
                    }
                }
            }
        },

        failedRequestHandler: async ({ request, error }) => {
            console.error(`❌ Failed: ${request.url} - ${error.message}`);

            if (error.message.includes('429') || error.message.includes('1015')) {
                throw new Error('Rate limited - stopping');
            }
        },
    });

    await crawler.run([listingUrl]);
}

// Local implementation of updateRemoveStatus
async function updateRemoveStatus(agent_id) {
    try {
        const remove_status = 1;
        await promisePool.query(
            `UPDATE property_for_sale SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
            [remove_status, agent_id]
        );
        console.log(`Removed old properties for agent ${agent_id}`);
    } catch (error) {
        console.error("Error updating remove status:", error.message);
    }
}

// Main execution
(async () => {
    try {
        await scrapeAgent();
        console.log("\n✅ All done!");
        await updateRemoveStatus(AGENT_ID);
        process.exit(0);
    } catch (err) {
        console.error("❌ Fatal error:", err?.message || err);
        process.exit(1);
    }
})();
