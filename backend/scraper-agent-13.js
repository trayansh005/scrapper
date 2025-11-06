// Bairstow Eves scraper using Playwright with Crawlee
// Agent ID: 13
// 
// Usage: 
// node backend/scraper-agent-13.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 13;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for sales and rentals
const PROPERTY_TYPES = [
    { urlPath: 'properties/sales/status-available/most-recent-first', totalRecords: 2825, recordsPerPage: 50, isRental: false, label: 'SALES' },
    // { urlPath: 'properties/lettings/status-available/most-recent-first', totalRecords: 634, recordsPerPage: 50, isRental: true, label: 'LETTINGS' }
];

async function scrapeBairstowEves() {
    console.log(`\n🚀 Starting Bairstow Eves scraper (Agent ${AGENT_ID})...\n`);

    const crawler = new PlaywrightCrawler({
        maxConcurrency: 1, // Process one page at a time
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 300,

        launchContext: {
            launchOptions: {
                headless: true,
                args: ['--no-sandbox', '--disable-setuid-sandbox']
            },
        },

        async requestHandler({ page, request }) {
            const { pageNum, isRental, label } = request.userData;

            // Processing listing page
            console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

            // Wait for properties to load
            await page.waitForTimeout(2000); // Reduced from 3000ms
            await page.waitForSelector('.card', { timeout: 30000 }).catch(() => {
                console.log(`⚠️ No properties found on page ${pageNum}`);
            });

            // Extract all properties from the page
            const properties = await page.$$eval('.card', (cards) => {
                const results = [];

                cards.forEach((card) => {
                    try {
                        // Extract link from a.card__link
                        const linkEl = card.querySelector('a.card__link');
                        const link = linkEl ? linkEl.getAttribute('href') : null;

                        // Extract title from .card__text-content
                        const titleEl = card.querySelector('.card__text-content');
                        const title = titleEl ? titleEl.textContent.trim() : null;

                        // Extract bedrooms from .card-content__spec-list-number (first occurrence)
                        const bedroomsEl = card.querySelector('.card-content__spec-list-number');
                        let bedrooms = null;
                        if (bedroomsEl) {
                            const bedroomsText = bedroomsEl.textContent.trim();
                            const bedroomsMatch = bedroomsText.match(/\d+/);
                            if (bedroomsMatch) {
                                bedrooms = bedroomsMatch[0];
                            }
                        }

                        // Extract price from .card__heading
                        const priceEl = card.querySelector('.card__heading');
                        let price = null;
                        if (priceEl) {
                            const priceText = priceEl.textContent.trim();
                            const priceMatch = priceText.match(/£([\d,]+)/);
                            if (priceMatch) {
                                price = priceMatch[1].replace(/,/g, '');
                            }
                        }

                        if (link && price && title) {
                            results.push({
                                link: link.startsWith('http') ? link : `https://www.bairstoweves.co.uk${link}`,
                                title: title,
                                price,
                                bedrooms
                            });
                        }
                    } catch (err) {
                        // Skip this card if error
                    }
                });

                return results;
            });

            console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

            // Process each property one by one
            for (let i = 0; i < properties.length; i++) {
                const property = properties[i];

                // Navigate to detail page directly
                try {
                    await page.goto(property.link, { waitUntil: 'domcontentloaded', timeout: 30000 });
                    await page.waitForTimeout(1000);

                    let coords = { latitude: null, longitude: null };

                    // Extract coordinates from HTML comments
                    const htmlContent = await page.content();
                    const latMatch = htmlContent.match(/<!--property-latitude:"([0-9.-]+)"-->/);
                    const lngMatch = htmlContent.match(/<!--property-longitude:"([0-9.-]+)"-->/);

                    if (latMatch && lngMatch) {
                        coords.latitude = parseFloat(latMatch[1]);
                        coords.longitude = parseFloat(lngMatch[1]);
                    }

                    await updatePriceByPropertyURL(
                        property.link,
                        property.price,
                        property.title,
                        property.bedrooms,
                        AGENT_ID,
                        isRental,
                        coords.latitude,
                        coords.longitude
                    );

                    totalSaved++;
                    totalScraped++;

                    if (coords.latitude && coords.longitude) {
                        console.log(`✅ ${property.title} - £${property.price} - ${coords.latitude}, ${coords.longitude}`);
                    } else {
                        console.log(`✅ ${property.title} - £${property.price} - No coords`);
                    }
                } catch (error) {
                    console.error(`❌ Error processing ${property.link}: ${error.message}`);
                }

                // Delay between properties
                await new Promise(resolve => setTimeout(resolve, 500));
            }
        },

        failedRequestHandler({ request }) {
            console.error(`❌ Failed: ${request.url}`);
        },
    });

    // Process property types one by one
    for (const propertyType of PROPERTY_TYPES) {
        const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
        console.log(`🏠 Processing ${propertyType.label} properties (${propertyType.totalRecords} total, ${totalPages} pages)\n`);

        // Add all listing pages to the queue (starting from page 45)
        const listingRequests = [];
        const startPage = 55;
        for (let page = startPage; page <= totalPages; page++) {
            listingRequests.push({
                url: `https://www.bairstoweves.co.uk/${propertyType.urlPath}/page-${page}#/`,
                userData: {
                    pageNum: page,
                    isRental: propertyType.isRental,
                    label: propertyType.label
                }
            });
        }

        console.log(`📋 Starting from page ${startPage} to ${totalPages}`);

        await crawler.addRequests(listingRequests);
        await crawler.run();
    }

    console.log(`\n✅ Completed Bairstow Eves - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

// Local implementation of updateRemoveStatus
async function updateRemoveStatus(agent_id) {
    try {
        const remove_status = 1;
        await promisePool.query(
            `UPDATE property_for_sale SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
            [remove_status, agent_id]
        );
        console.log(`🧹 Removed old properties for agent ${agent_id}`);
    } catch (error) {
        console.error("Error updating remove status:", error.message);
    }
}

// Main execution
(async () => {
    try {
        await scrapeBairstowEves();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n✅ All done!");
        process.exit(0);
    } catch (err) {
        console.error("❌ Fatal error:", err?.message || err);
        process.exit(1);
    }
})();
