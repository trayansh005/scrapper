// BHHS London Properties scraper using Playwright with Crawlee
// Agent ID: 63
// 
// Usage: 
// node backend/scraper-agent-63.js

const { PlaywrightCrawler, log } = require("crawlee");
const { firefox } = require("playwright");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 63;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for sales and lettings
const PROPERTY_TYPES = [
    { urlPath: 'properties-for-sale', totalRecords: 116, recordsPerPage: 20, isRental: false, label: 'SALES' },
    { urlPath: 'properties-for-rent', totalRecords: 74, recordsPerPage: 20, isRental: true, label: 'LETTINGS' }
];

async function scrapeBHHSLondon() {
    console.log(`\n🚀 Starting BHHS London Properties scraper (Agent ${AGENT_ID})...\n`);

    const crawler = new PlaywrightCrawler({
        maxConcurrency: 1,
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 300,

        launchContext: {
            launcher: firefox,
            launchOptions: {
                headless: true,
            },
        },

        async requestHandler({ page, request }) {
            const { isDetailPage, propertyData, pageNum, isRental, label } = request.userData;

            if (isDetailPage) {
                // Processing detail page to get coordinates
                try {
                    await page.waitForTimeout(1000);

                    let coords = { latitude: null, longitude: null };

                    // Extract coordinates from #mapView data attributes
                    try {
                        const mapView = await page.$('#mapView');
                        if (mapView) {
                            const lat = await mapView.getAttribute('data-lat');
                            const lon = await mapView.getAttribute('data-lon');

                            if (lat && lon) {
                                coords.latitude = parseFloat(lat);
                                coords.longitude = parseFloat(lon);
                            }
                        }
                    } catch (err) {
                        // Coordinates not found
                    }

                    await updatePriceByPropertyURL(
                        propertyData.link,
                        propertyData.price,
                        propertyData.title,
                        propertyData.bedrooms,
                        AGENT_ID,
                        isRental,
                        coords.latitude,
                        coords.longitude
                    );

                    totalSaved++;
                    totalScraped++;

                    if (coords.latitude && coords.longitude) {
                        console.log(`✅ ${propertyData.title} - £${propertyData.price} - ${coords.latitude}, ${coords.longitude}`);
                    } else {
                        console.log(`✅ ${propertyData.title} - £${propertyData.price} - No coords`);
                    }
                } catch (error) {
                    console.error(`❌ Error saving property: ${error.message}`);
                }
            } else {
                // Processing listing page
                console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

                // Wait for properties to load
                await page.waitForTimeout(2000);
                await page.waitForSelector('.property-card', { timeout: 30000 }).catch(() => {
                    console.log(`⚠️ No properties found on page ${pageNum}`);
                });

                // Extract all properties from the page
                const { properties, debug } = await page.$$eval('.property-card', (cards) => {
                    const results = [];
                    const debugData = { total: cards.length, processed: 0 };

                    cards.forEach((card) => {
                        try {
                            debugData.processed++;

                            // Extract link
                            const linkEl = card.querySelector('a');
                            const link = linkEl ? linkEl.getAttribute('href') : null;

                            // Extract title from h3.md-heading
                            const titleEl = card.querySelector('h3.md-heading');
                            const title = titleEl ? titleEl.textContent.trim() : null;

                            // Extract bedrooms from first p.text-sm.text-white
                            let bedrooms = null;
                            const bedroomsEl = card.querySelector('p.text-sm.text-white');
                            if (bedroomsEl) {
                                const bedroomsText = bedroomsEl.textContent.trim();
                                const bedroomsMatch = bedroomsText.match(/(\d+)\s*Bedrooms/);
                                if (bedroomsMatch) {
                                    bedrooms = bedroomsMatch[1];
                                }
                            }

                            // Extract price
                            let price = null;
                            const priceEl = card.querySelector('.price');
                            if (priceEl) {
                                const priceText = priceEl.textContent.trim();
                                const priceMatch = priceText.match(/£([\d,]+)/);
                                if (priceMatch) {
                                    price = priceMatch[1].replace(/,/g, '');
                                }
                            } else {
                                // Try alternative price location
                                const altPriceEl = card.querySelector('p.md-heading:last-child');
                                if (altPriceEl) {
                                    const priceText = altPriceEl.textContent.trim();
                                    if (priceText.includes('POA')) {
                                        price = 'POA';
                                    } else {
                                        const priceMatch = priceText.match(/£([\d,]+)/);
                                        if (priceMatch) {
                                            price = priceMatch[1].replace(/,/g, '');
                                        }
                                    }
                                }
                            }

                            // Store debug info for first property
                            if (results.length === 0) {
                                debugData.firstProperty = {
                                    hasLink: !!link,
                                    hasTitle: !!title,
                                    hasPrice: !!price,
                                    price: price,
                                    title: title ? title.substring(0, 60) : null
                                };
                            }

                            if (link && price && title) {
                                results.push({
                                    link: link,
                                    title: title,
                                    price,
                                    bedrooms
                                });
                            }
                        } catch (err) {
                            debugData.errors = (debugData.errors || 0) + 1;
                        }
                    });

                    return { properties: results, debug: debugData };
                });

                console.log(`🔍 Extraction debug:`, debug);
                console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

                // Add detail page requests to the queue with delay
                for (let i = 0; i < properties.length; i++) {
                    const property = properties[i];
                    await crawler.addRequests([{
                        url: property.link,
                        userData: {
                            isDetailPage: true,
                            propertyData: property,
                            isRental
                        }
                    }]);

                    // Add delay between detail page requests to avoid rate limiting
                    if (i < properties.length - 1) {
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                }
            }
        },

        failedRequestHandler({ request }) {
            console.error(`❌ Failed: ${request.url}`);
        },
    });

    // Add initial listing page URLs for both sales and lettings
    const requests = [];

    for (const propertyType of PROPERTY_TYPES) {
        const totalPages = Math.ceil(propertyType.totalRecords / propertyType.recordsPerPage);
        console.log(`🏠 Queueing ${propertyType.label} properties (${propertyType.totalRecords} total, ${totalPages} pages)`);

        for (let page = 1; page <= totalPages; page++) {
            requests.push({
                url: `https://www.bhhslondonproperties.com/${propertyType.urlPath}?location=&page=${page}`,
                userData: {
                    isDetailPage: false,
                    pageNum: page,
                    isRental: propertyType.isRental,
                    label: propertyType.label
                }
            });
        }
    }

    await crawler.addRequests(requests);
    await crawler.run();

    console.log(`\n✅ Completed BHHS London Properties - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
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
        await scrapeBHHSLondon();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n✅ All done!");
        process.exit(0);
    } catch (err) {
        console.error("❌ Fatal error:", err?.message || err);
        process.exit(1);
    }
})();
