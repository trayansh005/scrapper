// Marriott Vernon scraper using Playwright with Crawlee
// Agent ID: 25
// 
// Usage: 
// node backend/scraper-agent-25.js

const { PlaywrightCrawler, log } = require("crawlee");
const { firefox } = require("playwright");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 25;
let totalScraped = 0;
let totalSaved = 0;

async function scrapeMarriottVernon() {
    console.log(`\n🚀 Starting Marriott Vernon scraper (Agent ${AGENT_ID})...\n`);

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
            const { isDetailPage, propertyData, pageNum, isRental } = request.userData;

            if (isDetailPage) {
                // Processing detail page to get coordinates
                try {
                    await page.waitForTimeout(2000);

                    let coords = { latitude: null, longitude: null };

                    // Extract coordinates from the page script
                    try {
                        const htmlContent = await page.content();
                        // Look for: "latitude": "51.3035507202148448", "longitude": "-0.0547680015"
                        const latMatch = htmlContent.match(/"latitude"\s*:\s*"(-?\d+\.?\d*)"/);
                        const lngMatch = htmlContent.match(/"longitude"\s*:\s*"(-?\d+\.?\d*)"/);

                        if (latMatch && lngMatch) {
                            coords.latitude = parseFloat(latMatch[1]);
                            coords.longitude = parseFloat(lngMatch[1]);
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
                        isRental, // true for rental, false for sale
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
                const listingType = isRental ? 'Letting' : 'Sale';
                console.log(`📋 ${listingType} - Page ${pageNum} - ${request.url}`);

                // Wait for properties to load
                await page.waitForTimeout(3000);
                await page.waitForSelector('#search-results', { timeout: 30000 }).catch(() => {
                    console.log(`⚠️ No properties found on page ${pageNum}`);
                });

                // Scroll down multiple times to load all properties (lazy loading)
                let previousHeight = 0;
                let currentHeight = await page.evaluate(() => document.body.scrollHeight);
                let scrollAttempts = 0;
                const maxScrollAttempts = 10;

                while (previousHeight !== currentHeight && scrollAttempts < maxScrollAttempts) {
                    previousHeight = currentHeight;

                    // Scroll to bottom
                    await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
                    await page.waitForTimeout(1500);

                    currentHeight = await page.evaluate(() => document.body.scrollHeight);
                    scrollAttempts++;

                    console.log(`   📜 Scroll attempt ${scrollAttempts}: height ${currentHeight}px`);
                }

                // Wait a bit more after scrolling for all properties to load
                await page.waitForTimeout(2000);

                // Extract all properties from the page
                const { properties, debug } = await page.$$eval('.col-xl-6.mb-4.property', (listings) => {
                    const results = [];
                    const debugData = { total: listings.length, processed: 0 };

                    listings.forEach((listing) => {
                        try {
                            debugData.processed++;

                            // Get the link
                            const linkEl = listing.querySelector('a.cards--property');
                            const link = linkEl ? linkEl.getAttribute('href') : null;

                            // Get title and price from h4 and h5
                            const h4El = listing.querySelector('h4');
                            const h5El = listing.querySelector('h5');

                            const h4Text = h4El ? h4El.textContent.trim() : '';
                            const h5Text = h5El ? h5El.textContent.trim() : '';

                            // Extract bedrooms from h4 (e.g., "6 Beds House - Detached - For Sale")
                            const bedroomsMatch = h4Text.match(/(\d+)\s*Bed/i);
                            const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

                            // Extract title and price from h5 (e.g., "Tandridge Road, Warlingham\n Offers in excess of £2,500,000")
                            // or "Sanderstead Road, South Croydon Guide price £550,000"
                            const lines = h5Text.split('\n').map(l => l.trim()).filter(l => l);
                            const title = lines[0] || null;

                            // Extract price - search in the full h5Text
                            let price = null;
                            const priceMatch = h5Text.match(/£([\d,]+)/);
                            if (priceMatch) {
                                price = priceMatch[1].replace(/,/g, '');
                            }

                            // Store debug info for first property
                            if (results.length === 0) {
                                debugData.firstProperty = {
                                    hasLink: !!link,
                                    hasTitle: !!title,
                                    hasPrice: !!price,
                                    price: price,
                                    h4Text: h4Text.substring(0, 60),
                                    h5Text: h5Text.substring(0, 100)
                                };
                            }

                            if (link && price && title) {
                                results.push({
                                    link: link.startsWith('http') ? link : 'https://www.marriottvernon.com' + link,
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

                // Add detail page requests to the queue
                const detailRequests = properties.map(property => ({
                    url: property.link,
                    userData: {
                        isDetailPage: true,
                        propertyData: property,
                        isRental
                    }
                }));

                await crawler.addRequests(detailRequests);

                // Check for next page
                const nextPageExists = await page.$('a.page-link[rel="next"]');
                if (nextPageExists) {
                    const nextUrl = await nextPageExists.getAttribute('href');
                    if (nextUrl) {
                        await crawler.addRequests([{
                            url: nextUrl.startsWith('http') ? nextUrl : 'https://www.marriottvernon.com' + nextUrl,
                            userData: {
                                isDetailPage: false,
                                pageNum: pageNum + 1,
                                isRental
                            }
                        }]);
                        console.log(`   ➡️  Queued page ${pageNum + 1}`);
                    }
                }
            }
        },

        failedRequestHandler({ request }) {
            console.error(`❌ Failed: ${request.url}`);
        },
    });

    // Add initial listing page URLs for both Sale and Letting
    const requests = [
        {
            url: 'https://www.marriottvernon.com/search/?showstc=off&instruction_type=Sale&address_keyword=&minprice=&maxprice=&property_type=',
            userData: { isDetailPage: false, pageNum: 1, isRental: false }
        },
        {
            url: 'https://www.marriottvernon.com/search/?showstc=off&instruction_type=Letting&address_keyword=&minprice=&maxprice=&property_type=',
            userData: { isDetailPage: false, pageNum: 1, isRental: true }
        }
    ];

    await crawler.addRequests(requests);
    await crawler.run();

    console.log(`\n✅ Completed Marriott Vernon - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
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
        await scrapeMarriottVernon();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n✅ All done!");
        process.exit(0);
    } catch (err) {
        console.error("❌ Fatal error:", err?.message || err);
        process.exit(1);
    }
})();
