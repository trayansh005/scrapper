// Jackie Quinn scraper using Puppeteer with Crawlee
// Agent ID: 8
// 
// Usage: 
// node backend/scraper-agent-8.js

const { PuppeteerCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 8;
let totalScraped = 0;
let totalSaved = 0;

// Extract coordinates from Google Maps link
function extractCoordinatesFromHTML(html) {
    // Look for pattern: @latitude,longitude,zoom in Google Maps URLs
    const atMatch = html.match(/@([0-9.-]+),([0-9.-]+),\d+z/);
    if (atMatch) {
        return {
            latitude: parseFloat(atMatch[1]),
            longitude: parseFloat(atMatch[2])
        };
    }

    // Fallback to ll= pattern
    const llMatch = html.match(/ll=([0-9.-]+),([0-9.-]+)/);
    if (llMatch) {
        return {
            latitude: parseFloat(llMatch[1]),
            longitude: parseFloat(llMatch[2])
        };
    }

    return { latitude: null, longitude: null };
}

async function scrapeJackieQuinn() {
    console.log(`\n🚀 Starting Jackie Quinn scraper (Agent ${AGENT_ID})...\n`);

    const crawler = new PuppeteerCrawler({
        maxConcurrency: 1, // Process sequentially
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 300,

        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                ],
            },
        },

        async requestHandler({ page, request }) {
            const { pageNum, isDetailPage, propertyData } = request.userData;

            if (isDetailPage) {
                // Processing detail page to get coordinates
                try {
                    // Wait for the map link to be available
                    await page.waitForSelector('a[href*="mapcontainer"]', { timeout: 10000 }).catch(() => {
                        console.log(`⚠️ No map link found for ${propertyData.title}`);
                    });

                    // Click the map link to load coordinates
                    const mapLinkClicked = await page.evaluate(() => {
                        const mapLink = document.querySelector('a[href*="mapcontainer"]');
                        if (mapLink) {
                            mapLink.click();
                            return true;
                        }
                        return false;
                    });

                    let coords = { latitude: null, longitude: null };

                    if (mapLinkClicked) {
                        // Wait a bit for the map to load
                        await page.waitForTimeout(1500);

                        // Extract coordinates from the loaded content
                        const htmlContent = await page.content();
                        coords = extractCoordinatesFromHTML(htmlContent);
                    }

                    await updatePriceByPropertyURL(
                        propertyData.link,
                        propertyData.price,
                        propertyData.title,
                        propertyData.bedrooms,
                        AGENT_ID,
                        false,
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
                console.log(`📋 Page ${pageNum}/11 - ${request.url}`);

                await page.waitForSelector('.propertyBox', { timeout: 30000 }).catch(() => {
                    console.log(`⚠️ No properties found on page ${pageNum}`);
                });

                // Extract all properties from the page
                const properties = await page.$$eval('.propertyBox', (listings) => {
                    const items = [];

                    listings.forEach((listing) => {
                        try {
                            const linkEl = listing.querySelector('h2.searchProName a');
                            const link = linkEl ? linkEl.getAttribute('href') : null;

                            const titleEl = listing.querySelector('h2.searchProName a');
                            const title = titleEl ? titleEl.textContent.trim() : null;

                            const priceEl = listing.querySelector('h3 div');
                            const priceText = priceEl ? priceEl.textContent.trim() : '';

                            // Skip if "Sold Subject To Contract"
                            if (priceText.includes('Sold Subject To Contract')) {
                                return;
                            }

                            const priceMatch = priceText.match(/£([\d,]+)/);
                            const price = priceMatch ? priceMatch[1].replace(/,/g, '') : null;

                            const descEl = listing.querySelector('.featuredDescriptions');
                            const description = descEl ? descEl.textContent.trim() : '';
                            const bedroomMatch = description.match(/(\d+)\s+BEDROOM/i);
                            const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

                            if (link && title && price) {
                                items.push({
                                    link: link.startsWith('http') ? link : 'https://www.jackiequinn.co.uk' + link,
                                    title,
                                    price,
                                    bedrooms
                                });
                            }
                        } catch (err) {
                            // Silent error
                        }
                    });

                    return items;
                });

                console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

                // Add detail page requests to the queue
                const detailRequests = properties.map(property => ({
                    url: property.link,
                    userData: {
                        isDetailPage: true,
                        propertyData: property
                    }
                }));

                await crawler.addRequests(detailRequests);
            }
        },

        failedRequestHandler({ request }) {
            console.error(`❌ Failed: ${request.url}`);
        },
    });

    // Add all listing page URLs
    const requests = [];
    for (let pageNum = 1; pageNum <= 11; pageNum++) {
        requests.push({
            url: `https://www.jackiequinn.co.uk/search?category=1&listingtype=5&statusids=1%2C10%2C4%2C16%2C3&obc=Price&obd=Descending&page=${pageNum}`,
            userData: { pageNum, isDetailPage: false }
        });
    }

    await crawler.addRequests(requests);
    await crawler.run();

    console.log(`\n✅ Completed Jackie Quinn - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
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
        await scrapeJackieQuinn();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n✅ All done!");
        process.exit(0);
    } catch (err) {
        console.error("❌ Fatal error:", err?.message || err);
        process.exit(1);
    }
})();
