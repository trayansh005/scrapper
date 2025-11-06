// BELVOIR scraper using Playwright with Crawlee
// Agent ID: 107
// 
// Usage: 
// node backend/scraper-agent-107.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool, updatePriceByPropertyURL } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 107;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for sales and rentals
const PROPERTY_TYPES = [
    { urlPath: 'for-sale', totalRecords: 3672, recordsPerPage: 11, isRental: false, label: 'SALES' },
    // { urlPath: 'for-rent', totalRecords: 1300, recordsPerPage: 11, isRental: true, label: 'RENTALS' }
];

async function scrapeBelvoir() {
    console.log(`\n🚀 Starting BELVOIR scraper (Agent ${AGENT_ID})...\n`);

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
            await page.waitForTimeout(2000);
            await page.waitForSelector('.tease-property', { timeout: 30000 }).catch(() => {
                console.log(`⚠️ No properties found on page ${pageNum}`);
            });

            // Extract all properties from the page
            const properties = await page.$$eval('.tease-property', (elements) => {
                const results = [];

                elements.forEach((element) => {
                    try {
                        // Extract link from .text-link
                        const linkEl = element.querySelector('.text-link');
                        let link = linkEl ? linkEl.getAttribute('href') : null;
                        if (link && !link.startsWith('http')) {
                            link = 'https://www.belvoir.co.uk' + link;
                        }

                        // Extract title from .addr1 and .addr2
                        const addr1 = element.querySelector('.addr1')?.textContent || '';
                        const addr2 = element.querySelector('.addr2')?.textContent || '';
                        const title = [addr1, addr2]
                            .map(t => t.replace(/\s+/g, ' ').trim())
                            .filter(Boolean)
                            .join(', ');

                        // Extract bedrooms from bedroom icon's next sibling
                        const bedroomsText = element.querySelector('.bedroom-icon')?.nextElementSibling?.textContent?.trim() || '';
                        const bedroomsMatch = bedroomsText.match(/\d+/);
                        const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

                        // Extract price from .amount
                        const priceText = element.querySelector('.amount')?.textContent?.trim() || '';
                        const priceMatch = priceText.match(/£([\d,]+)/);
                        const price = priceMatch ? priceMatch[1].replace(/,/g, '') : null;

                        if (link && title && price) {
                            results.push({
                                link: link,
                                title: title,
                                price,
                                bedrooms
                            });
                        }
                    } catch (err) {
                        // Skip this property if error
                    }
                });

                return results;
            });

            console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);

            // Process properties in batches of 3
            const batchSize = 5;
            for (let i = 0; i < properties.length; i += batchSize) {
                const batch = properties.slice(i, i + batchSize);

                // Process batch in parallel
                await Promise.all(batch.map(async (property) => {
                    // Create a new page for each property in the batch
                    const detailPage = await page.context().newPage();

                    try {
                        await detailPage.goto(property.link, { waitUntil: 'domcontentloaded', timeout: 20000 });
                        await detailPage.waitForTimeout(300);

                        let coords = { latitude: null, longitude: null };

                        // Extract coordinates from JSON-LD script tags
                        const geoData = await detailPage.evaluate(() => {
                            const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
                            for (const script of scripts) {
                                try {
                                    const data = JSON.parse(script.textContent);
                                    if (data.geo?.latitude && data.geo?.longitude) {
                                        return {
                                            latitude: data.geo.latitude,
                                            longitude: data.geo.longitude
                                        };
                                    }
                                } catch (e) {
                                    // Continue to next script
                                }
                            }
                            return null;
                        });

                        if (geoData) {
                            coords.latitude = geoData.latitude;
                            coords.longitude = geoData.longitude;
                        }

                        // Debug log which table we're using
                        const tableName = isRental ? "property_for_rent" : "property_for_sale";
                        console.log(`🔍 Saving to table: ${tableName} | Rental: ${isRental} | URL: ${property.link.substring(0, 60)}...`);

                        // Check if property already exists in database with this agent_id
                        const [existingRows] = await promisePool.query(
                            `SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
                            [property.link.trim(), AGENT_ID]
                        );

                        // Also check if it exists with different agent_id
                        const [otherAgentRows] = await promisePool.query(
                            `SELECT agent_id, property_name FROM ${tableName} WHERE property_url = ? AND agent_id != ?`,
                            [property.link.trim(), AGENT_ID]
                        );

                        if (existingRows.length > 0) {
                            console.log(`🔍 Property exists with our agent_id: ${AGENT_ID} - will update`);

                            // Update existing property with our agent_id
                            await promisePool.query(
                                `UPDATE ${tableName} SET price = ?, latitude = ?, longitude = ?, updated_at = NOW() WHERE property_url = ? AND agent_id = ?`,
                                [property.price, coords.latitude, coords.longitude, property.link.trim(), AGENT_ID]
                            );
                            console.log(`✅ Updated: ${property.link.substring(0, 50)}... | Price: £${property.price} | Coords: ${coords.latitude}, ${coords.longitude}`);

                        } else if (otherAgentRows.length > 0) {
                            console.log(`🔍 Property exists with different agent_id: ${otherAgentRows[0].agent_id} - will create new entry for agent ${AGENT_ID}`);

                            // Create new property for our agent_id
                            const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                            const logo = "property_for_sale/logo.png";
                            const currentTime = new Date();

                            await promisePool.query(insertQuery, [
                                property.title,
                                AGENT_ID,
                                property.price,
                                property.bedrooms,
                                property.link.trim(),
                                logo,
                                coords.latitude,
                                coords.longitude,
                                currentTime,
                                currentTime,
                            ]);
                            console.log(`✅ Created: ${property.link.substring(0, 50)}... | Price: £${property.price} | Coords: ${coords.latitude}, ${coords.longitude}`);

                        } else {
                            console.log(`🔍 Property does not exist - will create new`);

                            // Create new property
                            const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;
                            const logo = "property_for_sale/logo.png";
                            const currentTime = new Date();

                            await promisePool.query(insertQuery, [
                                property.title,
                                AGENT_ID,
                                property.price,
                                property.bedrooms,
                                property.link.trim(),
                                logo,
                                coords.latitude,
                                coords.longitude,
                                currentTime,
                                currentTime,
                            ]);
                            console.log(`✅ Created: ${property.link.substring(0, 50)}... | Price: £${property.price} | Coords: ${coords.latitude}, ${coords.longitude}`);
                        }

                        totalSaved++;
                        totalScraped++;

                        if (coords.latitude && coords.longitude) {
                            console.log(`✅ ${property.title} - £${property.price} - ${coords.latitude}, ${coords.longitude}`);
                        } else {
                            console.log(`✅ ${property.title} - £${property.price} - No coords`);
                        }
                    } catch (error) {
                        console.error(`❌ Error processing ${property.link}: ${error.message}`);
                    } finally {
                        await detailPage.close();
                    }
                }));

                // Delay between batches
                await new Promise(resolve => setTimeout(resolve, 200));
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

        // Add all listing pages to the queue (starting from page 1)
        const listingRequests = [];
        const startPage = 130;
        for (let page = startPage; page <= totalPages; page++) {
            listingRequests.push({
                url: `https://www.belvoir.co.uk/properties/${propertyType.urlPath}/?per_page=11&drawMap=&address=&address_lat_lng=&price_min=&price_max=&bedrooms_min=-1&hide_under_offer=on&yield_min=&yield_max=&pg=${page}`,
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

    console.log(`\n✅ Completed BELVOIR - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
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
        await scrapeBelvoir();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n✅ All done!");
        process.exit(0);
    } catch (err) {
        console.error("❌ Fatal error:", err?.message || err);
        process.exit(1);
    }
})();