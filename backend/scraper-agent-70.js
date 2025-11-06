// Fine & Country scraper using Playwright with Crawlee
// Agent ID: 70
// 
// Usage: 
// node backend/scraper-agent-70.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 70;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for sales and lettings
const PROPERTY_TYPES = [
    // { urlPath: 'sales/property-for-sale', totalPages: 583, recordsPerPage: 10, isRental: false, label: 'SALES' },
    { urlPath: 'lettings/property-to-rent', totalPages: 21, recordsPerPage: 10, isRental: true, label: 'LETTINGS' }
];

async function scrapeFineAndCountry() {
    console.log(`\n🚀 Starting Fine & Country scraper (Agent ${AGENT_ID})...\n`);

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
            await page.waitForSelector('.card-property', { timeout: 30000 }).catch(() => {
                console.log(`⚠️ No properties found on page ${pageNum}`);
            });

            // Extract all properties from the page
            const properties = await page.$$eval('.card-property', (cards) => {
                const results = [];

                cards.forEach((card) => {
                    try {
                        // Extract link from .property-title-link
                        const linkEl = card.querySelector('.property-title-link');
                        const link = linkEl ? linkEl.getAttribute('href') : null;

                        // Extract title from .property-title-link span
                        const titleEl = card.querySelector('.property-title-link span');
                        const title = titleEl ? titleEl.textContent.trim() : null;

                        // Extract price from .property-price
                        const priceEl = card.querySelector('.property-price');
                        let price = null;
                        if (priceEl) {
                            const priceText = priceEl.textContent.trim();
                            const priceMatch = priceText.match(/£([\d,]+)/);
                            if (priceMatch) {
                                price = priceMatch[1].replace(/,/g, '');
                            }
                        }

                        // Extract bedrooms from .card__list-rooms li p
                        const bedroomsEl = card.querySelector('.card__list-rooms li p');
                        const bedrooms = bedroomsEl ? bedroomsEl.textContent.trim() : null;

                        if (link && title && price) {
                            results.push({
                                link: link,
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

            // Process properties in batches of 5
            const batchSize = 5;
            for (let i = 0; i < properties.length; i += batchSize) {
                const batch = properties.slice(i, i + batchSize);

                // Process batch in parallel
                await Promise.all(batch.map(async (property) => {
                    // Create a new page for each property in the batch
                    const detailPage = await page.context().newPage();

                    try {
                        await detailPage.goto(property.link, { waitUntil: 'domcontentloaded', timeout: 30000 });
                        await detailPage.waitForTimeout(300);

                        let coords = { latitude: null, longitude: null };

                        // Extract coordinates from map element data attributes
                        const geoData = await detailPage.evaluate(() => {
                            const mapEl = document.querySelector('#locrating-map');
                            if (mapEl) {
                                const lat = mapEl.getAttribute('data-lat');
                                const lng = mapEl.getAttribute('data-lang');
                                if (lat && lng) {
                                    return {
                                        latitude: parseFloat(lat),
                                        longitude: parseFloat(lng)
                                    };
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
        console.log(`🏠 Processing ${propertyType.label} properties (${propertyType.totalPages} pages)\n`);

        // Add all pages to the queue
        const requests = [];
        for (let page = 1; page <= propertyType.totalPages; page++) {
            requests.push({
                url: `https://www.fineandcountry.co.uk/${propertyType.urlPath}/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${page}`,
                userData: {
                    pageNum: page,
                    isRental: propertyType.isRental,
                    label: propertyType.label
                }
            });
        }

        await crawler.addRequests(requests);
        await crawler.run();
    }

    console.log(`\n✅ Completed Fine & Country - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
}

// Local implementation of updateRemoveStatus
async function updateRemoveStatus(agent_id) {
    try {
        const remove_status = 1;
        await promisePool.query(
            `UPDATE property_for_rent SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
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
        // await scrapeFineAndCountry();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n✅ All done!");
        process.exit(0);
    } catch (err) {
        console.error("❌ Fatal error:", err?.message || err);
        process.exit(1);
    }
})();