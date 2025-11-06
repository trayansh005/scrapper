// BridgFords scraper using Playwright with Crawlee
// Agent ID: 127
// 
// Usage: 
// node backend/scraper-agent-127.js

const { PlaywrightCrawler, log } = require("crawlee");
const { promisePool } = require("./db.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 127;
let totalScraped = 0;
let totalSaved = 0;

// Configuration for lettings only (sales is done)
const PROPERTY_TYPES = [
    { urlPath: 'properties/lettings/status-available/most-recent-first', totalRecords: 409, recordsPerPage: 10, isRental: true, label: 'LETTINGS' }
];

async function scrapeBridgFords() {
    console.log(`\n🚀 Starting BridgFords scraper (Agent ${AGENT_ID})...\n`);

    const crawler = new PlaywrightCrawler({
        maxConcurrency: 1, // Process one page at a time
        maxRequestRetries: 2,
        requestHandlerTimeoutSecs: 300,

        launchContext: {
            launchOptions: {
                headless: true,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-blink-features=AutomationControlled'
                ]
            },
        },

        async requestHandler({ page, request }) {
            const { pageNum, isRental, label } = request.userData;

            // Processing listing page
            console.log(`📋 ${label} - Page ${pageNum} - ${request.url}`);

            // No user agent setting (like agent 71)

            // Wait for properties to load
            await page.waitForTimeout(2000);
            await page.waitForSelector('.hf-property-results .card', { timeout: 30000 }).catch(() => {
                console.log(`⚠️ No properties found on page ${pageNum}`);
            });

            // Check for blocking/error messages (commented out for debugging)
            // const content = await page.content();
            // if (content.includes('Access Denied') || content.includes('blocked') || content.includes('captcha')) {
            //     console.error(`⚠️ Page ${pageNum} appears to be blocked or requires captcha`);
            //     return;
            // }

            // Extract all properties from the page
            const properties = await page.$$eval('.hf-property-results .card', (cards) => {
                const results = [];

                cards.forEach((card) => {
                    try {
                        // Extract link from anchor tag
                        const linkEl = card.querySelector('a');
                        let link = linkEl ? linkEl.getAttribute('href') : null;
                        if (link && !link.startsWith('http')) {
                            link = 'https://www.bridgfords.co.uk' + link;
                        }

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

            // If no properties found, stop pagination
            if (properties.length === 0) {
                console.log(`⚠️ No properties found on page ${pageNum}, stopping pagination`);
                return;
            }

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

        // Add all pages to the queue
        const requests = [];
        for (let page = 1; page <= totalPages; page++) {
            requests.push({
                url: `https://www.bridgfords.co.uk/${propertyType.urlPath}/page-${page}#/`,
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

    console.log(`\n✅ Completed BridgFords - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
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
        await scrapeBridgFords();
        await updateRemoveStatus(AGENT_ID);
        console.log("\n✅ All done!");
        process.exit(0);
    } catch (err) {
        console.error("❌ Fatal error:", err?.message || err);
        process.exit(1);
    }
})();