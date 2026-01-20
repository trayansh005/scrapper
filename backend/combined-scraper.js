const Hero = require("@ulixee/hero");
const { updatePriceByPropertyURL, updateRemoveStatus, promisePool } = require("./db.js");

// Keywords to identify sold properties
const SOLD_KEYWORDS = [
    'sold subject to contract',
    'sold stc',
    'sold',
    'under offer',
    'let agreed',
    'let',
    'withdrawn',
    'off market'
];

// Combined scraper for multiple agents using Hero
const AGENTS = [
    {
        id: 4,
        name: "Marsh & Parsons",
        propertyTypes: [
            {
                name: "Sales",
                baseUrl: "https://www.marshandparsons.co.uk/properties-for-sale/london/?filters=exclude_sold%2Cexclude_under_offer",
                isRent: false,
                totalPages: 30,
            },
        ],
    },
    {
        id: 8,
        name: "Jackie Quinn",
        propertyTypes: [
            {
                name: "Sales",
                baseUrl: "https://www.jackiequinn.co.uk/search?category=1&listingtype=5&statusids=1%2C10%2C4%2C16%2C3&obc=Price&obd=Descending",
                isRent: false,
                totalPages: 11,
            },
        ],
    },
    {
        id: 12,
        name: "Purplebricks",
        propertyTypes: [
            {
                name: "London Rents",
                baseUrl: "https://www.purplebricks.co.uk/search/property-to-rent/greater-london/london?sortBy=2&betasearch=true&latitude=51.5072178&longitude=-0.1275862&location=london&searchRadius=2&searchType=ForRent&soldOrLet=false",
                isRent: true,
                totalPages: 2,
            },
        ],
    },
    {
        id: 13,
        name: "Bairstow Eves",
        propertyTypes: [
            {
                name: "Lettings",
                baseUrl: "https://www.bairstoweves.co.uk/properties/lettings/status-available/most-recent-first",
                isRent: true,
                totalPages: 13, // 634 records / 50 per page
            },
        ],
    },
    {
        id: 14,
        name: "Chestertons",
        propertyTypes: [
            {
                name: "Lettings",
                baseUrl: "https://www.chestertons.co.uk/properties/lettings/status-available",
                isRent: true,
                totalPages: 95, // 1132 records / 12 per page
            },
        ],
    },
    {
        id: 15,
        name: "Sequence Home",
        propertyTypes: [
            {
                name: "Rentals",
                baseUrl: "https://www.sequencehome.co.uk/properties/lettings",
                isRent: true,
                totalPages: 191, // 1907 items / 10 per page
            },
        ],
    },
];
// Memory monitoring
function logMemoryUsage(label) {
    const used = process.memoryUsage();
    console.log(
        `[${label}] Memory: ${Math.round(used.heapUsed / 1024 / 1024)}MB / ${Math.round(
            used.heapTotal / 1024 / 1024
        )}MB`
    );
}

// Check if property is sold based on text content
function isSoldProperty(text) {
    const lowerText = text.toLowerCase();
    return SOLD_KEYWORDS.some(keyword => lowerText.includes(keyword));
}

// Optimized update function - only updates price for existing properties
async function updatePriceByPropertyURLOptimized(
    link,
    price,
    title,
    bedrooms,
    agent_id,
    is_rent = false,
    latitude = null,
    longitude = null
) {
    try {
        if (link) {
            let tableName = "property_for_sale";
            if (is_rent) {
                tableName = "property_for_rent";
            }

            const linkTrimmed = link.trim();

            // Check if property exists for THIS agent
            const [propertiesUrlRows] = await promisePool.query(
                `SELECT COUNT(*) as count FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
                [linkTrimmed, agent_id]
            );

            if (propertiesUrlRows[0].count > 0) {
                // UPDATE existing property - only price, no coordinates needed
                const [result] = await promisePool.query(
                    `UPDATE ${tableName}
                    SET price = ?, updated_at = NOW()
                    WHERE property_url = ? AND agent_id = ?`,
                    [price, linkTrimmed, agent_id]
                );

                if (result.affectedRows > 0) {
                    console.log(`✅ Updated price: ${linkTrimmed.substring(0, 50)}... | Price: £${price}`);
                }
                return { isExisting: true, updated: result.affectedRows > 0 };
            } else {
                // For new properties, we'll need coordinates - return false to indicate detail page needed
                return { isExisting: false, updated: false };
            }
        }
    } catch (error) {
        console.error(`❌ Error checking property: ${error.message}`);
        throw error;
    }
}

// Extract coordinates from various patterns
function extractCoordinates(html) {
    let latitude = null;
    let longitude = null;

    // Pattern 1: Google Maps URL (@lat,lng)
    const mapsMatch = html.match(/ll=([\d.-]+),([\d.-]+)/);
    // Pattern 2: JavaScript lat/lng
    const scriptMatch = html.match(/lat:\s*([\d.-]+),\s*lng:\s*([\d.-]+)/);
    // Pattern 3: JSON latitude/longitude
    const jsonMatch = html.match(/"latitude":\s*([\d.-]+),\s*"longitude":\s*([\d.-]+)/);
    // Pattern 4: @lat,lng format
    const atMatch = html.match(/@([0-9.-]+),([0-9.-]+),\d+z/);
    // Pattern 5: HTML comments (Bairstow Eves, Sequence Home)
    const latCommentMatch = html.match(/<!--property-latitude:"([0-9.-]+)"-->/);
    const lngCommentMatch = html.match(/<!--property-longitude:"([0-9.-]+)"-->/);

    if (mapsMatch) {
        latitude = parseFloat(mapsMatch[1]);
        longitude = parseFloat(mapsMatch[2]);
    } else if (scriptMatch) {
        latitude = parseFloat(scriptMatch[1]);
        longitude = parseFloat(scriptMatch[2]);
    } else if (jsonMatch) {
        latitude = parseFloat(jsonMatch[1]);
        longitude = parseFloat(jsonMatch[2]);
    } else if (atMatch) {
        latitude = parseFloat(atMatch[1]);
        longitude = parseFloat(atMatch[2]);
    } else if (latCommentMatch && lngCommentMatch) {
        latitude = parseFloat(latCommentMatch[1]);
        longitude = parseFloat(lngCommentMatch[1]);
    }

    return { latitude, longitude };
}

// Process property - optimized to skip detail page for existing properties
async function processProperty(hero, property, agentId) {
    const { url, title, location, priceRaw, bedrooms, isRent } = property;

    let priceClean = priceRaw.replace(/[£,]/g, "");
    if (isRent && priceClean.includes("p/w")) {
        priceClean = priceClean.replace("p/w", "").trim();
    }
    const price = parseFloat(priceClean);

    try {
        const fullTitle = location ? `${title}, ${location}` : title;

        // First check if property exists - if yes, just update price
        const result = await updatePriceByPropertyURLOptimized(
            url,
            price,
            fullTitle,
            bedrooms,
            agentId,
            isRent
        );

        if (result.isExisting) {
            // Property exists, price updated, no need to fetch coordinates
            return;
        }

        // New property - fetch coordinates from detail page
        console.log(`🆕 New property, fetching coordinates: ${url}`);
        await hero.goto(url);
        await hero.waitForMillis(2000);

        const html = await hero.document.body.innerHTML;
        const coords = extractCoordinates(html);

        // Insert new property with coordinates
        await updatePriceByPropertyURL(
            url,
            price,
            fullTitle,
            bedrooms,
            agentId,
            isRent,
            coords.latitude,
            coords.longitude
        );

        console.log(`✓ ${fullTitle} (£${price}) - Coords: ${coords.latitude}, ${coords.longitude}`);
    } catch (error) {
        console.error(`✗ Failed ${url}: ${error.message}`);
    }
}
// Scraper functions for each agent with sold property filtering
async function scrapeMarshParsons(hero, listingUrl, isRent) {
    console.log(`\n📋 Scraping Marsh & Parsons: ${listingUrl}`);

    try {
        await hero.goto(listingUrl);
        await hero.waitForMillis(2000);

        const properties = await hero.document.querySelectorAll("div.my-4.shadow-md.rounded-xl");
        const propertyList = [];

        for (const card of properties) {
            try {
                const linkElement = await card.querySelector('a[href*="/property/"]');
                const titleElement = await card.querySelector("h3");
                const locationElement = await card.querySelector("p");

                const textContent = await card.textContent;

                // Check for sold keywords before processing
                if (isSoldProperty(textContent)) {
                    console.log(`⏭️ Skipping sold property: ${textContent.substring(0, 50)}...`);
                    continue;
                }

                const priceMatch = textContent.match(/£[0-9,]+(p\/w)?/);
                const priceRaw = priceMatch ? priceMatch[0] : null;

                const bedImg = await card.querySelector('img[alt="bed"]');
                let bedrooms = null;
                if (bedImg) {
                    const parent = await bedImg.parentElement;
                    const bedroomText = await parent.textContent;
                    const bedroomMatch = bedroomText.trim().match(/\d+/);
                    bedrooms = bedroomMatch ? parseInt(bedroomMatch[0]) : null;
                }

                const url = linkElement ? await linkElement.getAttribute("href") : null;
                const title = titleElement ? await titleElement.textContent : "";
                const location = locationElement ? await locationElement.textContent : "";

                if (url && priceRaw) {
                    propertyList.push({
                        url: url.startsWith("http") ? url : `https://www.marshandparsons.co.uk${url}`,
                        title: title.trim(),
                        location: location.trim(),
                        priceRaw,
                        bedrooms,
                    });
                }
            } catch (err) {
                console.error(`Error extracting Marsh & Parsons property: ${err.message}`);
            }
        }

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(hero, { ...property, isRent }, 4);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Marsh & Parsons: ${error.message}`);
        return 0;
    }
}

async function scrapeJackieQuinn(hero, listingUrl, isRent) {
    console.log(`\n📋 Scraping Jackie Quinn: ${listingUrl}`);

    try {
        await hero.goto(listingUrl);
        await hero.waitForMillis(2000);

        const propertyBoxes = await hero.document.querySelectorAll('.propertyBox');
        const propertyList = [];

        for (const listing of propertyBoxes) {
            try {
                const linkEl = await listing.querySelector('h2.searchProName a');
                const link = linkEl ? await linkEl.getAttribute('href') : null;

                const titleEl = await listing.querySelector('h2.searchProName a');
                const title = titleEl ? await titleEl.textContent : null;

                const priceEl = await listing.querySelector('h3 div');
                const priceText = priceEl ? await priceEl.textContent : '';

                // Check for sold keywords
                if (isSoldProperty(priceText) || priceText.includes('Sold Subject To Contract')) {
                    console.log(`⏭️ Skipping sold property: ${title}`);
                    continue;
                }

                const priceMatch = priceText.match(/£([\d,]+)/);
                const priceRaw = priceMatch ? priceMatch[0] : null;

                const descEl = await listing.querySelector('.featuredDescriptions');
                const description = descEl ? await descEl.textContent : '';
                const bedroomMatch = description.match(/(\d+)\s+BEDROOM/i);
                const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

                if (link && title && priceRaw) {
                    propertyList.push({
                        url: link.startsWith('http') ? link : 'https://www.jackiequinn.co.uk' + link,
                        title: title.trim(),
                        location: '',
                        priceRaw,
                        bedrooms
                    });
                }
            } catch (err) {
                console.error(`Error extracting Jackie Quinn property: ${err.message}`);
            }
        }

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(hero, { ...property, isRent }, 8);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Jackie Quinn: ${error.message}`);
        return 0;
    }
}
async function scrapePurplebricks(hero, listingUrl, isRent) {
    console.log(`\n📋 Scraping Purplebricks: ${listingUrl}`);

    try {
        await hero.goto(listingUrl);
        await hero.waitForMillis(3000);

        const resultsList = await hero.document.querySelector('[data-testid="results-list"]');
        if (!resultsList) {
            console.log('No results list found');
            return 0;
        }

        const listItems = await resultsList.querySelectorAll('li');
        const propertyList = [];

        for (const li of listItems) {
            try {
                const linkEl = await li.querySelector('a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]');
                if (!linkEl) continue;

                const priceEl = await li.querySelector('[data-testid="search-result-price"]') ||
                    await li.querySelector('.sc-cda42038-7');
                const priceText = priceEl ? await priceEl.textContent : '';

                // Check for sold keywords
                if (isSoldProperty(priceText)) {
                    console.log(`⏭️ Skipping sold property: ${priceText}`);
                    continue;
                }

                const priceMatch = priceText.match(/£([\d,]+)/);
                const priceRaw = priceMatch ? priceMatch[0] : '';

                const addrEl = await li.querySelector('[data-testid="search-result-address"]') ||
                    await li.querySelector('.sc-cda42038-10');
                const address = addrEl ? await addrEl.textContent : '';

                const bedEl = await li.querySelector('[data-testid="search-result-bedrooms"]');
                const bedrooms = bedEl ? await bedEl.textContent : '';

                const href = await linkEl.getAttribute('href');
                const url = href && href.startsWith('http') ? href :
                    href ? `https://www.purplebricks.co.uk${href}` : null;

                if (url && priceRaw) {
                    propertyList.push({
                        url,
                        title: address.trim(),
                        location: '',
                        priceRaw,
                        bedrooms: bedrooms.trim()
                    });
                }
            } catch (err) {
                console.error(`Error extracting Purplebricks property: ${err.message}`);
            }
        }

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(hero, { ...property, isRent }, 12);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Purplebricks: ${error.message}`);
        return 0;
    }
}

async function scrapeBairstowEves(hero, listingUrl, isRent) {
    console.log(`\n📋 Scraping Bairstow Eves: ${listingUrl}`);

    try {
        await hero.goto(listingUrl);
        await hero.waitForMillis(2000);

        const cards = await hero.document.querySelectorAll('.card');
        const propertyList = [];

        for (const card of cards) {
            try {
                const linkEl = await card.querySelector('a.card__link');
                const link = linkEl ? await linkEl.getAttribute('href') : null;

                const titleEl = await card.querySelector('.card__text-content');
                const title = titleEl ? await titleEl.textContent : null;

                const priceEl = await card.querySelector('.card__heading');
                let priceRaw = null;
                if (priceEl) {
                    const priceText = await priceEl.textContent;

                    // Check for sold keywords
                    if (isSoldProperty(priceText)) {
                        console.log(`⏭️ Skipping sold property: ${title}`);
                        continue;
                    }

                    const priceMatch = priceText.match(/£[\d,]+/);
                    priceRaw = priceMatch ? priceMatch[0] : null;
                }

                const bedroomsEl = await card.querySelector('.card-content__spec-list-number');
                let bedrooms = null;
                if (bedroomsEl) {
                    const bedroomsText = await bedroomsEl.textContent;
                    const bedroomsMatch = bedroomsText.match(/\d+/);
                    bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
                }

                if (link && priceRaw && title) {
                    propertyList.push({
                        url: link.startsWith('http') ? link : `https://www.bairstoweves.co.uk${link}`,
                        title: title.trim(),
                        location: '',
                        priceRaw,
                        bedrooms
                    });
                }
            } catch (err) {
                console.error(`Error extracting Bairstow Eves property: ${err.message}`);
            }
        }

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(hero, { ...property, isRent }, 13);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Bairstow Eves: ${error.message}`);
        return 0;
    }
}
async function scrapeChestertons(hero, listingUrl, isRent) {
    console.log(`\n📋 Scraping Chestertons: ${listingUrl}`);

    try {
        await hero.goto(listingUrl);
        await hero.waitForMillis(2000);

        const cards = await hero.document.querySelectorAll('.pegasus-property-card');
        const propertyList = [];

        for (const card of cards) {
            try {
                const linkEl = await card.querySelector("a[href*='/properties/']");
                if (!linkEl) continue;

                let href = await linkEl.getAttribute('href');
                if (!href.startsWith('http')) {
                    href = 'https://www.chestertons.co.uk' + href;
                }

                let priceRaw = null;
                const spans = await card.querySelectorAll('span');
                for (const span of spans) {
                    const spanText = await span.textContent;

                    // Check for sold keywords
                    if (isSoldProperty(spanText)) {
                        console.log(`⏭️ Skipping sold property: ${spanText}`);
                        continue;
                    }

                    const priceMatch = spanText.match(/£([\d,]+)/);
                    if (priceMatch) {
                        priceRaw = priceMatch[0];
                        break;
                    }
                }

                // Skip if no valid price found (likely sold)
                if (!priceRaw) continue;

                const title = await linkEl.getAttribute('title') || await linkEl.textContent;

                let bedrooms = null;
                const bedroomElements = await card.querySelectorAll('svg[aria-labelledby]');
                for (const svg of bedroomElements) {
                    const titleEl = await svg.querySelector('title');
                    if (titleEl) {
                        const titleText = await titleEl.textContent;
                        if (titleText === 'Bedrooms') {
                            const parent = await svg.parentElement;
                            const nextSibling = await parent.nextElementSibling;
                            if (nextSibling) {
                                bedrooms = await nextSibling.textContent;
                            }
                            break;
                        }
                    }
                }

                if (href && priceRaw && title) {
                    propertyList.push({
                        url: href,
                        title: title.trim(),
                        location: '',
                        priceRaw,
                        bedrooms: bedrooms ? bedrooms.trim() : null
                    });
                }
            } catch (err) {
                console.error(`Error extracting Chestertons property: ${err.message}`);
            }
        }

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(hero, { ...property, isRent }, 14);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Chestertons: ${error.message}`);
        return 0;
    }
}

async function scrapeSequenceHome(hero, listingUrl, isRent, pageNum) {
    console.log(`\n📋 Scraping Sequence Home: ${listingUrl}`);

    try {
        await hero.goto(listingUrl);
        await hero.waitForMillis(2000);

        // Look for the specific page container
        const containerSelector = `div[data-page-no="${pageNum}"]`;
        let container = await hero.document.querySelector(containerSelector);

        // Fallback to any property container if specific page not found
        if (!container) {
            const anyProperty = await hero.document.querySelector('.property.list_block[data-property-id]');
            container = anyProperty ? anyProperty.parentElement : null;
        }

        if (!container) {
            console.log('No property container found');
            return 0;
        }

        const items = await container.querySelectorAll('.property.list_block[data-property-id]');
        const propertyList = [];

        for (const item of items) {
            try {
                const linkEl = await item.querySelector('a.property-list-link');
                const href = linkEl ? await linkEl.getAttribute('href') : null;
                const url = href ?
                    (href.startsWith('http') ? href : `https://www.sequencehome.co.uk${href}`) : null;

                const titleEl = await item.querySelector('.address');
                const title = titleEl ? await titleEl.textContent : '';

                const priceEl = await item.querySelector('.price-value');
                const priceText = priceEl ? await priceEl.textContent : '';

                // Check for sold keywords
                if (isSoldProperty(priceText)) {
                    console.log(`⏭️ Skipping sold property: ${title}`);
                    continue;
                }

                let bedrooms = null;
                const roomsEl = await item.querySelector('.rooms');
                if (roomsEl) {
                    bedrooms = await roomsEl.textContent;
                    if (!bedrooms) {
                        const titleAttr = await roomsEl.getAttribute('title');
                        if (titleAttr) {
                            const match = titleAttr.match(/(\d+)/);
                            bedrooms = match ? match[1] : null;
                        }
                    }
                }

                if (url && priceText && title) {
                    propertyList.push({
                        url,
                        title: title.trim(),
                        location: '',
                        priceRaw: priceText.trim(),
                        bedrooms: bedrooms ? bedrooms.trim() : null
                    });
                }
            } catch (err) {
                console.error(`Error extracting Sequence Home property: ${err.message}`);
            }
        }

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(hero, { ...property, isRent }, 15);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Sequence Home: ${error.message}`);
        return 0;
    }
}
// Main scraping function
async function runOptimizedCombinedScraper() {
    console.log(`Starting Optimized Combined Hero Scraper for agents: ${AGENTS.map(a => a.id).join(', ')}...`);
    logMemoryUsage("START");

    const hero = new Hero({
        showChrome: false,
        blockedResourceTypes: ["image", "media", "font", "stylesheet"],
        userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    });

    let totalProcessed = 0;
    let totalSkipped = 0;
    let totalUpdated = 0;
    let totalNew = 0;

    try {
        for (const agent of AGENTS) {
            console.log(`\n🏢 Processing ${agent.name} (Agent ${agent.id})...`);

            for (const type of agent.propertyTypes) {
                console.log(`\n📦 Processing ${type.name}...`);

                for (let pageNum = 1; pageNum <= type.totalPages; pageNum++) {
                    console.log(`\n📄 Page ${pageNum}/${type.totalPages}`);

                    let listingUrl;
                    let processed = 0;

                    // Build URL based on agent
                    switch (agent.id) {
                        case 4: // Marsh & Parsons
                            listingUrl = `${type.baseUrl}&page=${pageNum}`;
                            processed = await scrapeMarshParsons(hero, listingUrl, type.isRent);
                            break;

                        case 8: // Jackie Quinn
                            listingUrl = `${type.baseUrl}&page=${pageNum}`;
                            processed = await scrapeJackieQuinn(hero, listingUrl, type.isRent);
                            break;

                        case 12: // Purplebricks
                            listingUrl = type.baseUrl.replace(/page=\d+/, `page=${pageNum}`);
                            processed = await scrapePurplebricks(hero, listingUrl, type.isRent);
                            break;

                        case 13: // Bairstow Eves
                            listingUrl = `${type.baseUrl}/page-${pageNum}#/`;
                            processed = await scrapeBairstowEves(hero, listingUrl, type.isRent);
                            break;

                        case 14: // Chestertons
                            listingUrl = pageNum === 1 ? type.baseUrl : `${type.baseUrl}?page=${pageNum}`;
                            processed = await scrapeChestertons(hero, listingUrl, type.isRent);
                            break;

                        case 15: // Sequence Home
                            listingUrl = pageNum === 1 ? `${type.baseUrl}/` : `${type.baseUrl}/page-${pageNum}/`;
                            processed = await scrapeSequenceHome(hero, listingUrl, type.isRent, pageNum);
                            break;
                    }

                    totalProcessed += processed;
                    logMemoryUsage(`After ${agent.name} page ${pageNum}`);

                    // Small delay between pages
                    await new Promise((resolve) => setTimeout(resolve, 1000));
                }
            }

            // Update remove status for this agent
            await updateRemoveStatus(agent.id);
            console.log(`✅ Completed ${agent.name}`);
        }

        console.log(`\n✅ All scrapers completed.`);
        console.log(`📊 Summary: ${totalProcessed} properties processed`);
        logMemoryUsage("END");
    } catch (error) {
        console.error("❌ Fatal error:", error);
        throw error;
    } finally {
        await hero.close();
    }
}

// Run the optimized combined scraper
runOptimizedCombinedScraper()
    .then(() => {
        console.log("✅ All done!");
        process.exit(0);
    })
    .catch((err) => {
        console.error("❌ Scraper error:", err);
        process.exit(1);
    });