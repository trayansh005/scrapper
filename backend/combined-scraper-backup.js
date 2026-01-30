const { CheerioCrawler, PlaywrightCrawler } = require('crawlee');
const { chromium } = require('playwright');
const cheerio = require('cheerio');
const axios = require('axios');
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
async function extractCoordinates(page) {
    let latitude = null;
    let longitude = null;

    try {
        const html = await page.content();

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
    } catch (error) {
        console.error('Error extracting coordinates:', error.message);
    }

    return { latitude, longitude };
}

// Process property - optimized to skip detail page for existing properties
async function processProperty(browser, property, agentId) {
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

        // New property - fetch coordinates from detail page using Playwright
        console.log(`🆕 New property, fetching coordinates: ${url}`);
        const page = await browser.newPage();

        // Block unnecessary resources for this page
        await page.route('**/*', (route) => {
            const resourceType = route.request().resourceType();
            if (['image', 'font', 'stylesheet', 'media'].includes(resourceType)) {
                route.abort();
            } else {
                route.continue();
            }
        });

        try {
            await page.goto(url, { waitUntil: 'networkidle' });
            await page.waitForTimeout(2000);

            const coords = await extractCoordinates(page);

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
        } finally {
            await page.close();
        }
    } catch (error) {
        console.error(`✗ Failed ${url}: ${error.message}`);
    }
}
// Scraper functions for each agent with sold property filtering using Cheerio
async function scrapeMarshParsons(browser, listingUrl, isRent) {
    console.log(`\n📋 Scraping Marsh & Parsons: ${listingUrl}`);

    try {
        // Add delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 2000));

        const response = await axios.get(listingUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            },
            timeout: 30000
        });

        const $ = cheerio.load(response.data);
        const propertyList = [];

        $("div.my-4.shadow-md.rounded-xl").each((index, element) => {
            try {
                const $card = $(element);
                const linkElement = $card.find('a[href*="/property/"]').first();
                const titleElement = $card.find("h3").first();
                const locationElement = $card.find("p").first();

                const textContent = $card.text();

                // Check for sold keywords before processing
                if (isSoldProperty(textContent)) {
                    console.log(`⏭️ Skipping sold property: ${textContent.substring(0, 50)}...`);
                    return;
                }

                const priceMatch = textContent.match(/£[0-9,]+(p\/w)?/);
                const priceRaw = priceMatch ? priceMatch[0] : null;

                const bedImg = $card.find('img[alt="bed"]').first();
                let bedrooms = null;
                if (bedImg.length) {
                    const parent = bedImg.parent();
                    const bedroomText = parent.text();
                    const bedroomMatch = bedroomText.trim().match(/\d+/);
                    bedrooms = bedroomMatch ? parseInt(bedroomMatch[0]) : null;
                }

                const url = linkElement.attr("href");
                const title = titleElement.text() || "";
                const location = locationElement.text() || "";

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
        });

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(browser, { ...property, isRent }, 4);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Marsh & Parsons with Playwright: ${error.message}`);
        return 0;
    }
}

async function scrapeJackieQuinn(browser, listingUrl, isRent) {
    console.log(`\n📋 Scraping Jackie Quinn: ${listingUrl}`);

    try {
        const response = await axios.get(listingUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        });

        const $ = cheerio.load(response.data);
        const propertyList = [];

        $('.propertyBox').each((index, element) => {
            try {
                const $listing = $(element);
                const linkEl = $listing.find('h2.searchProName a').first();
                const link = linkEl.attr('href');

                const titleEl = $listing.find('h2.searchProName a').first();
                const title = titleEl.text();

                const priceEl = $listing.find('h3 div').first();
                const priceText = priceEl.text();

                // Check for sold keywords
                if (isSoldProperty(priceText) || priceText.includes('Sold Subject To Contract')) {
                    console.log(`⏭️ Skipping sold property: ${title}`);
                    return;
                }

                const priceMatch = priceText.match(/£([\d,]+)/);
                const priceRaw = priceMatch ? priceMatch[0] : null;

                const descEl = $listing.find('.featuredDescriptions').first();
                const description = descEl.text();
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
        });

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(browser, { ...property, isRent }, 8);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Jackie Quinn: ${error.message}`);
        return 0;
    }
}

async function scrapePurplebricks(browser, listingUrl, isRent) {
} catch (error) {
    console.error(`Error scraping Marsh & Parsons: ${error.message}`);
    return 0;
}
}

// Alternative Playwright-based scraper for Marsh & Parsons (use if axios fails)
async function scrapeMarshParsonsPlaywright(browser, listingUrl, isRent) {
    console.log(`\n📋 Scraping Marsh & Parsons with Playwright: ${listingUrl}`);

    try {
        const page = await browser.newPage();

        // Set realistic viewport and user agent
        await page.setViewportSize({ width: 1920, height: 1080 });
        await page.setExtraHTTPHeaders({
            'Accept-Language': 'en-US,en;q=0.9'
        });

        // Block unnecessary resources
        await page.route('**/*', (route) => {
            const resourceType = route.request().resourceType();
            if (['image', 'font', 'stylesheet', 'media'].includes(resourceType)) {
                route.abort();
            } else {
                route.continue();
            }
        });

        try {
            await page.goto(listingUrl, {
                waitUntil: 'networkidle',
                timeout: 30000
            });
            await page.waitForTimeout(3000);

            const content = await page.content();
            const $ = cheerio.load(content);
            const propertyList = [];

            $("div.my-4.shadow-md.rounded-xl").each((index, element) => {
                try {
                    const $card = $(element);
                    const linkElement = $card.find('a[href*="/property/"]').first();
                    const titleElement = $card.find("h3").first();
                    const locationElement = $card.find("p").first();

                    const textContent = $card.text();

                    // Check for sold keywords before processing
                    if (isSoldProperty(textContent)) {
                        console.log(`⏭️ Skipping sold property: ${textContent.substring(0, 50)}...`);
                        return;
                    }

                    const priceMatch = textContent.match(/£[0-9,]+(p\/w)?/);
                    const priceRaw = priceMatch ? priceMatch[0] : null;

                    const bedImg = $card.find('img[alt="bed"]').first();
                    let bedrooms = null;
                    if (bedImg.length) {
                        const parent = bedImg.parent();
                        const bedroomText = parent.text();
                        const bedroomMatch = bedroomText.trim().match(/\d+/);
                        bedrooms = bedroomMatch ? parseInt(bedroomMatch[0]) : null;
                    }

                    const url = linkElement.attr("href");
                    const title = titleElement.text() || "";
                    const location = locationElement.text() || "";

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
            });

            console.log(`Found ${propertyList.length} available properties`);

            for (const property of propertyList) {
                await processProperty(browser, { ...property, isRent }, 4);
            }

            return propertyList.length;
        } finally {
            await page.close();
        }
    } catch (error) {
        console.error(`Error scraping Marsh & Parsons with Playwright: ${error.message}`);
        return 0;
    }
}
console.log(`\n📋 Scraping Jackie Quinn: ${listingUrl}`);

try {
    const response = await axios.get(listingUrl, {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
    });

    const $ = cheerio.load(response.data);
    const propertyList = [];

    $('.propertyBox').each((index, element) => {
        try {
            const $listing = $(element);
            const linkEl = $listing.find('h2.searchProName a').first();
            const link = linkEl.attr('href');

            const titleEl = $listing.find('h2.searchProName a').first();
            const title = titleEl.text();

            const priceEl = $listing.find('h3 div').first();
            const priceText = priceEl.text();

            // Check for sold keywords
            if (isSoldProperty(priceText) || priceText.includes('Sold Subject To Contract')) {
                console.log(`⏭️ Skipping sold property: ${title}`);
                return;
            }

            const priceMatch = priceText.match(/£([\d,]+)/);
            const priceRaw = priceMatch ? priceMatch[0] : null;

            const descEl = $listing.find('.featuredDescriptions').first();
            const description = descEl.text();
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
    });

    console.log(`Found ${propertyList.length} available properties`);

    for (const property of propertyList) {
        await processProperty(browser, { ...property, isRent }, 8);
    }

    return propertyList.length;
} catch (error) {
    console.error(`Error scraping Jackie Quinn: ${error.message}`);
    return 0;
}
}
async function scrapePurplebricks(browser, listingUrl, isRent) {
    console.log(`\n📋 Scraping Purplebricks: ${listingUrl}`);

    try {
        const page = await browser.newPage();

        // Block unnecessary resources for this page
        await page.route('**/*', (route) => {
            const resourceType = route.request().resourceType();
            if (['image', 'font', 'stylesheet', 'media'].includes(resourceType)) {
                route.abort();
            } else {
                route.continue();
            }
        });

        try {
            await page.goto(listingUrl, { waitUntil: 'networkidle' });
            await page.waitForTimeout(3000);

            const resultsList = await page.locator('[data-testid="results-list"]').first();
            if (!(await resultsList.isVisible())) {
                console.log('No results list found');
                return 0;
            }

            const listItems = await resultsList.locator('li').all();
            const propertyList = [];

            for (const li of listItems) {
                try {
                    const linkEl = li.locator('a[href*="/property-for-sale/"], a[href*="/property-to-rent/"]').first();
                    if (!(await linkEl.isVisible())) continue;

                    const priceEl = li.locator('[data-testid="search-result-price"], .sc-cda42038-7').first();
                    const priceText = await priceEl.isVisible() ? await priceEl.textContent() : '';

                    // Check for sold keywords
                    if (isSoldProperty(priceText)) {
                        console.log(`⏭️ Skipping sold property: ${priceText}`);
                        continue;
                    }

                    const priceMatch = priceText.match(/£([\d,]+)/);
                    const priceRaw = priceMatch ? priceMatch[0] : '';

                    const addrEl = li.locator('[data-testid="search-result-address"], .sc-cda42038-10').first();
                    const address = await addrEl.isVisible() ? await addrEl.textContent() : '';

                    const bedEl = li.locator('[data-testid="search-result-bedrooms"]').first();
                    const bedrooms = await bedEl.isVisible() ? await bedEl.textContent() : '';

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
                await processProperty(browser, { ...property, isRent }, 12);
            }

            return propertyList.length;
        } finally {
            await page.close();
        }
    } catch (error) {
        console.error(`Error scraping Purplebricks: ${error.message}`);
        return 0;
    }
}

async function scrapeBairstowEves(browser, listingUrl, isRent) {
    console.log(`\n📋 Scraping Bairstow Eves: ${listingUrl}`);

    try {
        const response = await axios.get(listingUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        });

        const $ = cheerio.load(response.data);
        const propertyList = [];

        $('.card').each((index, element) => {
            try {
                const $card = $(element);
                const linkEl = $card.find('a.card__link').first();
                const link = linkEl.attr('href');

                const titleEl = $card.find('.card__text-content').first();
                const title = titleEl.text();

                const priceEl = $card.find('.card__heading').first();
                let priceRaw = null;
                if (priceEl.length) {
                    const priceText = priceEl.text();

                    // Check for sold keywords
                    if (isSoldProperty(priceText)) {
                        console.log(`⏭️ Skipping sold property: ${title}`);
                        return;
                    }

                    const priceMatch = priceText.match(/£[\d,]+/);
                    priceRaw = priceMatch ? priceMatch[0] : null;
                }

                const bedroomsEl = $card.find('.card-content__spec-list-number').first();
                let bedrooms = null;
                if (bedroomsEl.length) {
                    const bedroomsText = bedroomsEl.text();
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
        });

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(browser, { ...property, isRent }, 13);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Bairstow Eves: ${error.message}`);
        return 0;
    }
}
async function scrapeChestertons(browser, listingUrl, isRent) {
    console.log(`\n📋 Scraping Chestertons: ${listingUrl}`);

    try {
        const response = await axios.get(listingUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        });

        const $ = cheerio.load(response.data);
        const propertyList = [];

        $('.pegasus-property-card').each((index, element) => {
            try {
                const $card = $(element);
                const linkEl = $card.find("a[href*='/properties/']").first();
                if (!linkEl.length) return;

                let href = linkEl.attr('href');
                if (!href.startsWith('http')) {
                    href = 'https://www.chestertons.co.uk' + href;
                }

                let priceRaw = null;
                $card.find('span').each((i, span) => {
                    const spanText = $(span).text();

                    // Check for sold keywords
                    if (isSoldProperty(spanText)) {
                        console.log(`⏭️ Skipping sold property: ${spanText}`);
                        return false; // Skip this property
                    }

                    const priceMatch = spanText.match(/£([\d,]+)/);
                    if (priceMatch) {
                        priceRaw = priceMatch[0];
                        return false; // Break the loop
                    }
                });

                // Skip if no valid price found (likely sold)
                if (!priceRaw) return;

                const title = linkEl.attr('title') || linkEl.text();

                let bedrooms = null;
                $card.find('svg[aria-labelledby]').each((i, svg) => {
                    const titleEl = $(svg).find('title').first();
                    if (titleEl.length && titleEl.text() === 'Bedrooms') {
                        const parent = $(svg).parent();
                        const nextSibling = parent.next();
                        if (nextSibling.length) {
                            bedrooms = nextSibling.text();
                        }
                        return false; // Break the loop
                    }
                });

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
        });

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(browser, { ...property, isRent }, 14);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Chestertons: ${error.message}`);
        return 0;
    }
}

async function scrapeSequenceHome(browser, listingUrl, isRent, pageNum) {
    console.log(`\n📋 Scraping Sequence Home: ${listingUrl}`);

    try {
        const response = await axios.get(listingUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        });

        const $ = cheerio.load(response.data);
        const propertyList = [];

        // Look for the specific page container
        const containerSelector = `div[data-page-no="${pageNum}"]`;
        let container = $(containerSelector);

        // Fallback to any property container if specific page not found
        if (!container.length) {
            const anyProperty = $('.property.list_block[data-property-id]').first();
            container = anyProperty.length ? anyProperty.parent() : $();
        }

        if (!container.length) {
            console.log('No property container found');
            return 0;
        }

        container.find('.property.list_block[data-property-id]').each((index, element) => {
            try {
                const $item = $(element);
                const linkEl = $item.find('a.property-list-link').first();
                const href = linkEl.attr('href');
                const url = href ?
                    (href.startsWith('http') ? href : `https://www.sequencehome.co.uk${href}`) : null;

                const titleEl = $item.find('.address').first();
                const title = titleEl.text();

                const priceEl = $item.find('.price-value').first();
                const priceText = priceEl.text();

                // Check for sold keywords
                if (isSoldProperty(priceText)) {
                    console.log(`⏭️ Skipping sold property: ${title}`);
                    return;
                }

                let bedrooms = null;
                const roomsEl = $item.find('.rooms').first();
                if (roomsEl.length) {
                    bedrooms = roomsEl.text();
                    if (!bedrooms) {
                        const titleAttr = roomsEl.attr('title');
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
        });

        console.log(`Found ${propertyList.length} available properties`);

        for (const property of propertyList) {
            await processProperty(browser, { ...property, isRent }, 15);
        }

        return propertyList.length;
    } catch (error) {
        console.error(`Error scraping Sequence Home: ${error.message}`);
        return 0;
    }
}
// Main scraping function
async function runOptimizedCombinedScraper() {
    console.log(`Starting Optimized Combined Crawlee Scraper for agents: ${AGENTS.map(a => a.id).join(', ')}...`);
    logMemoryUsage("START");

    const browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    });

    // Set up resource blocking for all pages
    browser.on('page', (page) => {
        page.route('**/*', (route) => {
            const resourceType = route.request().resourceType();
            // Block images, fonts, stylesheets, and media to improve performance
            if (['image', 'font', 'stylesheet', 'media'].includes(resourceType)) {
                route.abort();
            } else {
                route.continue();
            }
        });
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
                        case 4: // Marsh & Parsons - Use Playwright due to 403 errors
                            listingUrl = `${type.baseUrl}&page=${pageNum}`;
                            processed = await scrapeMarshParsonsPlaywright(browser, listingUrl, type.isRent);
                            break;

                        case 8: // Jackie Quinn
                            listingUrl = `${type.baseUrl}&page=${pageNum}`;
                            processed = await scrapeJackieQuinn(browser, listingUrl, type.isRent);
                            break;

                        case 12: // Purplebricks
                            listingUrl = type.baseUrl.replace(/page=\d+/, `page=${pageNum}`);
                            processed = await scrapePurplebricks(browser, listingUrl, type.isRent);
                            break;

                        case 13: // Bairstow Eves
                            listingUrl = `${type.baseUrl}/page-${pageNum}#/`;
                            processed = await scrapeBairstowEves(browser, listingUrl, type.isRent);
                            break;

                        case 14: // Chestertons
                            listingUrl = pageNum === 1 ? type.baseUrl : `${type.baseUrl}?page=${pageNum}`;
                            processed = await scrapeChestertons(browser, listingUrl, type.isRent);
                            break;

                        case 15: // Sequence Home
                            listingUrl = pageNum === 1 ? `${type.baseUrl}/` : `${type.baseUrl}/page-${pageNum}/`;
                            processed = await scrapeSequenceHome(browser, listingUrl, type.isRent, pageNum);
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
        await browser.close();
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