const axios = require("axios");
const cheerio = require("cheerio");

// Debug function to examine BridgFords HTML structure
async function debugAgencyUKStructure() {
    console.log("🔍 Debugging BridgFords website structure...\n");

    try {
        // Test both sales and lettings listing pages
        const testUrls = [
            {
                type: "Sales",
                url: "https://www.bridgfords.co.uk/properties/sales/status-available/most-recent-first/page-1#/"
            },
            {
                type: "Lettings",
                url: "https://www.bridgfords.co.uk/properties/lettings/status-available/most-recent-first/page-1#/"
            }
        ];

        for (const testCase of testUrls) {
            console.log(`\n📄 Examining ${testCase.type} listing page: ${testCase.url}`);

            const { data } = await axios.get(testCase.url, {
                headers: {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                },
            });

            const $ = cheerio.load(data);

            console.log(`📋 Page Title: ${$("title").text()}`);

            // Try different selectors to find property containers
            const possibleSelectors = [
                ".property-item", ".property", ".listing-item", ".property-card",
                ".search-result", ".result-item", "[data-property]", ".property-listing",
                ".listing", ".card", ".item", ".result", ".property-wrapper"
            ];

            let foundProperties = false;
            let bestSelector = null;
            let maxCount = 0;

            console.log("\n🔍 Testing property container selectors:");
            for (const selector of possibleSelectors) {
                const elements = $(selector);
                if (elements.length > 0) {
                    console.log(`✅ Found ${elements.length} elements with selector: ${selector}`);
                    if (elements.length > maxCount) {
                        maxCount = elements.length;
                        bestSelector = selector;
                        foundProperties = true;
                    }
                }
            }

            if (foundProperties && bestSelector) {
                console.log(`\n🎯 Best selector appears to be: ${bestSelector} (${maxCount} elements)\n`);

                // Examine the first property in detail
                const firstProperty = $(bestSelector).first();
                console.log("🏠 First property HTML structure:");
                console.log("=".repeat(50));
                const propertyHtml = firstProperty.html();
                console.log(propertyHtml ? propertyHtml.substring(0, 1500) + "..." : "No HTML found");
                console.log("=".repeat(50));

                // Analyze all elements in the first property
                console.log("\n📝 All text content in first property:");
                console.log("-".repeat(30));
                firstProperty.find("*").each((i, el) => {
                    const text = $(el).text().trim();
                    const tagName = el.tagName;
                    const className = $(el).attr('class') || '';
                    if (text && text.length > 0 && text.length < 200) {
                        console.log(`${tagName}.${className}: "${text}"`);
                    }
                });

                // Look for links
                console.log("\n🔗 Looking for property links:");
                firstProperty.find("a").each((i, el) => {
                    const href = $(el).attr("href");
                    const text = $(el).text().trim();
                    if (href) {
                        console.log(`Link ${i + 1}: ${href} (text: "${text}")`);
                    }
                });

                // Look for price patterns
                console.log("\n💰 Looking for price patterns:");
                firstProperty.find("*").each((i, el) => {
                    const text = $(el).text().trim();
                    if (text.includes('£') || /\d{3,}/.test(text)) {
                        console.log(`Potential price: "${text}" in ${el.tagName}.${$(el).attr('class') || ''}`);
                    }
                });

                // Look for bedroom patterns
                console.log("\n🛏️ Looking for bedroom patterns:");
                firstProperty.find("*").each((i, el) => {
                    const text = $(el).text().trim().toLowerCase();
                    if (text.includes('bed') || text.includes('room')) {
                        console.log(`Potential bedrooms: "${text}" in ${el.tagName}.${$(el).attr('class') || ''}`);
                    }
                });

                // Look for address/title patterns
                console.log("\n📍 Looking for address/title patterns:");
                firstProperty.find("h1, h2, h3, h4, .title, .address, .location").each((i, el) => {
                    const text = $(el).text().trim();
                    if (text && text.length > 5) {
                        console.log(`Title/Address: "${text}" in ${el.tagName}.${$(el).attr('class') || ''}`);
                    }
                });

            } else {
                console.log(`⚠️ No properties found with common selectors for ${testCase.type}. Let's check the page structure...`);

                // Show elements with property-related classes
                const debugSelectors = ["[class*='property']", "[class*='listing']", "[class*='result']", "[class*='card']"];
                for (const debugSelector of debugSelectors) {
                    const elements = $(debugSelector);
                    if (elements.length > 0) {
                        console.log(`🔍 Found ${elements.length} elements matching ${debugSelector}`);
                        elements.slice(0, 3).each((i, el) => {
                            console.log(`   Element ${i + 1} classes: ${$(el).attr('class')}`);
                            console.log(`   Element ${i + 1} text preview: ${$(el).text().trim().substring(0, 100)}...`);
                        });
                    }
                }
            }

            // Add delay between requests
            await new Promise(resolve => setTimeout(resolve, 2000));
        }

        console.log("\n" + "=".repeat(60));
        console.log("🏠 Now let's try to find an individual property page...");

        // Try to get a property URL from the sales page
        const salesData = await axios.get(testUrls[0].url, {
            headers: {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            },
        });

        const $sales = cheerio.load(salesData.data);
        let propertyUrl = null;

        // Try to find a property link
        $sales("a").each((i, el) => {
            const href = $sales(el).attr("href");
            if (href && (href.includes('/property/') || href.includes('/properties/'))) {
                propertyUrl = href.startsWith('http') ? href : `https://www.bridgfords.co.uk${href}`;
                return false; // break
            }
        });

        if (propertyUrl) {
            console.log(`📄 Examining individual property page: ${propertyUrl}`);

            const propertyResponse = await axios.get(propertyUrl, {
                headers: {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
                },
            });

            const $prop = cheerio.load(propertyResponse.data);

            console.log("\n📋 Property page title:", $prop("title").text());

            // Look for price on property page
            console.log("\n💰 Looking for price on property page:");
            $prop("*").each((i, el) => {
                const text = $prop(el).text().trim();
                if (text.includes('£') && /£[\d,]+/.test(text)) {
                    console.log(`Price found: "${text}" in ${el.tagName}.${$prop(el).attr('class') || ''}`);
                }
            });

            // Look for bedrooms on property page
            console.log("\n🛏️ Looking for bedrooms on property page:");
            $prop("*").each((i, el) => {
                const text = $prop(el).text().trim().toLowerCase();
                if ((text.includes('bed') || text.includes('room')) && text.length < 50) {
                    console.log(`Bedroom info: "${text}" in ${el.tagName}.${$prop(el).attr('class') || ''}`);
                }
            });

            // Look for address/title
            console.log("\n📍 Looking for address/title on property page:");
            const h1Text = $prop("h1").text().trim();
            if (h1Text) console.log(`H1: "${h1Text}"`);

            $prop("h2, h3, .address, .title, .property-title").each((i, el) => {
                const text = $prop(el).text().trim();
                if (text && text.length > 10 && text.length < 100) {
                    console.log(`Title candidate: "${text}" in ${el.tagName}.${$prop(el).attr('class') || ''}`);
                }
            });
        } else {
            console.log("⚠️ Could not find a property URL to examine individual page");
        }

    } catch (error) {
        console.error("❌ Debug error:", error.message);
    }
}

// Run the debug
debugAgencyUKStructure();