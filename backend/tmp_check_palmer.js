const { chromium } = require('playwright');

async function check() {
    const browser = await chromium.connectOverCDP('ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv');
    const page = await browser.newPage();
    const searchUrl = 'https://www.palmerpartners.com/buy/property-for-sale/';

    console.log(`Visiting ${searchUrl}...`);
    try {
        await page.goto(searchUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await page.waitForTimeout(3000);

        // Find a property link
        const detailLink = await page.evaluate(() => {
            const a = document.querySelector('a[href*="/property/"]');
            return a ? a.href : null;
        });

        if (!detailLink) {
            console.log('No detail link found on search page.');
            const html = await page.content();
            console.log('Search page snippet:', html.substring(0, 500));
            return;
        }

        console.log(`Navigating to detail page: ${detailLink}`);
        await page.goto(detailLink, { waitUntil: 'domcontentloaded', timeout: 60000 });
        await page.waitForTimeout(3000);

        const html = await page.content();

        console.log('--- Detail Page HTML Snippet ---');
        // Search for bedrooms
        const bedMatches = html.match(/(\d+)\s*bedrooms?/i) || html.match(/bedroom\s*(\d+)/i);
        console.log('Potential bedroom matches:', bedMatches ? bedMatches.slice(0, 3) : 'none');

        // Search for coordinates
        const latLngMatches = html.match(/["']?lat["']?\s*[:=]\s*["']?([0-9.-]+)["']?/i);
        console.log('Potential lat match:', latLngMatches ? latLngMatches[1] : 'none');

        const lngMatches = html.match(/["']?lng["']?\s*[:=]\s*["']?([0-9.-]+)["']?/i);
        console.log('Potential lng match:', lngMatches ? lngMatches[1] : 'none');

        // Check for specific script data
        const scriptJson = html.match(/script[^>]*type="application\/ld\+json"[^>]*>([\s\S]*?)<\/script/gi);
        console.log('Found LD+JSON scripts:', scriptJson ? scriptJson.length : 0);

        // Print more HTML to see the structure
        const bodyIndex = html.indexOf('<body');
        console.log('Body snippet (first 1000):', html.substring(bodyIndex, bodyIndex + 1000));

    } catch (err) {
        console.error('Error:', err.message);
    } finally {
        await browser.close();
    }
}

check();
