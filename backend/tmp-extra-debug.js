const { chromium } = require('playwright');

(async () => {
    const browser = await chromium.launch();
    const page = await browser.newPage();
    const url = 'https://robsonsweb.com/property/high-road-ickenham/';
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    
    // Look for anything that looks like "map" or "location" in the whole page text
    const text = await page.evaluate(() => document.body.innerText);
    const mentionsMap = text.includes('Map') || text.includes('Location');
    console.log("Mentions Map/Location?", mentionsMap);
    
    const postcodeRegex = /([A-Z]{1,2}[0-9][A-Z0-9]? [0-9][ABD-HJLNP-UW-Z]{2})/gi;
    const postcode = text.match(postcodeRegex);
    console.log("Postcode found:", postcode);
    
    // Look for any <img> srcs that might be static maps
    const images = await page.$$eval('img', els => els.map(e => e.src));
    const mapImages = images.filter(s => s.includes('maps.googleapis.com') || s.includes('staticmap'));
    console.log("Map images found:", mapImages);

    await browser.close();
})();
