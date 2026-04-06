const { chromium } = require('playwright');
const { extractCoordinatesFromHTML } = require('./lib/property-helpers.js');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const url = 'https://robsonsweb.com/property/high-road-ickenham/';
  
  console.log(`Navigating to ${url}...`);
  await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
  const html = await page.content();
  
  const coords = await extractCoordinatesFromHTML(html);
  console.log("Coords found:", coords);
  
  if (!coords.latitude) {
      console.log("Searching for alternative mapping patterns...");
      // Check for Google Maps iframe
      const iframes = await page.$$eval('iframe', els => els.map(e => e.src));
      console.log("Iframes:", iframes);
      
      // Look for data attributes on common map containers
      const dataAttrs = await page.evaluate(() => {
          const els = document.querySelectorAll('*');
          const res = [];
          for (const el of els) {
              if (el.dataset.lat || el.dataset.lng || el.dataset.latitude || el.dataset.longitude) {
                  res.push({ tag: el.tagName, id: el.id, class: el.className, data: el.dataset });
              }
          }
          return res;
      });
      console.log("Data attributes with coords:", dataAttrs);
      
      // Check for Property Hive's JS map data
      const phMapData = await page.evaluate(() => typeof ph_map_data !== 'undefined' ? ph_map_data : null);
      if (phMapData) console.log("ph_map_data found in global scope!");
  }
  
  await browser.close();
})();
