const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const context = await browser.newContext();
  const page = await context.newPage();

  console.log("Navigating to Robsons sales...");
  await page.goto('https://robsonsweb.com/search-results/?department=residential-sales', { waitUntil: 'domcontentloaded', timeout: 60000 });
  
  const properties = await page.$$eval('ul.properties li.property, li.property', elements => {
      return elements.map(el => {
          let link = el.querySelector('a') ? el.querySelector('a').href : null;
          let statusText = '';
          const statusEl = el.querySelector('.property-status, .sticker, .flag');
          if (statusEl) statusText = statusEl.textContent.trim();
          
          let rawHTML = el.innerHTML;
          return { link, statusText, html_snippet: rawHTML.replace(/\s+/g, ' ').substring(0, 300) };
      });
  });

  console.log(`Found ${properties.length} properties.`);
  properties.slice(0, 3).forEach(p => console.log(p));
  
  // also check lettings page
  console.log("\nNavigating to lettings...");
  await page.goto('https://robsonsweb.com/search-results/?department=residential-lettings', { waitUntil: 'domcontentloaded', timeout: 60000 });
  const lettings = await page.$$eval('ul.properties li.property, li.property', elements => {
      return elements.map(el => {
          let link = el.querySelector('a') ? el.querySelector('a').href : null;
          let statusText = '';
          const statusEl = el.querySelector('.property-status, .sticker, .flag, .status');
          if (statusEl) statusText = statusEl.textContent.trim();
          
          // try to grab the full innerText to see if status is mentioned somewhere else
          let allText = el.innerText;
          return { link, statusText, allText: allText.replace(/\n/g, ' - ') };
      });
  });
  console.log(`Found ${lettings.length} lettings.`);
  lettings.slice(0, 3).forEach(p => console.log(p));

  await browser.close();
})();
