const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const url = "https://jacksonsproperty.co.uk/sales/property-for-sale?is_available=true&sort=suggested";
  
  console.log(`Navigating to ${url}...`);
  await page.goto(url, { waitUntil: 'networkidle' });
  await page.waitForTimeout(5000);
  
  const propertyCount = await page.evaluate(() => {
    return document.querySelectorAll('a[href*="/properties/"]').length;
  });
  console.log(`Initial properties found: ${propertyCount}`);
  
  const buttons = await page.evaluate(() => {
    return Array.from(document.querySelectorAll('button, a, div[role="button"]'))
      .map(b => ({
        tag: b.tagName,
        text: b.innerText.trim(),
        class: b.className,
        visible: b.offsetWidth > 0 && b.offsetHeight > 0
      }))
      .filter(b => b.text.length > 0 && b.text.length < 50);
  });
  
  console.log("Candidate buttons for 'Load More':");
  buttons.forEach(b => {
    if (b.text.toLowerCase().includes('more') || b.text.toLowerCase().includes('show') || b.text.toLowerCase().includes('view')) {
        console.log(JSON.stringify(b));
    }
  });

  // Take a screenshot to see what's happening
  await page.screenshot({ path: 'jacksons_debug.png', fullPage: true });
  console.log("Screenshot saved to jacksons_debug.png");

  await browser.close();
})();
