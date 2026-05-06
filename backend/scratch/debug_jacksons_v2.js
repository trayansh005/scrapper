const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const url = "https://jacksonsproperty.co.uk/sales/property-for-sale?is_available=true&sort=suggested";
  
  console.log(`Navigating to ${url}...`);
  await page.goto(url, { waitUntil: 'networkidle' });
  await page.waitForTimeout(5000);
  
  const buttonDetails = await page.evaluate(() => {
    const all = Array.from(document.querySelectorAll('*'));
    return all
      .filter(el => el.innerText && el.innerText.toUpperCase().includes('LOAD MORE'))
      .map(el => ({
        tag: el.tagName,
        text: el.innerText.trim(),
        class: el.className,
        visible: el.offsetWidth > 0 && el.offsetHeight > 0,
        rect: el.getBoundingClientRect()
      }))
      .filter(el => el.text.length < 100);
  });
  
  console.log("Found elements with 'LOAD MORE':");
  console.log(JSON.stringify(buttonDetails, null, 2));

  await browser.close();
})();
