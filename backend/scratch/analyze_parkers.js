const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto('https://www.parkersproperties.co.uk/search-results/for-sale/in-south-england/?orderby=price_desc&department=residential');
  
  await page.waitForSelector('.property-banner, .property-banner-image-wrapper', { timeout: 10000 }).catch(() => {});
  
  const data = await page.evaluate(() => {
    // Find property cards
    // Usually it's an element that contains the property link and price
    const cards = Array.from(document.querySelectorAll('div, section')).filter(el => {
        return el.innerHTML.includes('/properties-for-sale/') && (el.innerText.includes('£') || el.innerText.includes('POA'));
    });
    
    // Sort by smallest elements that contain the link to find the actual card
    const card = cards.sort((a, b) => a.innerText.length - b.innerText.length)[0];

    return {
      totalCount: document.body.innerText.match(/Showing \d+ of (\d+) results/)?.[1],
      cardHtml: card ? card.outerHTML : 'Not found',
      cardText: card ? card.innerText : 'Not found'
    };
  });
  
  console.log(JSON.stringify(data, null, 2));
  await browser.close();
})();
