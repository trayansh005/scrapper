const { chromium } = require('playwright');

async function debug() {
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    console.log('Navigating to Gatekeeper...');
    await page.goto('https://www.gatekeeper.co.uk/properties', { waitUntil: 'networkidle' });
    
    await page.click('#buyBtn');
    await page.waitForTimeout(1000);
    await page.evaluate(() => {
        const buttons = Array.from(document.querySelectorAll('button'));
        const searchBtn = buttons.find(b => b.innerText.trim() === 'Search' && b.id !== 'postcode_search_btn');
        if (searchBtn) searchBtn.click();
    });
    await page.waitForTimeout(5000);

    const cardHtml = await page.evaluate(() => {
        const card = document.querySelector('a[id^="property_"]');
        return card ? card.innerHTML : 'no card';
    });
    console.log('Card HTML:', cardHtml);

    await browser.close();
}

debug().catch(console.error);
