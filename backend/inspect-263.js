const { chromium } = require('playwright');
const fs = require('fs');

(async () => {
    const browser = await chromium.launch();
    const page = await browser.newPage();
    await page.goto("https://www.sallyhatcher.co.uk/properties-to-buy");
    await page.waitForTimeout(3000);
    
    const html = await page.evaluate(() => {
        const links = document.querySelectorAll('a[href$=".php"]');
        let data = [];
        for (let i = 0; i < Math.min(2, links.length); i++) {
            // go up up up until we see £
            let el = links[i];
            let topHTML = '';
            for(let j=0; j<5; j++) {
                if(el && el.parentElement) {
                    el = el.parentElement;
                    if(el.textContent.includes('£')) {
                        topHTML = el.outerHTML;
                        break;
                    }
                }
            }
            // Also check for SOLD STC
            const hasSold = topHTML.includes('SOLD STC');

            data.push({
                href: links[i].href,
                hasSold,
                html: topHTML || el.outerHTML
            });
        }
        return data;
    });

    console.log(JSON.stringify(html, null, 2));
    await browser.close();
})();
