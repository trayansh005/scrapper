const { chromium } = require("playwright");

(async () => {
    // We'll use headless: true for CI/automation unless user needs to see it
    const browser = await chromium.launch({ headless: true });
    const context = await browser.newContext();
    const page = await context.newPage();

    console.log('--- SNIFFING CONNELLS RSC ---');

    page.on("response", async (response) => {
        const url = response.url();
        if (url.includes("_rsc=")) {
            try {
                const text = await response.text();
                // Check if it's the large property list payload
                if (text.includes('"lat"') && text.includes('"lng"')) {
                    console.log(`\n[SUCCESS] Found Property Payload: ${url.split('?')[0]}`);
                    console.log(`Length: ${text.length}`);
                    // Save sample for analysis
                    const fs = require('fs');
                    fs.writeFileSync('backend/rsc_sample.txt', text);
                    console.log('Sample saved to backend/rsc_sample.txt');
                }
            } catch (e) {}
        }
    });

    try {
        await page.goto("https://www.connells.co.uk/properties/sales", { waitUntil: "networkidle", timeout: 45000 });
        console.log('Base page loaded. Waiting for lazy RSC...');
        
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight / 2));
        await new Promise((r) => setTimeout(r, 2000));
        
        // Trigger page 2
        console.log('Attempting to trigger Page 2 RSC...');
        await page.evaluate(() => {
            const nextBtn = Array.from(document.querySelectorAll('button')).find(el => el.innerText.trim() === 'NEXT');
            if (nextBtn) nextBtn.click();
        });
        
        await new Promise((r) => setTimeout(r, 5000));
    } catch (err) {
        console.error('Navigation/Action Error:', err.message);
    } finally {
        await browser.close();
        console.log('--- DONE ---');
    }
})();
