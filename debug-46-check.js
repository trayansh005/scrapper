const { chromium } = require("playwright");
(async () => {
    try {
        const browser = await chromium.launch({ headless: true });
        const context = await browser.newContext({ userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36" });
        const page = await context.newPage();
        console.log("Navigating to Connells...");
        await page.goto("https://www.connells.co.uk/properties/sales", { waitUntil: "domcontentloaded", timeout: 60000 });
        await page.waitForTimeout(5000);
        const data = await page.evaluate(() => {
            return {
                next: !!window.__NEXT_DATA__,
                props: !!window.properties,
                propertyData: !!window.propertyData,
                text: document.body.innerText.substring(0, 200)
            };
        });
        console.log("RESULT:", JSON.stringify(data));
        await browser.close();
    } catch (e) {
        console.log("ERROR:", e.message);
    }
})();
