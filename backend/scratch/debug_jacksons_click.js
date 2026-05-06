const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  const url = "https://jacksonsproperty.co.uk/sales/property-for-sale?is_available=true&sort=suggested";
  
  console.log(`Navigating to ${url}...`);
  await page.goto(url, { waitUntil: 'networkidle' });
  await page.waitForTimeout(5000);
  
  // Accept cookies if present
  const acceptBtn = await page.getByText('Accept', { exact: true }).first();
  if (await acceptBtn.isVisible()) {
      console.log("Accepting cookies...");
      await acceptBtn.click();
      await page.waitForTimeout(2000);
  }

  const initialCount = await page.evaluate(() => document.querySelectorAll('a[href*="/properties/"]').length);
  console.log(`Initial count: ${initialCount}`);

  // Find and click "Load More"
  const loadMoreBtn = await page.locator('div:has-text("LOAD MORE PROPERTIES")').last();
  if (await loadMoreBtn.isVisible()) {
      console.log("Clicking Load More...");
      await loadMoreBtn.click();
      await page.waitForTimeout(5000);
      
      const afterCount = await page.evaluate(() => document.querySelectorAll('a[href*="/properties/"]').length);
      console.log(`Count after click: ${afterCount}`);
  } else {
      console.log("Load More button not found or not visible");
  }

  await page.screenshot({ path: 'jacksons_after_click.png', fullPage: true });
  await browser.close();
})();
