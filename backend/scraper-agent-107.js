// Belvoir scraper using Playwright with Crawlee
// Agent ID: 107
// Website: belvoir.co.uk
// Usage:
// node backend/scraper-agent-107.js

const { PlaywrightCrawler, log } = require('crawlee');
const { updateRemoveStatus } = require('./db.js');
const {
  updatePriceByPropertyURLOptimized,
  processPropertyWithCoordinates
} = require('./lib/db-helpers.js');
const { isSoldProperty, parsePrice } = require('./lib/property-helpers.js');

const AGENT_ID = 107;

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const stats = {
  totalScraped: 0,
  totalSaved: 0
};

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
  return (
    process.env.BROWSERLESS_WS_ENDPOINT ||
    `ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
  );
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
  const detailPage = await browserContext.newPage();
  
  try {
    // Block unnecessary resources
    await detailPage.route('**/*', (route) => {
      const resourceType = route.request().resourceType();
      if (['image', 'font', 'stylesheet', 'media'].includes(resourceType)) {
        route.abort();
      } else {
        route.continue();
      }
    });

    await detailPage.goto(property.link, {
      waitUntil: 'domcontentloaded',
      timeout: 60000
    });

    // Wait for the property content to load
    await detailPage.waitForSelector('.property--card', { timeout: 30000 }).catch(() => {
        // Detail page might have a different structure
    });

    const html = await detailPage.content();
    
    // Save property to database
    // Belvoir detail pages are simple, coordinates usually extracted from HTML by processPropertyWithCoordinates
    await processPropertyWithCoordinates(
      property.link,
      property.price,
      property.title,
      property.bedrooms || null,
      AGENT_ID,
      isRental,
      html
    );

    stats.totalScraped++;
    stats.totalSaved++;
  } catch (error) {
    console.error(` Error scraping detail page ${property.link}:`, error.message);
  } finally {
    await detailPage.close();
  }
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
  const { isRental, label, pageNumber } = request.userData;
  console.log(`\n Loading [${label}] Page ${pageNumber}: ${request.url}`);

  try {
    // Navigate and wait for content
    await page.goto(request.url, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.waitForTimeout(2000); // Small wait for dynamic content

    // Extract properties
    const properties = await page.evaluate(() => {
      const containers = Array.from(document.querySelectorAll('.property--card'));
      const items = [];

      for (const container of containers) {
        const linkEl = container.querySelector('a');
        const link = linkEl ? linkEl.href : null;
        
        const titleEl = container.querySelector('.property--card-title');
        const title = titleEl ? titleEl.innerText.trim() : '';
        
        const priceEl = container.querySelector('.property--card-price');
        const priceText = priceEl ? priceEl.innerText.trim() : '';
        
        const statusEl = container.querySelector('.property--card-status');
        const statusText = statusEl ? statusEl.innerText.trim() : '';
        
        let bedrooms = null;
        // Search for bedrooms in title or features
        const bedMatch = title.match(/(\d+)\s*bedrooms?/i);
        if (bedMatch) {
          bedrooms = parseInt(bedMatch[1]);
        }

        if (link && priceText) {
          items.push({ link, title, priceText, bedrooms, statusText });
        }
      }
      return items;
    });

    // De-duplicate properties on the same page
    const uniqueProperties = [];
    const seenLinks = new Set();
    for (const p of properties) {
      if (!seenLinks.has(p.link)) {
        seenLinks.add(p.link);
        uniqueProperties.push(p);
      }
    }

    console.log(`    Found ${uniqueProperties.length} unique properties on [${label}] Page ${pageNumber}`);

    for (const property of uniqueProperties) {
      if (isSoldProperty(property.statusText || '')) {
        console.log(`    Skipping sold/let: ${property.title}`);
        continue;
      }

      const price = parsePrice(property.priceText);
      if (!price) {
        console.log(`    Price not found for: ${property.title}`);
        continue;
      }

      const updateResult = await updatePriceByPropertyURLOptimized(
        property.link,
        price,
        property.title,
        property.bedrooms,
        AGENT_ID,
        isRental
      );

      if (updateResult.updated) {
        stats.totalSaved++;
      }

      if (!updateResult.isExisting && !updateResult.error) {
        console.log(`    New property: ${property.title} - £${price}`);
        await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
        // Delay between detail requests
        await new Promise(r => setTimeout(r, 2000));
      } else {
        // No noise for existing properties
      }
    }
    // Delay between listing pages
    await new Promise(r => setTimeout(r, 3000));
      }
    }
  } catch (error) {
    console.error(` Error in handleListingPage: ${error.message}`);
  }
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
  return new PlaywrightCrawler({
    maxConcurrency: 1, // Be polite
    maxRequestRetries: 2,
    requestHandlerTimeoutSecs: 300,
    launchContext: {
      launcher: undefined,
      launchOptions: {
        browserWSEndpoint,
        args: ['--no-sandbox', '--disable-setuid-sandbox']
      }
    },
    requestHandler: handleListingPage,
    failedRequestHandler({ request }) {
      console.error(` Failed listing page: ${request.url}`);
    }
  });
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeBelvoir() {
  console.log(` Starting Belvoir Scraper (Agent ${AGENT_ID})...`);
  
  const browserWSEndpoint = getBrowserlessEndpoint();
  const crawler = createCrawler(browserWSEndpoint);

  // Belvoir now uses specific URLs for "In United Kingdom" which support pagination via /page/N/
  const PROPERTY_TYPES = [
    {
      baseUrl: "https://www.belvoir.co.uk/properties/for-sale/in-united-kingdom/",
      isRental: false,
      label: "SALES",
      totalPages: 37
    },
    {
      baseUrl: "https://www.belvoir.co.uk/properties/to-rent/in-united-kingdom/",
      isRental: true,
      label: "RENTALS",
      totalPages: 24
    }
  ];

  for (const type of PROPERTY_TYPES) {
    const requests = [];
    for (let p = 1; p <= type.totalPages; p++) {
      const url = p === 1 ? type.baseUrl : `${type.baseUrl}page/${p}/`;
      requests.push({
        url,
        userData: {
          pageNumber: p,
          isRental: type.isRental,
          label: type.label
        }
      });
    }
    await crawler.addRequests(requests);
  }

  await crawler.run();

  console.log(`\n Finished Belvoir - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
  try {
    await scrapeBelvoir();
    await updateRemoveStatus(AGENT_ID);
    process.exit(0);
  } catch (err) {
    console.error(' Fatal error:', err?.message || err);
    process.exit(1);
  }
})();
