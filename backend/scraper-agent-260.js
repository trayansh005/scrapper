'use strict';

const { PlaywrightCrawler, sleep } = require('crawlee');
const { createAgentLogger } = require('./lib/logger-helpers.js');
const { isSoldProperty, parsePrice, formatPriceDisplay } = require('./lib/property-helpers.js');
const { updatePriceByPropertyURLOptimized, processPropertyWithCoordinates } = require('./lib/db-helpers.js');
const { updateRemoveStatus } = require('./db.js');

const AGENT_ID = 260;
const logger = createAgentLogger(AGENT_ID);

const BROWSERLESS_URL = process.env.BROWSERLESS_URL || 'ws://browserless-e44co4wws040gcokws8k0c00:3000';

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

function blockNonEssentialResources(page) {
  return page.route('**/*', (route) => {
    const url = route.request().url();
    if (/\.(png|jpg|jpeg|gif|webp|svg|woff|woff2|ttf|eot|mp4|webm)$/i.test(url)) {
      route.abort();
    } else {
      route.continue();
    }
  });
}

async function handleListingPage({ page, request }) {
  const { pageNum, totalPages, isRental, label } = request.userData;
  
  logger.page(pageNum, label, request.url, totalPages);

  await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => null);
  await page.waitForTimeout(1000);

  // Try to load all properties (handle pagination/scroll if needed)
  let loadedAtLeastOnce = false;
  let previousCount = 0;
  let attempts = 0;
  const maxAttempts = 10;

  while (attempts < maxAttempts) {
    const currentCount = await page.evaluate(() => {
      return document.querySelectorAll('ul.properties li.property, li.property').length;
    });

    if (currentCount === previousCount && loadedAtLeastOnce) {
      break; // No more items loading
    }

    previousCount = currentCount;
    loadedAtLeastOnce = true;

    // Try to scroll to bottom to trigger lazy loading
    await page.evaluate(() => {
      window.scrollBy(0, window.innerHeight);
    });

    // Try to click "load more" button if exists
    const loadMoreClicked = await page.evaluate(() => {
      const buttons = Array.from(document.querySelectorAll('button, a, input'));
      const loadBtn = buttons.find(btn => 
        btn.textContent.toLowerCase().includes('load more') ||
        btn.textContent.toLowerCase().includes('show more') ||
        btn.textContent.toLowerCase().includes('next')
      );
      if (loadBtn && !loadBtn.disabled) {
        loadBtn.click();
        return true;
      }
      return false;
    });

    await page.waitForTimeout(800);
    attempts++;
  }

  logger.page(pageNum, label, `After pagination attempt: ${previousCount} properties found on page`, totalPages);

  const extractionResult = await page.evaluate(() => {
    const results = [];
    const seen = new Set();
    let debugInfo = {
      selectors_tried: [],
      elements_found: 0
    };

    // FreeAgent247 uses Property Hive plugin - target the container <li class="property">
    const selectorPatterns = [
      'ul.properties li.property',     // Property Hive: ul > li.property
      'ul.properties li.type-property', // Alternative Property Hive selector
      'li.property',                    // Fallback: just the li.property class
      'li.type-property',               // Alternative fallback
      'ul.properties li',               // Generic: all li in properties ul
      '.propertyhive li.property',      // Wrapper class fallback
      'article.property',               // Some themes use article tags
      '[data-property-id]'              // Data attribute approach
    ];

    let propertyContainers = [];
    
    for (const selector of selectorPatterns) {
      propertyContainers = Array.from(document.querySelectorAll(selector));
      debugInfo.selectors_tried.push(selector);
      if (propertyContainers.length > 0) {
        debugInfo.elements_found = propertyContainers.length;
        debugInfo.matching_selector = selector;
        break;
      }
    }

    // Process each property container
    for (const container of propertyContainers) {
      try {
        // Find the property link within the container (in thumbnail or h3)
        const linkEl = container.querySelector('a[href*="/property/"]');
        if (!linkEl) continue;

        let url = linkEl.getAttribute('href') || '';
        if (!url || url.length < 8 || url.includes('/page/') || url === '#') continue;

        const fullLink = url.startsWith('http') ? url : new URL(url, window.location.origin).href;
        if (seen.has(fullLink)) continue;
        seen.add(fullLink);

        // Extract title from h3 link or fallback to linkEl text
        let title = '';
        const h3Link = container.querySelector('h3 a');
        if (h3Link) {
          title = h3Link.textContent?.trim() || '';
        } else {
          title = linkEl.textContent?.trim() || linkEl.getAttribute('title') || '';
        }
        
        if (!title || title.length < 3) title = `Property ${seen.size}`;
        title = title.replace(/^\s*[#\s]+/, '').substring(0, 160).trim();

        // Extract price from .price or .fa247price div
        let priceText = '';
        const priceDiv = container.querySelector('.price, .fa247price, [class*="price"]');
        if (priceDiv) {
          const priceMatch = priceDiv.textContent.match(/£[\s]?[\d,]+(?:\.\d+)?/);
          if (priceMatch) priceText = priceMatch[0].trim();
        }

        // Extract bedrooms from .bedrooms span
        let bedrooms = '';
        const bedroomSpan = container.querySelector('.bedrooms, [class*="bedroom"]');
        if (bedroomSpan) {
          const bedMatch = bedroomSpan.textContent.match(/(\d{1,2})\s*(?:bed|bedroom)/i);
          if (bedMatch) bedrooms = bedMatch[1];
        }

        // Extract status from flag or container text
        let statusText = '';
        const flagDiv = container.querySelector('.flag, [class*="flag"]');
        if (flagDiv) {
          const flagText = flagDiv.textContent?.toLowerCase() || '';
          const statusPatterns = ['sold', 'let agreed', 'under offer', 'reserved', 'withdrawn', 'for sale', 'to let'];
          for (const pattern of statusPatterns) {
            if (flagText.includes(pattern)) {
              statusText = pattern.toUpperCase();
              break;
            }
          }
        }

        // Only include if has valid URL and looks like a real property listing
        // Be less strict - include if URL has /property/ (that's the key indicator)
        if (fullLink && fullLink.includes('/property/')) {
          results.push({
            link: fullLink,
            title: title,
            priceText: priceText,
            bedrooms: bedrooms,
            statusText: statusText
          });
        }
      } catch (e) {
        // Skip element silently
      }
    }

    debugInfo.final_count = results.length;
    return { results, debugInfo };
  });

  const properties = extractionResult.results;
  logger.page(pageNum, label, `Extraction details - Selector: "${extractionResult.debugInfo.matching_selector || 'NONE'}" | Elements found: ${extractionResult.debugInfo.elements_found} | Properties extracted: ${extractionResult.debugInfo.final_count}`, totalPages);

  logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

  // Filter and prepare for DB processing
  const scraped = [];
  for (const prop of properties) {
    let url = prop.link?.trim();
    if (!url || !url.includes('/property/')) continue;

    if (isSoldProperty(prop.statusText)) {
      logger.property(pageNum, label, prop.title.substring(0, 40), prop.priceText, url, isRental, totalPages, 'SKIPPED');
      continue;
    }

    const price = parsePrice(prop.priceText);
    if (!price) {
      logger.page(pageNum, label, `Skipping - no valid price: ${prop.title}`, totalPages);
      continue;
    }

    scraped.push({
      title: prop.title,
      url: url,
      price,
      priceDisplay: prop.priceText || '',
      bedrooms: prop.bedrooms ? parseInt(prop.bedrooms, 10) : null,
      statusText: prop.statusText,
    });
  }

  logger.page(pageNum, label, `→ ${scraped.length} valid properties ready to process`, totalPages);

  // Process each property
  for (const property of scraped) {
    if (!property.url) continue;

    const result = await updatePriceByPropertyURLOptimized(
      property.url,
      property.price,
      property.title,
      property.bedrooms,
      AGENT_ID,
      isRental,
    );

    let propertyAction = 'UNCHANGED';

    if (result.updated) {
      counts.totalSaved++;
      propertyAction = 'UPDATED';
    }

    if (!result.isExisting && !result.error) {
      const detail = await scrapePropertyDetail(page.context(), property.url);

      await processPropertyWithCoordinates(
        property.url.trim(),
        property.price,
        property.title,
        property.bedrooms,
        AGENT_ID,
        isRental,
        null,
        detail?.coords?.latitude || null,
        detail?.coords?.longitude || null,
      );

      counts.totalSaved++;
      counts.totalScraped++;
      if (isRental) counts.savedRentals++;
      else counts.savedSales++;
      propertyAction = 'CREATED';
    } else if (result.error) {
      propertyAction = 'ERROR';
    }

    logger.property(
      pageNum,
      label,
      property.title.substring(0, 40),
      formatPriceDisplay(property.price, isRental),
      property.url,
      isRental,
      totalPages,
      propertyAction,
    );

    // Only sleep if property was created (optimization pattern)
    if (propertyAction === 'CREATED') {
      await sleep(500);
    }
  }
}

async function scrapePropertyDetail(browserContext, propertyUrl) {
  await sleep(500);
  const detailPage = await browserContext.newPage();

  try {
    await blockNonEssentialResources(detailPage);
    await detailPage.goto(propertyUrl, { waitUntil: 'domcontentloaded', timeout: 60000 });
    await detailPage.waitForTimeout(500);

    const coords = await detailPage.evaluate(() => {
      // Method 1: Try JSON-LD schema markup
      const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
      for (const script of scripts) {
        try {
          const data = JSON.parse(script.textContent);
          if (data?.geo?.latitude && data?.geo?.longitude) {
            return {
              latitude: parseFloat(data.geo.latitude),
              longitude: parseFloat(data.geo.longitude)
            };
          }
        } catch (e) { }
      }

      // Method 2: Try to extract from Google Maps JavaScript (FreeAgent247 pattern)
      // Looks for: new google.maps.LatLng(52.129371840672, -2.3227071762085)
      const allScripts = Array.from(document.querySelectorAll('script'));
      for (const script of allScripts) {
        const content = script.textContent;
        if (content && content.includes('google.maps.LatLng')) {
          const coordMatch = content.match(/google\.maps\.LatLng\s*\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)/);
          if (coordMatch && coordMatch[1] && coordMatch[2]) {
            const lat = parseFloat(coordMatch[1]);
            const lon = parseFloat(coordMatch[2]);
            if (!isNaN(lat) && !isNaN(lon) && lat !== 0 && lon !== 0) {
              return {
                latitude: lat,
                longitude: lon
              };
            }
          }
        }
      }

      return { latitude: null, longitude: null };
    });

    return { coords };
  } catch (err) {
    logger.error(`Error scraping detail page ${propertyUrl}: ${err.message}`);
    return { coords: { latitude: null, longitude: null } };
  } finally {
    await detailPage.close();
  }
}

async function run() {
  const args = process.argv.slice(2);
  const startPage = args[0] ? parseInt(args[0], 10) : 1;
  const isPartialRun = startPage > 1;
  const scrapeStartTime = new Date();

  logger.step(`Starting Agent ${AGENT_ID} - FreeAgent247`);
  logger.step(`Start Page: ${startPage}`);

  const PROPERTY_TYPES = [
    {
      baseUrl: 'https://freeagent247.com/buy-property/?department=residential-sales&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&availability=4&property_type=',
      totalPages: 2,
      isRental: false,
      label: 'SALES',
    },
    {
      baseUrl: 'https://freeagent247.com/rent-property/?department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&availability=4&property_type=',
      totalPages: 8,
      isRental: true,
      label: 'RENTALS',
    }
  ];

  const crawler = new PlaywrightCrawler({
    maxConcurrency: 1,
    maxRequestRetries: 2,
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 180,

    launchContext: {
      launchOptions: {
        browserWSEndpoint: BROWSERLESS_URL,
        args: ['--no-sandbox', '--disable-setuid-sandbox'],
        viewport: { width: 1920, height: 1080 },
      },
    },

    preNavigationHooks: [
      async ({ page }) => {
        await blockNonEssentialResources(page);
      },
    ],

    requestHandler: handleListingPage,

    failedRequestHandler: async ({ request }) => {
      const { pageNum, label } = request.userData;
      logger.error(`Failed listing page - ${label} page ${pageNum}: ${request.url}`);
    },
  });

  // Pre-build all requests
  const allRequests = [];
  for (const type of PROPERTY_TYPES) {
    logger.step(`Queueing ${type.label} (${type.totalPages} pages)`);
    for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
      allRequests.push({
        url: `${type.baseUrl}${type.baseUrl.includes('?') ? '&' : '?'}page=${pg}`,
        userData: {
          pageNum: pg,
          totalPages: type.totalPages,
          isRental: type.isRental,
          label: type.label,
        },
      });
    }
  }

  try {
    if (startPage === 1) {
      logger.step('Full run detected - will cleanup removed properties at end');
    } else {
      logger.step('Partial run detected - skipping cleanup');
    }

    if (allRequests.length > 0) {
      await crawler.run(allRequests);
    } else {
      logger.step('No requests to process');
    }

    logger.step(`Completed Agent ${AGENT_ID} - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}`);
    logger.step(`Breakdown - SALES: ${counts.savedSales}, RENTALS: ${counts.savedRentals}`);

    if (startPage === 1) {
      logger.step('Updating remove status...');
      await updateRemoveStatus(AGENT_ID, scrapeStartTime);
    } else {
      logger.step('Partial run - skipping updateRemoveStatus');
    }

    logger.step(`Agent ${AGENT_ID} completed successfully`);
  } catch (err) {
    logger.error(`Agent ${AGENT_ID} failed: ${err.message}`);
    process.exit(1);
  }
}

run().catch(err => {
  logger.error(`Unhandled error: ${err.message}`);
  process.exit(1);
});