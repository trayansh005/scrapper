'use strict';

const { PlaywrightCrawler, sleep } = require('crawlee');
const { createAgentLogger } = require('./lib/logger-helpers.js');
const { isSoldProperty, parsePrice } = require('./lib/property-helpers.js');
const { updatePriceByPropertyURLOptimized, processPropertyWithCoordinates } = require('./lib/db-helpers.js');
const { updateRemoveStatus } = require('./db.js');

const AGENT_ID = 260;
const PROPERTY_TYPES = { SALE: 'sale', RENTAL: 'rental' };

const logger = createAgentLogger('Agent-260');

const BROWSERLESS_URL = process.env.BROWSERLESS_URL || 'ws://browserless-e44co4wws040gcokws8k0c00:3000';

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

async function handleListingPage(page, pageNum, totalPages, isRental, label) {
  const PROPERTY_CARD_SELECTOR = 'article, .property-card, .listing-item, p, [class*="property"], [class*="listing"]';

  logger.page(pageNum, label, 'Waiting for page to load...');

  await page.waitForLoadState('networkidle', { timeout: 30000 }).catch(() => null);

  await page.waitForSelector(PROPERTY_CARD_SELECTOR, { timeout: 15000 })
    .catch(() => logger.page(pageNum, label, 'Property selector timeout'));

  const properties = await page.evaluate(() => {
    const results = [];
    const seen = new Set();

    // 1. Standard anchor tags
    const standardLinks = Array.from(document.querySelectorAll('a[href*="/property/"]'));

    // 2. Markdown-style links: [text](url)
    const markdownRegex = /\[([^\]]+?)\]\((https?:\/\/[^\s)]+)\)/gi;
    const bodyHTML = document.documentElement.innerHTML || document.body.innerHTML || '';
    const markdownLinks = [];

    let match;
    while ((match = markdownRegex.exec(bodyHTML)) !== null) {
      const url = match[2].trim();
      if (url.includes('/property/') && !url.includes('/page/')) {
        markdownLinks.push({
          href: url,
          text: match[1].trim()
        });
      }
    }

    const allLinkItems = [...standardLinks, ...markdownLinks];

    for (const item of allLinkItems) {
      let href = null;

      if (typeof item === 'object') {
        if (item.href && typeof item.href === 'string') {
          href = item.href;
        } else if (item.getAttribute) {
          const attr = item.getAttribute('href');
          if (typeof attr === 'string') href = attr;
        }
      }

      if (typeof href !== 'string') return;
      href = href.trim(); if (!href || typeof href !== 'string') continue;

      href = href.trim();
      if (seen.has(href)) continue;
      seen.add(href);

      const fullLink = href.startsWith('http')
        ? href
        : `https://freeagent247.com${href.startsWith('/') ? '' : '/'}${href}`;

      // Find best container for metadata
      let container = item instanceof Element ? item : null;
      let depth = 0;
      const maxDepth = 10;

      while (container && depth < maxDepth) {
        container = container.parentElement;
        if (!container) break;

        const textLen = (container.textContent || '').trim().length;
        if (textLen > 150 && textLen < 15000) {
          break;
        }
        depth++;
      }

      const containerText = container ? (container.textContent || '') : '';

      // Extract Title
      let title = '';
      if (item.text && typeof item.text === 'string') {
        title = item.text.trim();
      } else if (item instanceof Element) {
        title = (item.textContent || '').trim().replace(/\s+/g, ' ');
      }

      // Fallback: Look for #WelcomeHome pattern
      if (title.length < 15 && containerText) {
        const welcomeMatch = containerText.match(/#?WelcomeHome\s+to[:\s]*(.+?)(?=\n|\r|\s{2,})/i);
        if (welcomeMatch) title = welcomeMatch[1].trim();
      }

      if (title.length > 180) title = title.substring(0, 180);
      if (!title) title = 'Property Listing';

      // Extract Price
      const priceMatch = containerText.match(/£\s*[\d,]+(?:\.\d+)?(?:\s*(?:Offers?\s*Over|OVO|OIEO))?/i);
      const priceText = priceMatch ? priceMatch[0].trim() : '';

      // Extract Bedrooms
      const bedMatch = containerText.match(/(\d+)\s*(?:bed|bedroom|beds)/i);
      const bedrooms = bedMatch ? bedMatch[1] : '';

      // Extract Status
      let statusText = '';
      const statusKeywords = ['sold', 'let agreed', 'under offer', 'reserved', 'withdrawn', 'sold stc'];
      for (const kw of statusKeywords) {
        if (containerText.toLowerCase().includes(kw)) {
          statusText = kw.toUpperCase();
          break;
        }
      }

      results.push({
        link: fullLink,
        title: title,
        priceText: priceText,
        bedrooms: bedrooms,
        statusText: statusText,
      });
    }

    return results;
  });

  logger.page(pageNum, label, `Found ${properties.length} properties`);

  // Process properties with strong validation
  const scraped = [];
  for (const prop of properties) {
    let url = prop.link;

    // Critical fix: Ensure url is a valid string
    if (!url || typeof url !== 'string') {
      logger.page(pageNum, label, `Skipped - invalid link type: ${typeof url}`);
      continue;
    }

    url = url.trim();
    if (!url.includes('/property/')) {
      logger.page(pageNum, label, `Skipped - malformed URL: ${url}`);
      continue;
    }

    if (isSoldProperty(prop.statusText)) {
      logger.property(prop.title.substring(0, 50), url, prop.priceText, prop.bedrooms, prop.statusText, 'SKIPPED');
      continue;
    }

    const price = parsePrice(prop.priceText);
    if (!price) {
      logger.page(pageNum, label, `Skipped - no valid price: ${prop.title}`);
      continue;
    }

    scraped.push({
      title: prop.title,
      url: url,
      price,
      priceDisplay: prop.priceText || '',
      bedrooms: prop.bedrooms ? parseInt(prop.bedrooms, 10) : null,
      statusText: prop.statusText,
      isRental,
    });
  }

  return {
    hasProperties: scraped.length > 0,
    properties: scraped
  };
}

async function handleDetailPage(page, propertyUrl) {
  try {
    const response = await page.goto(propertyUrl, { waitUntil: 'networkidle', timeout: 30000 }).catch(() => null);
    
    if (!response) {
      return { lat: null, lon: null, html: '' };
    }

    // Get page HTML
    const html = await page.content().catch(() => '');

    // Extract coordinates from JSON-LD
    const coords = await page.evaluate(() => {
      const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
      for (const script of scripts) {
        try {
          const data = JSON.parse(script.textContent);
          if (data?.geo?.latitude && data?.geo?.longitude) {
            return {
              lat: parseFloat(data.geo.latitude),
              lon: parseFloat(data.geo.longitude)
            };
          }
        } catch (e) { }
      }
      return { lat: null, lon: null };
    });

    return { ...coords, html };
  } catch (err) {
    return { lat: null, lon: null, html: '' };
  }
}

async function run() {
  const args = process.argv.slice(2);
  const startPage = args[0] ? parseInt(args[0], 10) : 1;

  logger.step(`Starting Agent ${AGENT_ID} - FreeAgent247`);
  logger.step(`Start Page: ${startPage}`);

  const scrapeStartTime = new Date();
  const ESTIMATED_TOTAL_PAGES = 8;

  const baseUrlSales = 'https://freeagent247.com/buy-property/?department=residential-sales&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&availability=4&property_type=';

  const initialRequests = [{
    url: baseUrlSales,
    userData: {
      pageNum: 1,
      totalPages: ESTIMATED_TOTAL_PAGES,
      isRental: false,
      label: 'Sales',
      baseUrl: baseUrlSales,
    },
  }];

  const crawler = new PlaywrightCrawler({
    maxConcurrency: 1,
    maxRequestRetries: 3,
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

    requestHandler: async ({ page, request }) => {
      const { pageNum, totalPages, isRental, label, baseUrl } = request.userData;

      logger.page(pageNum, label, `fetching`);

      const { hasProperties, properties } = await handleListingPage(page, pageNum, totalPages, isRental, label);

      for (const prop of properties) {
        try {
          const safeUrl = typeof prop.url === 'string' ? prop.url.trim() : '';

          if (!safeUrl) {
            logger.page(pageNum, label, `Skipped - invalid URL before DB: ${prop.url}`);
            continue;
          }

          const existing = await updatePriceByPropertyURLOptimized(
            safeUrl,
            prop.price,
            prop.title,
            prop.bedrooms,
            AGENT_ID,
            isRental
          );
          if (!existing.isExisting) {
            logger.property(prop.title, prop.url, prop.priceDisplay, prop.bedrooms, prop.statusText, 'CREATED');

            const detailCoords = await handleDetailPage(page, prop.url);

            await processPropertyWithCoordinates(
              prop.url,
              prop.price,
              prop.title,
              prop.bedrooms,
              AGENT_ID,
              isRental,
              detailCoords.html,
              detailCoords.lat,
              detailCoords.lon
            );

            await sleep(800);
          } else if (existing.updated) {
            logger.property(prop.title, prop.url, prop.priceDisplay, prop.bedrooms, prop.statusText, 'UPDATED');
            await sleep(200);
          } else {
            logger.property(prop.title, prop.url, prop.priceDisplay, prop.bedrooms, prop.statusText, 'UNCHANGED');
          }
        } catch (err) {
          logger.error(`Error processing property ${prop.title}: ${err.message}`);
          logger.property(prop.title, prop.url, prop.priceDisplay, prop.bedrooms, prop.statusText, 'ERROR');
        }
      }

      // Improved Next Page Logic
      if (hasProperties && pageNum < totalPages) {
        let nextPageUrl;

        if (baseUrl.includes('/page/')) {
          nextPageUrl = baseUrl.replace(/\/page\/\d+\/?/, `/page/${pageNum + 1}/`);
        } else {
          nextPageUrl = baseUrl.replace(/\/$/, '') + `/page/${pageNum + 1}/`;
        }

        // Preserve query parameters
        if (baseUrl.includes('?')) {
          const query = baseUrl.substring(baseUrl.indexOf('?'));
          nextPageUrl += query;
        }

        await crawler.addRequests([{
          url: nextPageUrl,
          userData: {
            pageNum: pageNum + 1,
            totalPages,
            isRental,
            label,
            baseUrl: baseUrl,
          },
        }]);

        logger.page(pageNum, label, `Queued next page ${pageNum + 1}`);
      }
    },

    errorHandler: async ({ request, error }) => {
      const { pageNum, label } = request.userData;
      logger.error(`Page ${pageNum} (${label}): ${error.message}`);
    },
  });

  try {
    if (startPage === 1) {
      logger.step('Full run detected - will cleanup removed properties at end');
    } else {
      logger.step('Partial run detected - skipping cleanup');
    }

    await crawler.run(initialRequests);

    if (startPage === 1) {
      await updateRemoveStatus(AGENT_ID, scrapeStartTime);
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