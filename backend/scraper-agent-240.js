// Ashtons scraper using Playwright with Crawlee
// Agent ID: 240
// Usage: node backend/scraper-agent-240.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
  formatPriceUk,
  updatePriceByPropertyURLOptimized,
  processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Inline sleep
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 240;
const logger = createAgentLogger(AGENT_ID);

const stats = {
  totalScraped: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
};

const processedUrls = new Set();

function getBrowserlessEndpoint() {
  return (
    process.env.BROWSERLESS_WS_ENDPOINT ||
    "ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
  );
}

// ------------------------------------------------------------------
// DETAIL PAGE
// ------------------------------------------------------------------

async function scrapePropertyDetail(context, property, isRental) {
  await sleep(1000 + Math.random() * 1000);

  const detailPage = await context.newPage();

  try {
    await blockNonEssentialResources(detailPage);

    logger.step(`[Detail] ${property.title}`);

    await detailPage.goto(property.link, {
      waitUntil: "domcontentloaded",
      timeout: 40000,
    });

    const htmlContent = await detailPage.content();

    await processPropertyWithCoordinates(
      property.link,
      property.price,
      property.title,
      property.bedrooms || null,
      AGENT_ID,
      isRental,
      htmlContent
    );

    stats.totalScraped++;
    if (isRental) stats.savedRentals++;
    else stats.savedSales++;

    logger.step(`[Detail] Saved: ${property.title}`);
  } catch (err) {
    logger.error(`Detail failed → ${property.link}`, err.message || err);
  } finally {
    await detailPage.close().catch(() => {});
  }
}

// ------------------------------------------------------------------
// PROPERTY TYPES
// ------------------------------------------------------------------

const PROPERTY_TYPES = [
  {
    url: "https://www.ashtons.co.uk/buy?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
    isRental: false,
    label: "FOR SALE",
  },
  {
    url: "https://www.ashtons.co.uk/rent?location=&radius=0.5&min_price=&max_price=&min_bedrooms=&exclude_unavailable=on",
    isRental: true,
    label: "FOR LETTING",
  },
];

// ------------------------------------------------------------------
// MAIN SCRAPER
// ------------------------------------------------------------------

async function scrapeAshtons() {
  const scrapeStartTime = new Date();
  logger.step(`Starting Ashtons scraper (Agent ${AGENT_ID})...`);

  const browserWSEndpoint = getBrowserlessEndpoint();
  logger.step(`Browserless: ${browserWSEndpoint.split('?')[0]}`);

  const crawler = new PlaywrightCrawler({
    maxConcurrency: 1,
    maxRequestRetries: 3,
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 600,

    launchContext: {
      launchOptions: { browserWSEndpoint },
    },

    preNavigationHooks: [
      async ({ page }) => {
        await blockNonEssentialResources(page);
      },
    ],

    async requestHandler({ page, request }) {
      const { isRental, label } = request.userData;
      logger.step(`Processing ${label}`);

      await sleep(2000 + Math.random() * 1500);

      // Wait for any property-like content
      await page.waitForSelector('body', { timeout: 30000 }).catch(() => {});

      // ────────────────────────────────────────────────
      // INFINITE SCROLL SIMULATION (most likely mechanism)
      // ────────────────────────────────────────────────
      logger.step("Starting scroll to load all properties...");
      let previousHeight = 0;
      let scrollAttempts = 0;
      const maxScrolls = 50;

      while (scrollAttempts < maxScrolls) {
        await page.evaluate(() => window.scrollTo(0, document.body.scrollHeight));
        await sleep(3000 + Math.random() * 2000);

        const currentHeight = await page.evaluate(() => document.body.scrollHeight);
        if (currentHeight === previousHeight) {
          logger.step("No more content loaded (scroll height stable)");
          break;
        }
        previousHeight = currentHeight;
        scrollAttempts++;
        logger.step(`Scroll ${scrollAttempts}/${maxScrolls} - height: ${currentHeight}`);
      }

      // ────────────────────────────────────────────────
      // DEBUG: Screenshot + count potential cards
      // ────────────────────────────────────────────────
      const safeLabel = label.toLowerCase().replace(/\s+/g, '-');
      const screenshotPath = `ashtons-${safeLabel}-final.png`;
      await page.screenshot({ path: screenshotPath, fullPage: true });
      logger.step(`Screenshot saved: ${screenshotPath}`);

      const cardCount = await page.$$eval(
        '.c-property-card, [data-testid*="property"], .property-card, .listing-item, article, li',
        els => els.length
      );
      logger.step(`Detected ${cardCount} potential property card elements`);

      // ────────────────────────────────────────────────
      // EXTRACT PROPERTIES - resilient version
      // ────────────────────────────────────────────────
      const properties = await page.evaluate(() => {
        const items = [];

        // Try multiple card selectors in order of likelihood
        const cardSelectors = [
          '.c-property-card',
          '[data-testid*="property-card"]',
          '.property-card',
          '.listing-item',
          '.search-result',
          'article.property',
          'li.result',
          '.card'
        ];

        let cards = [];
        let usedSelector = '';

        for (const sel of cardSelectors) {
          const found = Array.from(document.querySelectorAll(sel));
          if (found.length > 0) {
            cards = found;
            usedSelector = sel;
            break;
          }
        }

        if (cards.length === 0) return [];

        cards.forEach(card => {
          try {
            // Link - most reliable signal
            const linkEl = card.querySelector('a[href*="/property/"], a[href*="/details/"], a.card-link, a.title-link');
            if (!linkEl) return;
            let href = linkEl.getAttribute('href');
            const link = href.startsWith('http') ? href : `https://www.ashtons.co.uk${href.startsWith('/') ? '' : '/'}${href}`;

            // Price
            let price = null;
            const pricePatterns = [
              card.querySelector('.price, .property-price, span.price-amount, .c-property-price__value'),
              ...card.querySelectorAll('span, div, p, strong')
            ].find(el => el?.textContent?.match(/£[\d,]+/));

            if (pricePatterns) {
              const text = pricePatterns.textContent.trim();
              const match = text.match(/£[\d,]+(\.?\d+)?/);
              if (match) price = match[0].replace(/[£,]/g, '');
            }

            // Title / Address
            const titleEl = card.querySelector('h3, h4, .title, .address, .property-title, strong');
            const title = titleEl ? titleEl.textContent.trim() : '';

            // Bedrooms
            let bedrooms = null;
            const bedMatch = card.innerText.match(/(\d+)\s*(bed|bedroom|beds)/i);
            if (bedMatch) bedrooms = bedMatch[1];

            if (link && title && price) {
              items.push({ link, title, price, bedrooms });
            }
          } catch {}
        });

        return items;
      });

      logger.step(`Extracted ${properties.length} valid properties`);

      // ────────────────────────────────────────────────
      // PROCESS PROPERTIES
      // ────────────────────────────────────────────────
      const batchSize = 5;
      for (let i = 0; i < properties.length; i += batchSize) {
        const batch = properties.slice(i, i + batchSize);

        await Promise.all(batch.map(async (property) => {
          if (processedUrls.has(property.link)) return;
          processedUrls.add(property.link);

          try {
            let actionTaken = "UNCHANGED";
            const priceNum = parsePrice(property.price);

            if (!priceNum) {
              logger.warn(`Bad price → ${property.link}`);
              return;
            }

            const result = await updatePriceByPropertyURLOptimized(
              property.link.trim(),
              priceNum,
              property.title,
              property.bedrooms,
              AGENT_ID,
              isRental
            );

            if (result.updated) {
              actionTaken = "UPDATED";
              stats.totalSaved++;
            }

            if (!result.isExisting && !result.error) {
              await scrapePropertyDetail(page.context(), { ...property, price: priceNum }, isRental);
              actionTaken = "CREATED";
            }

            logger.property(
              null,
              label,
              property.title,
              formatPriceUk(priceNum),
              property.link,
              isRental,
              null,
              actionTaken
            );

            if (actionTaken === "CREATED") {
              await sleep(1500 + Math.random() * 1000);
            }
          } catch (err) {
            logger.error(`Property failed → ${property.link}`, err.message || err);
          }
        }));

        await sleep(800 + Math.random() * 700);
      }
    },

    failedRequestHandler({ request }) {
      logger.error(`Failed: ${request.url}`);
    },
  });

  const initialRequests = PROPERTY_TYPES.map(type => ({
    url: type.url,
    userData: { isRental: type.isRental, label: type.label },
  }));

  await crawler.run(initialRequests);

  logger.step(`Finished - Total scraped: ${stats.totalScraped} | Saved: ${stats.totalSaved}`);
  logger.step(`Sales: ${stats.savedSales} | Lettings: ${stats.savedRentals}`);

  await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
  try {
    await scrapeAshtons();
    logger.step("Done!");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal:", err?.message || err);
    process.exit(1);
  }
})();