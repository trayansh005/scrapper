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

// Inline sleep function (no dependency on scraper-utils sleep)
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
// DETAIL PAGE SCRAPE
// ------------------------------------------------------------------

async function scrapePropertyDetail(context, property, isRental) {
  await sleep(800 + Math.random() * 700); // 800–1500 ms

  const detailPage = await context.newPage();

  try {
    await blockNonEssentialResources(detailPage);

    logger.step(`[Detail] Scraping: ${property.title}`);

    await detailPage.goto(property.link, {
      waitUntil: "domcontentloaded",
      timeout: 35000,
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

    logger.step(`[Detail] Done: ${property.title}`);
  } catch (err) {
    logger.error(`Detail page error → ${property.link}`, err.message || err);
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
  logger.step(`Connecting to browserless...`);

  const crawler = new PlaywrightCrawler({
    maxConcurrency: 1,              // start safe – increase to 2 later if stable
    maxRequestRetries: 2,
    navigationTimeoutSecs: 45,
    requestHandlerTimeoutSecs: 300,

    launchContext: {
      launchOptions: {
        browserWSEndpoint,
      },
    },

    preNavigationHooks: [
      async ({ page }) => {
        await blockNonEssentialResources(page);
      },
    ],

    async requestHandler({ page, request }) {
      const { isRental, label } = request.userData;

      logger.step(`Processing ${label} page`);

      await sleep(1500 + Math.random() * 1000);

      // Wait for property cards
      await page.waitForSelector(".c-property-card", { timeout: 20000 })
        .catch(() => logger.warn("No property cards detected"));

      // ----------------------------------------------------
      // CLICK "SHOW MORE" until no more button or disabled
      // ----------------------------------------------------
      let clickCount = 0;
      const maxClicks = 60;

      while (clickCount < maxClicks) {
        const showMoreButton = await page.$(
          ".c-property-search__list-action button.c-button--tertiary"
        );

        if (!showMoreButton) {
          logger.step("No more 'Show More' button found");
          break;
        }

        const isDisabled = await showMoreButton.evaluate(el => el.disabled || !el.offsetParent);
        if (isDisabled) {
          logger.step("Show More button is disabled");
          break;
        }

        logger.step(`Clicking Show More (${clickCount + 1}/${maxClicks})`);

        await showMoreButton.click().catch(() => {});

        await sleep(2000 + Math.random() * 1000); // wait for new content
        clickCount++;
      }

      logger.step(`Finished loading – ${clickCount} clicks performed`);

      // ----------------------------------------------------
      // EXTRACT PROPERTIES
      // ----------------------------------------------------
      const properties = await page.evaluate(() => {
        const cards = Array.from(document.querySelectorAll(".c-property-card"));

        return cards.map(card => {
          try {
            const linkEl = card.querySelector("a.c-property-card__anchor");
            if (!linkEl) return null;

            const href = linkEl.getAttribute("href");
            if (!href) return null;

            const link = href.startsWith("/")
              ? `https://www.ashtons.co.uk${href}`
              : href;

            const priceEl = card.querySelector(".c-property-price__value");
            let price = null;
            if (priceEl) {
              const text = priceEl.textContent.trim();
              const match = text.match(/£[\d,]+/);
              if (match) price = match[0].replace(/[£,]/g, "");
            }

            const titleEl = card.querySelector(".c-property-card__title");
            const title = titleEl ? titleEl.textContent.trim() : "";

            const bedEl = card.querySelector(
              ".c-property-feature--bedrooms .c-property-feature__value"
            );
            let bedrooms = null;
            if (bedEl) {
              const match = bedEl.textContent.match(/\d+/);
              if (match) bedrooms = match[0];
            }

            if (!link || !title || !price) return null;

            return { link, title, price, bedrooms };
          } catch (e) {
            return null;
          }
        }).filter(Boolean);
      });

      logger.step(`Found ${properties.length} properties`);

      // ----------------------------------------------------
      // PROCESS PROPERTIES IN BATCHES
      // ----------------------------------------------------
      const batchSize = 5;

      for (let i = 0; i < properties.length; i += batchSize) {
        const batch = properties.slice(i, i + batchSize);

        await Promise.all(batch.map(async (property) => {
          if (processedUrls.has(property.link)) {
            return;
          }
          processedUrls.add(property.link);

          try {
            let actionTaken = "UNCHANGED";

            const priceNum = parsePrice(property.price);

            if (!priceNum) {
              logger.warn(`Invalid price → ${property.link}`);
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
              await scrapePropertyDetail(
                page.context(),
                { ...property, price: priceNum },
                isRental
              );
              actionTaken = "CREATED";
            }

            const priceDisplay = formatPriceUk(priceNum);

            logger.property(
              null,                    // no page number
              label,
              property.title,
              priceDisplay,
              property.link,
              isRental,
              null,
              actionTaken
            );

            if (actionTaken === "CREATED") {
              await sleep(1200 + Math.random() * 800);
            }

          } catch (err) {
            logger.error(`Property processing failed → ${property.link}`, err.message || err);
          }
        }));

        await sleep(600 + Math.random() * 600); // small batch delay
      }
    },

    failedRequestHandler({ request }) {
      logger.error(`Request failed permanently: ${request.url}`);
    },
  });

  const initialRequests = PROPERTY_TYPES.map(type => ({
    url: type.url,
    userData: {
      isRental: type.isRental,
      label: type.label,
    },
  }));

  await crawler.run(initialRequests);

  // Final summary
  logger.step(`Agent ${AGENT_ID} finished`);
  logger.step(`Total scraped: ${stats.totalScraped}`);
  logger.step(`Total saved:   ${stats.totalSaved}`);
  logger.step(`Sales:         ${stats.savedSales}`);
  logger.step(`Rentals:       ${stats.savedRentals}`);

  await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

// ------------------------------------------------------------------
// RUN
// ------------------------------------------------------------------

(async () => {
  try {
    await scrapeAshtons();
    logger.step("All done!");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal error:", err?.message || err);
    process.exit(1);
  }
})();