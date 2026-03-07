// Dixons Estate Agents scraper using Playwright with Crawlee
// Agent ID: 243
// Usage: node backend/scraper-agent-243.js

const { PlaywrightCrawler, log } = require("crawlee");

const { updateRemoveStatus } = require("./db.js");
const {
  formatPriceUk,
  updatePriceByPropertyURLOptimized,
} = require("./lib/db-helpers.js");

const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Inline sleep – fixes "sleep is not a function" reliably
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 243;
const logger = createAgentLogger(AGENT_ID);

const stats = {
  totalScraped: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// BROWSERLESS ENDPOINT
// ============================================================================

function getBrowserlessEndpoint() {
  return (
    process.env.BROWSERLESS_WS_ENDPOINT ||
    "ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
  );
}

// ============================================================================
// DETAIL PAGE SCRAPER
// ============================================================================

async function scrapePropertyDetail(context, property, isRental) {
  await sleep(800 + Math.random() * 700); // random delay 800–1500 ms

  const detailPage = await context.newPage();

  try {
    await detailPage.route("**/*", (route) => {
      const type = route.request().resourceType();
      if (["image", "font", "stylesheet", "media"].includes(type)) {
        route.abort();
      } else {
        route.continue();
      }
    });

    await detailPage.goto(property.link, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });

    const coords = await detailPage.evaluate(() => {
      const html = document.documentElement.innerHTML;
      const lat = html.match(/"latitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
      const lon = html.match(/"longitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
      return {
        lat: lat ? parseFloat(lat[1]) : null,
        lon: lon ? parseFloat(lon[1]) : null,
      };
    });

    return { latitude: coords.lat, longitude: coords.lon };
  } catch (err) {
    logger.error(`Detail failed → ${property.link}`, err.message || err);
    return null;
  } finally {
    await detailPage.close().catch(() => {});
  }
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
  const { pageNum, isRental, label } = request.userData;

  logger.page(pageNum, label, "Processing listing page...");

  await sleep(1500 + Math.random() * 1000);

  await page.waitForSelector(".card", { timeout: 25000 })
    .catch(() => logger.warn("No .card elements found – selectors may have changed", pageNum, label));

  const properties = await page.evaluate(() => {
    const cards = document.querySelectorAll(".card");
    const results = [];
    const baseUrl = window.location.origin;

    cards.forEach((card) => {
      try {
        const linkEl = card.querySelector("a.card__link");
        if (!linkEl) return;
        const rel = linkEl.getAttribute("href");
        if (!rel) return;
        const link = rel.startsWith("http") ? rel : `${baseUrl}${rel.startsWith("/") ? "" : "/"}${rel}`;

        const priceText = card.querySelector(".card__heading")?.innerText.trim() || "";
        const title = card.querySelector(".card__text-content")?.innerText.trim() || "Property";

        let bedrooms = null;
        const specs = card.querySelectorAll(".card-content__spec-list-item");
        specs.forEach((spec) => {
          if (spec.querySelector(".icon-bedroom")) {
            const val = spec.querySelector(".card-content__spec-list-number")?.innerText.trim();
            if (val) bedrooms = parseInt(val, 10);
          }
        });

        const statusText = card.innerText.trim().toLowerCase();

        results.push({ link, title, priceText, bedrooms, statusText });
      } catch {}
    });

    return results;
  });

  logger.step(`Found ${properties.length} properties on page ${pageNum}`, pageNum, label);

  const batchSize = 5;

  for (let i = 0; i < properties.length; i += batchSize) {
    const batch = properties.slice(i, i + batchSize);

    await Promise.all(
      batch.map(async (property) => {
        if (!property.link) return;

        if (isSoldProperty(property.statusText || "")) {
          logger.warn(`Skipping sold/let agreed → ${property.link}`, pageNum, label);
          return;
        }

        if (processedUrls.has(property.link)) {
          logger.warn(`Skipping duplicate → ${property.link}`, pageNum, label);
          return;
        }

        processedUrls.add(property.link);

        try {
          let actionTaken = "UNCHANGED";

          const priceNum = parsePrice(property.priceText);

          if (!priceNum || isNaN(priceNum)) {
            logger.warn(`Invalid/no price → ${property.link}`, pageNum, label);
            return;
          }

          const result = await updatePriceByPropertyURLOptimized(
            property.link.trim(),
            priceNum,
            property.title,
            property.bedrooms || null,
            AGENT_ID,
            isRental
          );

          if (result.updated) {
            actionTaken = "UPDATED";
            stats.totalSaved++;
          }

          if (!result.isExisting && !result.error) {
            logger.step(`Scraping new detail → ${property.title}`, pageNum, label);
            const detail = await scrapePropertyDetail(page.context(), property, isRental);

            // Update with coordinates
            await updatePriceByPropertyURL(
              property.link.trim(),
              priceNum,
              property.title,
              property.bedrooms || null,
              AGENT_ID,
              isRental,
              detail?.latitude || null,
              detail?.longitude || null
            );

            stats.totalSaved++;
            stats.totalScraped++;
            if (isRental) stats.savedRentals++;
            else stats.savedSales++;

            actionTaken = "CREATED";
          }

          logger.property(
            pageNum,
            label,
            property.title,
            formatPriceUk(priceNum),
            property.link,
            isRental,
            null,
            actionTaken
          );

          if (actionTaken === "CREATED") {
            await sleep(1200 + Math.random() * 800);
          }
        } catch (err) {
          logger.error(`Property processing failed → ${property.link}`, err.message || err, pageNum, label);
        }
      })
    );

    await sleep(500 + Math.random() * 500);
  }

  // Dynamic pagination (only enqueue if we found properties)
  if (properties.length > 0) {
    const nextPage = pageNum + 1;
    const type = isRental ? "lettings" : "sales";
    const nextUrl = `https://www.dixonsestateagents.co.uk/properties/${type}/status-available/most-recent-first/page-${nextPage}#/`;

    logger.step(`Enqueuing next page ${nextPage}: ${nextUrl}`, pageNum, label);

    await crawler.addRequests([{
      url: nextUrl,
      userData: { pageNum: nextPage, isRental, label },
    }]);
  }
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
  return new PlaywrightCrawler({
    maxConcurrency: 1,              // start low to avoid context closing issues
    maxRequestRetries: 3,
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 400,

    preNavigationHooks: [
      async ({ page }) => {
        await blockNonEssentialResources(page);
      },
    ],

    launchContext: {
      launchOptions: { browserWSEndpoint },
    },

    requestHandler: handleListingPage,

    failedRequestHandler({ request }) {
      logger.error(`Permanent failure: ${request.url}`);
    },
  });
}

// ============================================================================
// MAIN ENTRY POINT
// ============================================================================

async function scrapeDixons() {
  const scrapeStartTime = new Date();
  logger.step(`Starting Dixons scraper (Agent ${AGENT_ID})`);

  const browserWSEndpoint = getBrowserlessEndpoint();

  const crawler = createCrawler(browserWSEndpoint);

  // Queue both sales and lettings
  await crawler.addRequests([
    {
      url: "https://www.dixonsestateagents.co.uk/properties/sales/status-available/most-recent-first/page-1#/",
      userData: { pageNum: 1, isRental: false, label: "SALES" },
    },
    {
      url: "https://www.dixonsestateagents.co.uk/properties/lettings/status-available/most-recent-first/page-1#/",
      userData: { pageNum: 1, isRental: true, label: "LETTINGS" },
    },
  ]);

  await crawler.run();

  logger.step(`Completed – Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`);
  logger.step(`Breakdown → SALES: ${stats.savedSales} | LETTINGS: ${stats.savedRentals}`);

  await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
  try {
    await scrapeDixons();
    logger.step("All done!");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal error:", err?.message || err);
    process.exit(1);
  }
})();