// Dixons Estate Agents scraper using Playwright with Crawlee
// Agent ID: 243
// Usage: node backend/scraper-agent-243.js

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

const AGENT_ID = 243;
const logger = createAgentLogger(AGENT_ID);

const stats = {
  totalProcessed: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
  return (
    process.env.BROWSERLESS_WS_ENDPOINT ||
    "ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv"
  );
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(context, property, isRental, pageNum, label) {
  await sleep(1500 + Math.random() * 1000); // Polite delay

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
      timeout: 45000,
    });

    const htmlContent = await detailPage.content();

    const coords = await detailPage.evaluate(() => {
      const html = document.documentElement.innerHTML;
      const latMatch = html.match(/"latitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
      const lonMatch = html.match(/"longitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
      return {
        lat: latMatch ? parseFloat(latMatch[1]) : null,
        lon: lonMatch ? parseFloat(lonMatch[1]) : null,
      };
    });

    // CRITICAL: Insert the property into DB with coordinates
    await processPropertyWithCoordinates(
      property.link,
      property.price,
      property.title,
      property.bedrooms || null,
      AGENT_ID,
      isRental,
      htmlContent,
      coords.lat,
      coords.lon
    );

    stats.totalSaved++;
    if (isRental) stats.savedRentals++;
    else stats.savedSales++;

    logger.property(
      pageNum,
      label,
      property.title,
      formatPriceUk(property.price),
      property.link,
      isRental,
      coords.lat && coords.lon ? `${coords.lat.toFixed(5)}, ${coords.lon.toFixed(5)}` : null,
      "CREATED"
    );

    logger.step(`Coords found → lat=${coords.lat ?? 'null'}, lon=${coords.lon ?? 'null'} for ${property.title}`, pageNum, label);
  } catch (err) {
    logger.error(`Detail scrape failed → ${property.link}`, err.message || err, pageNum, label);
  } finally {
    await detailPage.close().catch(() => {});
  }
}

// ============================================================================
// PROPERTY TYPES
// ============================================================================

const PROPERTY_TYPES = [
  {
    url: "https://www.dixonsestateagents.co.uk/properties/sales/status-available/most-recent-first/page-1#/",
    isRental: false,
    label: "SALES",
  },
  {
    url: "https://www.dixonsestateagents.co.uk/properties/lettings/status-available/most-recent-first/page-1#/",
    isRental: true,
    label: "LETTINGS",
  },
];

// ============================================================================
// MAIN SCRAPER
// ============================================================================

async function scrapeDixons() {
  const scrapeStartTime = new Date();
  logger.step(`Starting Dixons scraper (Agent ${AGENT_ID})`);

  const browserWSEndpoint = getBrowserlessEndpoint();
  logger.step(`Connecting to browserless...`);

  const crawler = new PlaywrightCrawler({
    maxConcurrency: 1,
    maxRequestRetries: 3,
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 450,

    launchContext: {
      launchOptions: {
        browserWSEndpoint,
        args: ['--disable-blink-features=AutomationControlled'],
      },
    },

    preNavigationHooks: [
      async ({ page }) => {
        await blockNonEssentialResources(page);
      },
    ],

    async requestHandler({ page, request, crawler }) {
      const { pageNum = 1, isRental, label } = request.userData;

      logger.page(pageNum, label, request.url);

      await sleep(2000 + Math.random() * 1000);

      await page.waitForSelector(".card", { timeout: 30000 })
        .catch(() => logger.warn("No .card elements found – page may be empty", pageNum, label));

      const properties = await page.evaluate(() => {
        const cards = document.querySelectorAll(".card");
        const results = [];
        const baseUrl = window.location.origin;

        cards.forEach((card) => {
          try {
            const linkEl = card.querySelector("a.card__link");
            if (!linkEl) return;
            let href = linkEl.getAttribute("href");
            if (!href) return;
            // Canonicalize URL
            let link = href.startsWith("http") ? href : `${baseUrl}${href.startsWith("/") ? "" : "/"}${href}`;
            link = new URL(link).href; // Normalize (removes fragments, etc.)

            const priceText = card.querySelector(".card__heading")?.innerText?.trim() || "";
            const title = (card.querySelector(".card__text-content")?.innerText?.trim() || "Property at Dixons");

            let bedrooms = null;
            const specs = card.querySelectorAll(".card-content__spec-list-item");
            specs.forEach((spec) => {
              if (spec.querySelector(".icon-bedroom")) {
                const val = spec.querySelector(".card-content__spec-list-number")?.innerText?.trim();
                if (val) bedrooms = parseInt(val, 10);
              }
            });

            const statusText = card.innerText.toLowerCase();

            if (link && priceText) {
              results.push({ link, title, priceText, bedrooms, statusText });
            }
          } catch {}
        });

        return results;
      });

      logger.step(`Found ${properties.length} properties on page ${pageNum}`, pageNum, label);

      const batchSize = 3;

      for (let i = 0; i < properties.length; i += batchSize) {
        const batch = properties.slice(i, i + batchSize);

        await Promise.all(
          batch.map(async (property) => {
            if (!property.link) return;

            if (isSoldProperty(property.statusText || "")) {
              logger.property(pageNum, label, property.title, null, property.link, isRental, null, "SKIPPED_SOLD");
              return;
            }

            if (processedUrls.has(property.link)) {
              logger.property(pageNum, label, property.title, null, property.link, isRental, null, "DUPLICATE");
              return;
            }

            processedUrls.add(property.link);

            stats.totalProcessed++;

            try {
              let actionTaken = "UNCHANGED";

              // Dixons-specific price cleanup
              let priceTextClean = property.priceText
                .replace(/pcm/gi, '')
                .replace(/pw/gi, '')
                .replace(/guide price/gi, '')
                .replace(/offers (in )?excess of/gi, '')
                .replace(/offers over/gi, '')
                .replace(/from/gi, '')
                .replace(/poa/gi, '0')
                .replace(/[,£ ]/g, '');

              let priceNum = parseInt(priceTextClean, 10);
              if (isNaN(priceNum) || priceNum === 0) {
                priceNum = parsePrice(property.priceText); // Fallback to shared parser
              }

              logger.step(`Price debug → Raw: "${property.priceText}" | Clean: "${priceTextClean}" | Parsed: ${priceNum} (type: ${typeof priceNum})`, pageNum, label);

              if (!priceNum || isNaN(priceNum) || priceNum === 0) {
                logger.warn(`Invalid price → skipping ${property.link}`, pageNum, label);
                return;
              }

              const result = await updatePriceByPropertyURLOptimized(
                property.link,
                priceNum,
                property.title,
                property.bedrooms || null,
                AGENT_ID,
                isRental
              );

              logger.step(`DB result → isExisting: ${result.isExisting}, updated: ${result.updated}, error: ${result.error || 'none'}`, pageNum, label);

              if (result.updated) {
                actionTaken = "UPDATED";
                stats.totalSaved++;
              }

              if (!result.isExisting && !result.error) {
                logger.step(`New property → scraping detail for ${property.title}`, pageNum, label);
                await scrapePropertyDetail(page.context(), { ...property, price: priceNum }, isRental, pageNum, label);
                actionTaken = "CREATED";
              }

              const priceDisplay = formatPriceUk(priceNum);

              logger.property(
                pageNum,
                label,
                property.title,
                priceDisplay,
                property.link,
                isRental,
                null,
                actionTaken
              );

              // Conditional sleep only on CREATED
              if (actionTaken === "CREATED") {
                await sleep(2000 + Math.random() * 1000);
              }
            } catch (err) {
              logger.error(`Property processing failed → ${property.link}`, err.message || err, pageNum, label);
            }
          })
        );

        // Batch politeness
        if (i + batchSize < properties.length) {
          await sleep(1500 + Math.random() * 500);
        }
      }

      // Pagination: Enqueue next if properties were found (stop if empty page)
      if (properties.length > 0) {
        const nextPage = pageNum + 1;
        const type = isRental ? "lettings" : "sales";
        const nextUrl = `https://www.dixonsestateagents.co.uk/properties/${type}/status-available/most-recent-first/page-${nextPage}#/`;
        logger.step(`Enqueuing next page ${nextPage}: ${nextUrl}`, pageNum, label);
        await crawler.addRequests([{
          url: nextUrl,
          userData: { pageNum: nextPage, isRental, label },
        }]);
      } else {
        logger.step(`No more properties – stopping pagination`, pageNum, label);
      }
    },

    failedRequestHandler({ request }) {
      logger.error(`Request permanently failed → ${request.url}`);
    },
  });

  logger.step(`Queueing initial requests for SALES and LETTINGS`);

  await crawler.addRequests(
    PROPERTY_TYPES.map(type => ({
      url: type.url,
      userData: {
        pageNum: 1,
        isRental: type.isRental,
        label: type.label,
      },
    }))
  );

  await crawler.run();

  logger.step(`Completed Dixons scraper — Processed: ${stats.totalProcessed} | Saved: ${stats.totalSaved}`);
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