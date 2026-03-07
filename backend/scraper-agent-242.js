// Fenn Wright scraper using Playwright with Crawlee
// Agent ID: 242
// Usage: node backend/scraper-agent-242.js

const { PlaywrightCrawler, log } = require("crawlee");
const cheerio = require("cheerio");

const { updateRemoveStatus } = require("./db.js");
const {
  updatePriceByPropertyURLOptimized,
  processPropertyWithCoordinates,
  formatPriceUk,
} = require("./lib/db-helpers.js");

const { parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Inline sleep function (fixes "sleep is not a function")
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 242;
const logger = createAgentLogger(AGENT_ID);

const stats = {
  totalScraped: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// PARSING LISTING PAGE WITH CHEERIO
// ============================================================================

function parseListingPage(html) {
  const $ = cheerio.load(html);
  const properties = [];

  $(".info-item").each((_, el) => {
    const linkEl = $(el).find("a.caption");
    const link = linkEl.attr("href");
    const title = $(el).find("h3").text().trim();
    const priceText = $(el).find(".price").text().trim();
    const price = parsePrice(priceText);

    let bedrooms = null;
    const bedText = $(el).find("figure").text().trim();
    const bedMatch = bedText.match(/(\d+)/);
    if (bedMatch) bedrooms = bedMatch[1];

    // Make sure link is absolute
    const fullLink = link ? (link.startsWith("http") ? link : `https://www.fennwright.co.uk${link}`) : null;

    if (fullLink && title && price) {
      properties.push({ link: fullLink, title, price, bedrooms });
    }
  });

  return properties;
}

// ============================================================================
// DETAIL PAGE SCRAPER
// ============================================================================

async function scrapePropertyDetail(context, property, isRental) {
  await sleep(800 + Math.random() * 700);

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
      timeout: 40000,
    });

    const htmlContent = await detailPage.content();

    const geo = await detailPage.evaluate(() => {
      const html = document.documentElement.innerHTML;
      const lat = html.match(/"latitude":\s*(-?\d+\.\d+)/i);
      const lon = html.match(/"longitude":\s*(-?\d+\.\d+)/i);
      return {
        lat: lat ? parseFloat(lat[1]) : null,
        lon: lon ? parseFloat(lon[1]) : null,
      };
    });

    await processPropertyWithCoordinates(
      property.link,
      property.price,
      property.title,
      property.bedrooms || null,
      AGENT_ID,
      isRental,
      htmlContent,
      geo.lat,
      geo.lon
    );

    stats.totalScraped++;
    stats.totalSaved++;
    if (isRental) stats.savedRentals++;
    else stats.savedSales++;
  } catch (err) {
    logger.error(`Detail scrape failed → ${property.link}`, err.message || err);
  } finally {
    await detailPage.close().catch(() => {});
  }
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
  const { isRental, label, pageNum } = request.userData;

  logger.page(pageNum, label, "Processing listing page...");

  await sleep(1200 + Math.random() * 800);

  await page.waitForSelector(".info-item", { timeout: 25000 }).catch(() => {
    logger.warn("No .info-item found – page may be empty or changed", pageNum, label);
  });

  const html = await page.content();
  const properties = parseListingPage(html);

  logger.step(`Found ${properties.length} properties on page ${pageNum}`, pageNum, label);

  const batchSize = 5;

  for (let i = 0; i < properties.length; i += batchSize) {
    const batch = properties.slice(i, i + batchSize);

    await Promise.all(
      batch.map(async (property) => {
        if (!property.link) return;

        if (processedUrls.has(property.link)) {
          logger.warn(`Skipping duplicate → ${property.link}`, pageNum, label);
          return;
        }

        processedUrls.add(property.link);

        try {
          let actionTaken = "UNCHANGED";

          const priceNum = parsePrice(property.price) || parseInt(property.price?.replace(/[^0-9]/g, ""), 10);

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
            logger.step(`Creating new property → ${property.title}`, pageNum, label);
            await scrapePropertyDetail(page.context(), { ...property, price: priceNum }, isRental);
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

          if (actionTaken === "CREATED") {
            await sleep(800 + Math.random() * 700);
          }
        } catch (err) {
          logger.error(`Property processing failed → ${property.link}`, err.message || err, pageNum, label);
        }
      })
    );

    await sleep(400 + Math.random() * 400);
  }

  // Pagination – look for next page link
  const nextButton = await page.$("a.next.page-numbers");

  if (nextButton) {
    const nextUrl = await nextButton.getAttribute("href");
    if (nextUrl) {
      const fullNextUrl = nextUrl.startsWith("http") ? nextUrl : `https://www.fennwright.co.uk${nextUrl}`;
      logger.step(`Enqueuing next page: ${fullNextUrl}`, pageNum, label);
      await crawler.addRequests([
        {
          url: fullNextUrl,
          userData: {
            isRental,
            label,
            pageNum: pageNum + 1,
          },
        },
      ]);
    }
  }
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
  return new PlaywrightCrawler({
    maxConcurrency: 2,
    maxRequestRetries: 3,
    navigationTimeoutSecs: 45,
    requestHandlerTimeoutSecs: 400,

    preNavigationHooks: [
      async ({ page }) => {
        await blockNonEssentialResources(page);
      },
    ],

    launchContext: {
      launchOptions: {
        browserWSEndpoint,
      },
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

async function scrapeFennWright() {
  const scrapeStartTime = new Date();
  logger.step(`Starting Fenn Wright scraper (Agent ${AGENT_ID})`);

  const browserWSEndpoint =
    process.env.BROWSERLESS_WS_ENDPOINT ||
    "ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv";

  const crawler = createCrawler(browserWSEndpoint);

  await crawler.addRequests([
    {
      url: "https://www.fennwright.co.uk/property-search/?department=residential-sales",
      userData: { isRental: false, label: "SALES", pageNum: 1 },
    },
    {
      url: "https://www.fennwright.co.uk/property-search/?department=residential-lettings",
      userData: { isRental: true, label: "RENTALS", pageNum: 1 },
    },
  ]);

  await crawler.run();

  logger.step(`Completed – Scraped: ${stats.totalScraped}, Saved: ${stats.totalSaved}`);
  logger.step(`Breakdown → Sales: ${stats.savedSales} | Rentals: ${stats.savedRentals}`);

  await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
  try {
    await scrapeFennWright();
    logger.step("All done!");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal error:", err?.message || err);
    process.exit(1);
  }
})();