// HKL Home scraper using Playwright with Crawlee
// Agent ID: 239
// Usage: node backend/scraper-agent-239.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
  formatPriceUk,
  updatePriceByPropertyURLOptimized,
  processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources, sleep } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 239;
const logger = createAgentLogger(AGENT_ID);

const stats = {
  totalScraped: 0,
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
// PROPERTY TYPES
// ============================================================================

const PROPERTY_TYPES = [
  {
    urlBase: "https://www.hklhome.co.uk/search/",
    suffix:
      ".html?showstc=off&showsold=off&instruction_type=Sale&ajax_polygon=&minprice=&maxprice=&property_type=",
    isRental: false,
    label: "FOR SALE",
    typeIndex: 0,
  },
  {
    urlBase: "https://www.hklhome.co.uk/search/",
    suffix:
      ".html?showstc=off&showsold=off&instruction_type=Letting&ajax_polygon=&minprice=&maxprice=&property_type=",
    isRental: true,
    label: "FOR LETTING",
    typeIndex: 1,
  },
];

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(context, property, isRental, pageNum, label) {
  await sleep(800 + Math.random() * 700); // 800–1500 ms random delay

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
      htmlContent,
    );

    stats.totalSaved++;
    stats.totalScraped++;
    if (isRental) stats.savedRentals++;
    else stats.savedSales++;

    logger.property(pageNum, label, property.title, formatPriceUk(property.price), property.link, isRental, null, "CREATED");
  } catch (error) {
    logger.error(`Detail page failed → ${property.link}`, error.message, pageNum, label);
  } finally {
    await detailPage.close().catch(() => {});
  }
}

// ============================================================================
// MAIN SCRAPER
// ============================================================================

async function scrapeHKLHome() {
  const scrapeStartTime = new Date();
  logger.step(`Starting HKL Home scraper (Agent ${AGENT_ID})...`);

  const browserWSEndpoint = getBrowserlessEndpoint();
  logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

  const crawler = new PlaywrightCrawler({
    maxConcurrency: 1,              // Start conservative — increase later if stable
    maxRequestRetries: 2,
    requestHandlerTimeoutSecs: 180,
    navigationTimeoutSecs: 45,

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
      const { pageNum, isRental, label, typeIndex } = request.userData;
      logger.page(pageNum, label, "Processing listing page...");

      await sleep(1000 + Math.random() * 800);

      // Wait for listings
      await page.waitForSelector("a[href*='/property-details/']", { timeout: 20000 })
        .catch(() => logger.warn(`No property links found`, pageNum, label));

      const properties = await page.evaluate(() => {
        const items = [];
        const links = document.querySelectorAll("a[href*='/property-details/']");

        links.forEach((a) => {
          try {
            const href = a.getAttribute("href");
            if (!href) return;

            const link = href.startsWith("/") ? `https://www.hklhome.co.uk${href}` : href;

            // Find closest price element (usually h4 or strong after link)
            let priceEl = a.closest("div")?.querySelector("h4, strong, .price");
            if (!priceEl) {
              priceEl = a.parentElement?.nextElementSibling?.querySelector("h4, strong");
            }

            let priceText = priceEl ? priceEl.textContent.trim() : "";
            let priceRaw = priceText.match(/£[0-9,]+/)?.[0] || "";
            let price = priceRaw ? parseInt(priceRaw.replace(/[£,]/g, "")) : null;

            let status = priceText.toLowerCase().includes("sale") ? "For Sale" :
                         priceText.toLowerCase().includes("let")  ? "To Let"  : "";

            // Skip already sold/stc/let agreed
            if (/sold|stc|let\s*agreed|under offer/i.test(priceText)) return;

            // Title = full address from the link text
            const title = a.textContent.trim();

            // Bedrooms – look for pattern like * 4 * 2 * 1
            let bedrooms = null;
            const sibling = a.parentElement?.nextElementSibling || a.closest("div")?.nextElementSibling;
            if (sibling) {
              const text = sibling.textContent;
              const bedMatch = text.match(/\*\s*(\d+)/);
              if (bedMatch) bedrooms = bedMatch[1];
            }

            if (link && price && title) {
              items.push({ link, title, price, bedrooms, statusText: status + " " + priceText });
            }
          } catch (e) {}
        });

        return items;
      });

      logger.page(pageNum, label, `Found ${properties.length} valid properties`);

      // Enqueue next page if we found something
      if (properties.length >= 5) {   // reasonable threshold to assume more pages exist
        const propertyType = PROPERTY_TYPES[typeIndex];
        const nextUrl = `${propertyType.urlBase}${pageNum + 1}${propertyType.suffix}`;
        await crawler.addRequests([{
          url: nextUrl,
          userData: {
            pageNum: pageNum + 1,
            isRental,
            label,
            typeIndex,
          },
        }]);
      }

      // Process properties in small batches
      const batchSize = 4;
      for (let i = 0; i < properties.length; i += batchSize) {
        const batch = properties.slice(i, i + batchSize);

        await Promise.all(batch.map(async (property) => {
          if (processedUrls.has(property.link)) {
            logger.warn(`Already processed → skipping`, pageNum, label);
            return;
          }
          processedUrls.add(property.link);

          if (isSoldProperty(property.statusText || "")) {
            logger.warn(`Skipping sold/STC property`, pageNum, label);
            return;
          }

          try {
            let actionTaken = "UNCHANGED";

            const priceNum = property.price;

            const result = await updatePriceByPropertyURLOptimized(
              property.link.trim(),
              priceNum,
              property.title,
              property.bedrooms,
              AGENT_ID,
              isRental,
            );

            if (result.updated) {
              actionTaken = "UPDATED";
              stats.totalSaved++;
            }

            if (!result.isExisting && !result.error) {
              await scrapePropertyDetail(
                page.context(),
                { ...property, price: priceNum },
                isRental,
                pageNum,
                label
              );
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
              await sleep(1200 + Math.random() * 800);
            }
          } catch (err) {
            logger.error(`Property processing failed → ${property.link}`, err.message, pageNum, label);
          }
        }));

        await sleep(400 + Math.random() * 400); // small inter-batch delay
      }
    },

    failedRequestHandler({ request }) {
      logger.error(`Request permanently failed → ${request.url}`);
    },
  });

  // Start both sale & letting queues
  for (const propertyType of PROPERTY_TYPES) {
    logger.step(`Starting ${propertyType.label}`);
    const startUrl = `${propertyType.urlBase}1${propertyType.suffix}`;
    await crawler.addRequests([{
      url: startUrl,
      userData: {
        pageNum: 1,
        isRental: propertyType.isRental,
        label: propertyType.label,
        typeIndex: propertyType.typeIndex,
      },
    }]);
  }

  await crawler.run();

  logger.step(`Completed HKL Home scraper`);
  logger.step(`Total scraped: ${stats.totalScraped} | Total saved: ${stats.totalSaved}`);
  logger.step(`Breakdown → SALES: ${stats.savedSales} | LETTINGS: ${stats.savedRentals}`);

  await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}

(async () => {
  try {
    await scrapeHKLHome();
    logger.step("\nAll done!");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal error:", err?.message || err);
    process.exit(1);
  }
})();