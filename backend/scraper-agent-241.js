// Nestseekers scraper (United Kingdom section)
// Agent ID: 241
// Usage: node backend/scraper-agent-241.js  [startPage]

const { PlaywrightCrawler, log } = require("crawlee");

const { updateRemoveStatus } = require("./db.js");
const {
  updatePriceByPropertyURLOptimized,
  processPropertyWithCoordinates,
  formatPriceUk,
} = require("./lib/db-helpers.js");

const { parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const cheerio = require("cheerio");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 241;
const logger = createAgentLogger(AGENT_ID);

const stats = {
  totalProcessed: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
};

const processedUrls = new Set(); // light duplicate protection per run

// ============================================================================
// SHARED UTILITIES
// ============================================================================

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizeUrl(href) {
  if (!href) return null;
  return href.startsWith("http") ? href : `https://www.nestseekers.com${href}`;
}

// ============================================================================
// LISTING PAGE PARSER (Cheerio)
// ============================================================================

function parseListingPage(html) {
  const $ = cheerio.load(html);
  const properties = [];

  $("tr[id]").each((_, el) => {
    const $row = $(el);

    const $link = $row.find("a[href]").first();
    const href = $link.attr("href");
    const link = normalizeUrl(href);
    if (!link) return;

    let title = $row.find("a strong").text().trim();
    const address = $row.find("h2").text().trim();
    if (address) title = `${title} - ${address}`.replace(/\s+/g, " ");

    // Price
    let priceText = $row.find(".price").text().trim();
    if (!priceText) priceText = $row.find(".p-4.text-center").first().text().trim();

    const price = parsePrice(priceText); // shared helper
    if (!price || price === "0") return; // skip POA / no price

    // Bedrooms
    let bedrooms = null;
    const info = $row.find(".info .tight").text().trim();
    const bedMatch = info.match(/(\d+)\+?\s*(?:BR|bedroom|beds?)/i);
    if (bedMatch) bedrooms = bedMatch[1];

    properties.push({ link, title, price, bedrooms });
  });

  return properties;
}

// ============================================================================
// DETAIL PAGE SCRAPER
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental, pageNum, label) {
  const detailPage = await browserContext.newPage();

  try {
    await detailPage.route("**/*", (route) => {
      const rt = route.request().resourceType();
      if (["image", "font", "stylesheet", "media"].includes(rt)) {
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

    const coords = await detailPage.evaluate(() => {
      try {
        const geoEl = document.querySelector("#mapWrap[geo]");
        if (!geoEl) return null;

        let geo = geoEl.getAttribute("geo");
        if (!geo) return null;

        geo = geo
          .replace(/&quot;/g, '"')
          .replace(/&amp;quot;/g, '"')
          .replace(/&amp;/g, "&");

        const data = JSON.parse(geo);
        const lat = data?.lat ?? data?.latitude;
        const lon = data?.lon ?? data?.lng ?? data?.longitude;

        if (lat && lon) return { lat: parseFloat(lat), lon: parseFloat(lon) };
      } catch {
        // silent
      }
      return null;
    });

    await processPropertyWithCoordinates(
      property.link,
      property.price,
      property.title,
      property.bedrooms || null,
      AGENT_ID,
      isRental,
      htmlContent,
      coords?.lat ?? null,
      coords?.lon ?? null
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
      coords ? `${coords.lat?.toFixed(5)}, ${coords.lon?.toFixed(5)}` : null,
      "CREATED"
    );
  } catch (err) {
    logger.error(`Detail failed → ${property.link}`, err.message || err, pageNum, label);
  } finally {
    await detailPage.close().catch(() => {});
  }
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
  const { pageNum, isRental, label } = request.userData;

  logger.page(pageNum, label, request.url);

  await blockNonEssentialResources(page);

  await page.waitForSelector("tr[id]", { timeout: 30000 }).catch(() => {});

  const html = await page.content();
  const properties = parseListingPage(html);

  logger.step(`Found ${properties.length} properties`, pageNum, label);

  for (const property of properties) {
    if (processedUrls.has(property.link)) {
      logger.property(pageNum, label, property.title, null, property.link, isRental, null, "DUPLICATE");
      continue;
    }
    processedUrls.add(property.link);

    stats.totalProcessed++;

    const priceNum = parsePrice(property.price);
    if (!priceNum) {
      logger.warn(`Invalid price skipped → ${property.link}`, pageNum, label);
      continue;
    }

    const result = await updatePriceByPropertyURLOptimized(
      property.link,
      priceNum,
      property.title,
      property.bedrooms || null,
      AGENT_ID,
      isRental
    );

    let action = "UNCHANGED";

    if (result.updated) {
      action = "UPDATED";
      stats.totalSaved++;
    }

    if (!result.isExisting && !result.error) {
      logger.step(`New property → ${property.title}`, pageNum, label);
      await scrapePropertyDetail(page.context(), { ...property, price: priceNum }, isRental, pageNum, label);
      action = "CREATED";
    }

    logger.property(
      pageNum,
      label,
      property.title,
      formatPriceUk(priceNum),
      property.link,
      isRental,
      null,
      action
    );

    // Politeness: only slow down when we actually created something new
    if (action === "CREATED") {
      await sleep(1200 + Math.random() * 800);
    }
  }

  // Simple next-page detection (can be improved if pagination is stable)
  const nextLink = await page.$('a[href*="?page="]:has-text("Next")');
  if (nextLink) {
    const nextHref = await nextLink.getAttribute("href");
    const nextUrl = normalizeUrl(nextHref);
    if (nextUrl) {
      logger.step(`Enqueuing next → ${nextUrl}`, pageNum, label);
      await crawler.addRequests([
        {
          url: nextUrl,
          userData: { pageNum: pageNum + 1, isRental, label },
        },
      ]);
    }
  }
}

// ============================================================================
// CRAWLER FACTORY
// ============================================================================

function createCrawler(browserWSEndpoint) {
  return new PlaywrightCrawler({
    maxConcurrency: 2,
    maxRequestRetries: 2,
    navigationTimeoutSecs: 45,
    requestHandlerTimeoutSecs: 300,

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
      logger.error(`Permanent failure → ${request.url}`);
    },
  });
}

// ============================================================================
// MAIN
// ============================================================================

async function runNestseekers() {
  const scrapeStartTime = new Date();
  logger.step(`Starting Nestseekers UK scraper (Agent ${AGENT_ID})`);

  const startPage = Number(process.argv[2]) || 1;

  const browserWSEndpoint =
    process.env.BROWSERLESS_WS_ENDPOINT ||
    "ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv";

  const crawler = createCrawler(browserWSEndpoint);

  const initialRequests = [];

  // Sales
  for (let p = Math.max(1, startPage); p <= 15; p++) {
    const url =
      p === 1
        ? "https://www.nestseekers.com/Sales/united-kingdom/"
        : `https://www.nestseekers.com/Sales/united-kingdom/?page=${p}`;
    initialRequests.push({
      url,
      userData: { pageNum: p, isRental: false, label: "SALES" },
    });
  }

  // Rentals (only from page 1 unless --startPage forces partial run)
  if (startPage <= 1) {
    for (let p = 1; p <= 10; p++) {
      const url =
        p === 1
          ? "https://www.nestseekers.com/Rentals/united-kingdom/"
          : `https://www.nestseekers.com/Rentals/united-kingdom/?page=${p}`;
      initialRequests.push({
        url,
        userData: { pageNum: p, isRental: true, label: "RENTALS" },
      });
    }
  }

  if (initialRequests.length === 0) {
    logger.step("No pages to scrape (startPage too high?)");
    return;
  }

  await crawler.addRequests(initialRequests);
  await crawler.run();

  logger.step(
    `Finished — Processed: ${stats.totalProcessed} | Saved: ${stats.totalSaved} ` +
      `(Sales: ${stats.savedSales} | Rentals: ${stats.savedRentals})`
  );

  // Only run remove-status when we scraped from page 1 (full run protection)
  if (startPage <= 1) {
    await updateRemoveStatus(AGENT_ID, scrapeStartTime);
  } else {
    logger.step(`Skipping updateRemoveStatus (partial run from page ${startPage})`);
  }
}

(async () => {
  try {
    await runNestseekers();
    logger.step("Done.");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal error", err?.message || err);
    process.exit(1);
  }
})();