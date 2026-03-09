// Galbraith Group scraper (Scotland-focused)
// Agent ID: 223
// Usage: node backend/scraper-agent-223.js [startPage]

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

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 223;
const logger = createAgentLogger(AGENT_ID);

const stats = {
  totalProcessed: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// CONFIG
// ============================================================================

const PROPERTY_TYPES = [
  {
    baseUrl: "https://www.galbraithgroup.com/sales-and-lettings/search/",
    params: "sq.BuyOrLet=true&sq.MaxDistance=30&sq.sq_stc=true&sq.Sort=newest",
    maxPages: 24,          // observed up to ~24 for sales
    isRental: false,
    label: "SALES",
  },
  {
    baseUrl: "https://www.galbraithgroup.com/sales-and-lettings/search/",
    params: "sq.BuyOrLet=false&sq.MaxDistance=30&sq.sq_stc=true&sq.Sort=newest",
    maxPages: 10,
    isRental: true,
    label: "RENTALS",
  },
];

// ============================================================================
// UTILS
// ============================================================================

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function buildPageUrl(base, params, pageNum, pageSize = 10) {
  const query = pageNum === 1
    ? params
    : `${params}&sq.Page=${pageNum}&sq.PageSize=${pageSize}`;
  return `${base}?${query}`;
}

// ============================================================================
// DETAIL SCRAPER (coords only for new properties)
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental, pageNum, label) {
  const detailPage = await browserContext.newPage();

  try {
    await detailPage.route("**/*", route => {
      const rt = route.request().resourceType();
      if (["image", "font", "stylesheet", "media"].includes(rt)) route.abort();
      else route.continue();
    });

    await detailPage.goto(property.link, {
      waitUntil: "domcontentloaded",
      timeout: 45000,
    });

    const htmlContent = await detailPage.content();

    const coords = await detailPage.evaluate(() => {
      const scripts = Array.from(document.querySelectorAll("script"));
      for (const s of scripts) {
        const txt = s.textContent || "";
        if (txt.includes("GeoCoordinates")) {
          const match = txt.match(/{\s*"@type"\s*:\s*"GeoCoordinates"[^}]*}/);
          if (match) {
            try {
              const geo = JSON.parse(match[0]);
              if (geo.latitude && geo.longitude) {
                return { lat: parseFloat(geo.latitude), lon: parseFloat(geo.longitude) };
              }
            } catch {}
          }
        }
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
// LISTING HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
  const { pageNum, isRental, label } = request.userData;

  logger.page(pageNum, label, request.url);

  await blockNonEssentialResources(page);

  await page.waitForSelector("div.property-information", { timeout: 30000 }).catch(() => {
    logger.warn("No property-information divs found – page empty/blocked?", pageNum, label);
  });

  const properties = await page.evaluate(() => {
    const results = [];
    const cards = document.querySelectorAll('div.property-information');  // top-level wrapper

    cards.forEach(card => {
      const titleEl = card.querySelector('h2 a.property-link');
      const title = titleEl?.textContent?.trim() || '';
      const link = titleEl?.href || '';

      if (!title || !link) return;

      const priceEl = card.querySelector('p.price .price-value');
      const priceValue = priceEl?.textContent?.trim() || '';

      // Full price context
      const fullPriceP = card.querySelector('p.price');
      const fullPriceText = fullPriceP?.textContent?.trim() || '';

      // Bedrooms
      const bedEl = card.querySelector('p.bedrooms');
      const bedroomsText = bedEl?.textContent?.trim() || '';
      const bedrooms = bedroomsText ? parseInt(bedroomsText.replace(/\D/g, ''), 10) : null;

      results.push({
        link,
        title,
        price: fullPriceText || priceValue,  // full context for parsing
        bedrooms,
      });
    });

    return results.filter(p => p.link && p.title && p.price);
  });

  logger.step(`Found ${properties.length} properties`, pageNum, label);

  if (properties.length === 0) return; // stop pagination if empty

  for (const prop of properties) {
    const fullLink = prop.link.startsWith("http") ? prop.link : `https://www.galbraithgroup.com${prop.link}`;

    if (processedUrls.has(fullLink)) continue;
    processedUrls.add(fullLink);

    stats.totalProcessed++;

    // Enhanced Galbraith-specific price extraction/cleanup
    let priceText = prop.price?.trim() || '';

    // 1. Prefer the inner <span class="price-value"> if present
    if (priceText.includes('£') && !priceText.includes('Offers Over')) {
      priceText = priceText; // already clean
    } else {
      // Remove common prefixes and keep only the numeric part
      priceText = priceText
        .replace(/Offers (Over|In Excess|Around)/gi, '')
        .replace(/Guide Price/gi, '')
        .replace(/Fixed Price/gi, '')
        .replace(/POA|Price On Application/gi, '0')
        .replace(/[^0-9.]/g, '');  // strip everything except digits and decimal
    }

    let priceNum = parseFloat(priceText);

    // 3. Fallback to shared parsePrice if cleanup failed
    if (isNaN(priceNum) || priceNum <= 0) {
      priceNum = parsePrice(prop.price);
    }

    if (!priceNum || priceNum <= 0) {
      logger.warn(`Price parse failed → Raw: "${prop.price}" | After cleanup: "${priceText}" → ${priceNum} for ${prop.title}`, pageNum, label);
      continue;
    }

    const result = await updatePriceByPropertyURLOptimized(
      fullLink,
      priceNum,
      prop.title,
      prop.bedrooms || null,
      AGENT_ID,
      isRental
    );

    let action = "UNCHANGED";

    if (result.updated) {
      action = "UPDATED";
      stats.totalSaved++;
      if (isRental) stats.savedRentals++;
      else stats.savedSales++;
    }

    if (!result.isExisting && !result.error) {
      logger.step(`New property → detail scrape ${prop.title}`, pageNum, label);
      await scrapePropertyDetail(page.context(), { ...prop, link: fullLink, price: priceNum }, isRental, pageNum, label);
      action = "CREATED";
    }

    logger.property(
      pageNum,
      label,
      prop.title,
      formatPriceUk(priceNum),
      fullLink,
      isRental,
      null,
      action
    );

    if (action === "CREATED") {
      await sleep(1200 + Math.random() * 800);
    }
  }

  // Pagination: enqueue next if properties found
  if (properties.length > 0) {
    const type = PROPERTY_TYPES.find(t => t.label === label);
    if (pageNum < type.maxPages) {
      const nextUrl = buildPageUrl(type.baseUrl, type.params, pageNum + 1);
      logger.step(`Enqueuing page ${pageNum + 1}`, pageNum, label);
      await crawler.addRequests([{
        url: nextUrl,
        userData: { pageNum: pageNum + 1, isRental, label },
      }]);
    }
  }
}

// ============================================================================
// CRAWLER
// ============================================================================

function createCrawler(browserWSEndpoint) {
  return new PlaywrightCrawler({
    maxConcurrency: 1,
    maxRequestRetries: 3,
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 300,

    preNavigationHooks: [
      async ({ page }) => {
        await blockNonEssentialResources(page);
      },
    ],

    launchContext: {
      launchOptions: {
        browserWSEndpoint,
        args: ["--no-sandbox", "--disable-setuid-sandbox"],
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

async function scrapeGalbraith() {
  const scrapeStartTime = new Date();
  logger.step(`Starting Galbraith scraper (Agent ${AGENT_ID})`);

  const startPage = Number(process.argv[2]) || 1;
  const isPartial = startPage > 1;

  const browserWSEndpoint = process.env.BROWSERLESS_WS_ENDPOINT ||
    "ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv";

  const crawler = createCrawler(browserWSEndpoint);

  const requests = [];

  for (const type of PROPERTY_TYPES) {
    const from = Math.max(1, startPage);
    for (let p = from; p <= type.maxPages; p++) {
      const url = buildPageUrl(type.baseUrl, type.params, p);
      requests.push({
        url,
        userData: { pageNum: p, isRental: type.isRental, label: type.label },
      });
    }
  }

  if (requests.length === 0) {
    logger.step("No pages queued");
    return;
  }

  await crawler.addRequests(requests);
  await crawler.run();

  logger.step(
    `Finished – Processed: ${stats.totalProcessed} | Saved: ${stats.totalSaved} ` +
    `(Sales: ${stats.savedSales} | Rentals: ${stats.savedRentals})`
  );

  if (!isPartial) {
    await updateRemoveStatus(AGENT_ID, scrapeStartTime);
  } else {
    logger.step(`Partial run from page ${startPage} → skipping remove status`);
  }
}

(async () => {
  try {
    await scrapeGalbraith();
    logger.step("Done");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal error", err?.message || err);
    process.exit(1);
  }
})();