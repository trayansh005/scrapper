// Balgores Property scraper using Playwright with Crawlee
// Agent ID: 254
// Updated 2026-03: Improved address extraction + very detailed geocoding debug logs
// Usage: node backend/scraper-agent-254.js [optional startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const axios = require("axios");
const { updateRemoveStatus } = require("./db.js");
const {
  updatePriceByPropertyURLOptimized,
  processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
  isSoldProperty,
  parsePrice,
  formatPriceDisplay,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.INFO); // Keep INFO to see geocoding attempts

const AGENT_ID = 254;
const logger = createAgentLogger(AGENT_ID);

const PROPERTY_TYPES = [
  {
    baseUrl: "https://www.balgoresproperty.co.uk/properties-for-sale/essex-and-kent/",
    totalPages: 20,
    isRental: false,
    label: "SALES",
  },
  {
    baseUrl: "https://www.balgoresproperty.co.uk/properties-to-rent/essex-and-kent/",
    totalPages: 15,
    isRental: true,
    label: "RENTALS",
  },
];

const counts = {
  totalScraped: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
};

const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function blockNonEssentialResources(page) {
  return page.route("**/*", (route) => {
    const resourceType = route.request().resourceType();
    if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
      return route.abort();
    }
    return route.continue();
  });
}

async function geocodeAddress(address, fallbackTitle = null) {
  let attempts = [address.trim()];

  if (fallbackTitle && fallbackTitle !== address && fallbackTitle.trim().length >= 8) {
    attempts.push(fallbackTitle.trim());
  }

  for (let i = 0; i < attempts.length; i++) {
    let addr = attempts[i].replace(/\s+/g, ' ').replace(/ ,/g, ',').trim();

    if (addr.length < 8) {
      logger.warn(`Geocode attempt ${i+1} skipped - too short: "${addr}"`);
      continue;
    }

    logger.info(`Geocode attempt ${i+1}/${attempts.length} → "${addr}"`);

    try {
      const query = encodeURIComponent(`${addr}, UK`);
      const url = `https://nominatim.openstreetmap.org/search?q=${query}&format=json&limit=1&countrycodes=gb`;

      const response = await axios.get(url, {
        headers: {
          "User-Agent": "BalgoresScraper/1.0 (your.real.email@example.com)", // ← CHANGE THIS
        },
        timeout: 15000,
      });

      const data = response.data;

      if (data?.length > 0 && data[0].lat && data[0].lon) {
        const lat = parseFloat(data[0].lat);
        const lon = parseFloat(data[0].lon);
        logger.info(`GEOCODE SUCCESS → "${addr}"  →  ${lat.toFixed(6)}, ${lon.toFixed(6)}`);
        return { latitude: lat, longitude: lon };
      }

      logger.warn(`No result from Nominatim for "${addr}"`);
    } catch (err) {
      logger.error(`Geocode failed for "${addr}": ${err.message}`);
    }

    // Small jitter to help with rate limiting
    await sleep(1100 + Math.floor(Math.random() * 400));
  }

  logger.warn(`All geocoding attempts failed for property`);
  return { latitude: null, longitude: null };
}

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
  return (
    process.env.BROWSERLESS_WS_ENDPOINT ||
    `ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
  );
}

// ============================================================================
// LISTING PAGE HANDLER – improved address collection
// ============================================================================

async function handleListingPage({ page, request }) {
  const { pageNum, isRental, label, totalPages } = request.userData;
  logger.page(pageNum, label, request.url, totalPages);

  try {
    await page.waitForSelector(
      "a[href*='/property-for-sale/'], a[href*='/property-to-rent/']",
      { timeout: 15000 }
    );
  } catch (e) {
    logger.error("Listing container not found", e, pageNum, label);
  }

  const properties = await page.evaluate(() => {
    try {
      const results = [];
      const seen = new Set();

      const links = Array.from(
        document.querySelectorAll("a[href*='/property-for-sale/'], a[href*='/property-to-rent/']")
      ).filter((a) => {
        const h = a.getAttribute("href");
        return h && !h.includes("#") && !h.includes("/branch/");
      });

      for (const a of links) {
        const href = a.getAttribute("href");
        if (seen.has(href)) continue;
        seen.add(href);

        let container = a;
        for (let i = 0; i < 8; i++) {
          container = container?.parentElement;
          if (!container || container.textContent.trim().length > 220) break;
        }
        if (!container) continue;

        const fullUrl = href.startsWith("http") ? href : new URL(href, window.location.origin).href;

        // Title
        const titleEl = container.querySelector("h2, h3, .title, .property-title, .address");
        const title = titleEl?.textContent?.trim() || "Unknown Property";

        // Collect all possible address fragments
        const addressFragments = new Set();

        const selectors = [
          ".address", ".property-address", ".location", ".property-location",
          ".address-line", ".postcode", ".town", ".county", "p", ".details p",
          ".info", ".property-info", ".description", ".meta"
        ];

        selectors.forEach(sel => {
          container.querySelectorAll(sel).forEach(el => {
            const txt = el.textContent?.trim();
            if (txt && txt.length > 4 && txt !== title) {
              addressFragments.add(txt);
            }
          });
        });

        // Also include title parts if they look like address
        title.split(',').forEach(part => {
          const t = part.trim();
          if (t.length > 5) addressFragments.add(t);
        });

        let address = Array.from(addressFragments)
          .join(", ")
          .replace(/,\s*,/g, ',')
          .replace(/\s+/g, ' ')
          .trim();

        if (!address || address.length < 12) {
          address = title;
        }

        let priceRaw = container.textContent.match(/£[\d,]+(?:[\.,]\d+)?/)?.[0] || "";
        let bedText = container.textContent.match(/(\d+)\s*(?:bed|bedroom|beds)/i)?.[0] || "";

        const statusText = container.textContent;

        results.push({
          link: fullUrl,
          title,
          address,
          priceRaw,
          bedText,
          statusText,
        });
      }

      return results;
    } catch (e) {
      console.error(e);
      return [];
    }
  });

  logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

  for (const prop of properties) {
    if (!prop.link) continue;

    if (isSoldProperty(prop.statusText || "")) continue;

    if (processedUrls.has(prop.link)) continue;
    processedUrls.add(prop.link);

    const price = parsePrice(prop.priceRaw);
    let bedrooms = null;
    if (prop.bedText) {
      const match = prop.bedText.match(/\d+/);
      if (match) bedrooms = parseInt(match[0], 10);
    }

    if (!price) {
      logger.page(pageNum, label, `Skipped: No price - ${prop.title.substring(0, 40)}`, totalPages);
      continue;
    }

    let coords = { latitude: null, longitude: null };

    const result = await updatePriceByPropertyURLOptimized(
      prop.link,
      price,
      prop.title,
      bedrooms,
      AGENT_ID,
      isRental
    );

    let propertyAction = "UNCHANGED";

    if (result.updated) {
      counts.totalSaved++;
      propertyAction = "UPDATED";
    }

    if (!result.isExisting && !result.error) {
      coords = await geocodeAddress(prop.address, prop.title);
      await sleep(1200 + Math.random() * 600); // 1.2–1.8s jitter

      await processPropertyWithCoordinates(
        prop.link.trim(),
        price,
        prop.title,
        bedrooms,
        AGENT_ID,
        isRental,
        null,
        coords.latitude,
        coords.longitude
      );

      counts.totalScraped++;
      counts.totalSaved++;
      if (isRental) counts.savedRentals++;
      else counts.savedSales++;
      propertyAction = "CREATED";
    } else if (result.error) {
      propertyAction = "ERROR";
    }

    logger.property(
      pageNum,
      label,
      prop.title.substring(0, 40),
      formatPriceDisplay(price, isRental),
      prop.link,
      isRental,
      totalPages,
      propertyAction,
      coords.latitude && coords.longitude
        ? `${coords.latitude.toFixed(6)}, ${coords.longitude.toFixed(6)}`
        : "NO_COORDS"
    );

    if (propertyAction !== "UNCHANGED") {
      await sleep(600);
    } else {
      await sleep(150);
    }
  }
}

// ============================================================================
// CRAWLER SETUP (unchanged)
// ============================================================================

function createCrawler(browserWSEndpoint) {
  return new PlaywrightCrawler({
    maxConcurrency: 1,
    maxRequestRetries: 2,
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 180,
    preNavigationHooks: [
      async ({ page }) => {
        await blockNonEssentialResources(page);
      },
    ],
    launchContext: {
      launchOptions: {
        browserWSEndpoint,
        args: ["--no-sandbox", "--disable-setuid-sandbox"],
        viewport: { width: 1920, height: 1080 },
      },
    },
    requestHandler: handleListingPage,
    failedRequestHandler({ request }) {
      logger.error(`Failed listing page: ${request.url}`);
    },
  });
}

// ============================================================================
// MAIN SCRAPER LOGIC (unchanged)
// ============================================================================

async function scrapeBalgoresProperty() {
  logger.step("Starting Balgores Property scraper (improved geocoding 2026-03)...");

  const args = process.argv.slice(2);
  const startPage = args.length > 0 ? parseInt(args[0], 10) || 1 : 1;
  const isPartialRun = startPage > 1;
  const scrapeStartTime = new Date();

  const browserWSEndpoint = getBrowserlessEndpoint();
  logger.step(`Connecting to Browserless: ${browserWSEndpoint.split("?")[0]}`);

  const crawler = createCrawler(browserWSEndpoint);

  const allRequests = [];
  for (const type of PROPERTY_TYPES) {
    logger.step(`Queueing ${type.label} (${type.totalPages} pages)`);
    for (let pg = Math.max(1, startPage); pg <= type.totalPages; pg++) {
      const pageParam = pg > 1 ? `?page=${pg}` : "";
      allRequests.push({
        url: `${type.baseUrl}${pageParam}`,
        userData: {
          pageNum: pg,
          isRental: type.isRental,
          label: type.label,
          totalPages: type.totalPages,
        },
      });
    }
  }

  if (allRequests.length > 0) {
    await crawler.run(allRequests);
  } else {
    logger.warn("No requests to process.");
  }

  logger.step(
    `Completed - Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved} ` +
      `(Sales: ${counts.savedSales}, Rentals: ${counts.savedRentals})`
  );

  if (!isPartialRun) {
    logger.step("Updating remove status...");
    await updateRemoveStatus(AGENT_ID, scrapeStartTime);
  } else {
    logger.warn("Partial run — skipping updateRemoveStatus.");
  }
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
  try {
    await scrapeBalgoresProperty();
    logger.step("All done!");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal error", err);
    process.exit(1);
  }
})();