// Robert Holmes scraper (optimized for 401 bypass & baseline compliance)
// Agent ID: 78
// Usage: node backend/scraper-agent-78.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");

const { updateRemoveStatus } = require("./db.js");
const {
  updatePriceByPropertyURLOptimized,
  processPropertyWithCoordinates,
  formatPriceUk, // assuming you have this; fallback to formatPriceDisplay if needed
} = require("./lib/db-helpers.js");

const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 78;
const logger = createAgentLogger(AGENT_ID);

const stats = {
  totalProcessed: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
};

const processedUrls = new Set();

// Realistic headers to mimic real Chrome
const browserHeaders = {
  "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
  "accept-language": "en-GB,en-US;q=0.9,en;q=0.8",
  "sec-ch-ua": '"Google Chrome";v="120", "Chromium";v="120", "Not=A?Brand";v="24"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"Windows"',
  "sec-fetch-dest": "document",
  "sec-fetch-mode": "navigate",
  "sec-fetch-site": "none",
  "sec-fetch-user": "?1",
  "upgrade-insecure-requests": "1",
};

// Random lightweight UA rotation
const userAgents = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
];

function getRandomUA() {
  return userAgents[Math.floor(Math.random() * userAgents.length)];
}

// Sleep helper
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

const PROPERTY_TYPES = [
  {
    urlBase: "https://robertholmes.co.uk/search/",
    params: "address_keyword=&department=residential-sales&availability=2",
    totalPages: 10,
    isRental: false,
    label: "SALES",
  },
  {
    urlBase: "https://robertholmes.co.uk/search/",
    params: "address_keyword=&department=residential-lettings",
    totalPages: 10,
    isRental: true,
    label: "RENTALS",
  },
];

async function scrapePropertyDetail(browserContext, property, isRental, pageNum, label) {
  await sleep(1200 + Math.random() * 800);

  const detailPage = await browserContext.newPage();

  try {
    await detailPage.setExtraHTTPHeaders({
      ...browserHeaders,
      "user-agent": getRandomUA(),
    });

    await blockNonEssentialResources(detailPage);

    await detailPage.goto(property.link, {
      waitUntil: "domcontentloaded",
      timeout: 45000,
    });

    await sleep(1500); // give time for any lazy JS

    const htmlContent = await detailPage.content();

    const coords = await detailPage.evaluate(() => {
      const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
      for (const s of scripts) {
        try {
          const data = JSON.parse(s.textContent);
          if (data?.["@type"] === "GeoCoordinates" && data.latitude && data.longitude) {
            return {
              lat: parseFloat(data.latitude),
              lon: parseFloat(data.longitude),
            };
          }
        } catch {}
      }

      // Fallback regex
      const html = document.documentElement.innerHTML;
      const m1 = html.match(/"latitude"\s*:\s*([-+]?[0-9.]+).*?"longitude"\s*:\s*([-+]?[0-9.]+)/i);
      if (m1) return { lat: parseFloat(m1[1]), lon: parseFloat(m1[2]) };

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
      formatPriceUk?.(property.price) || property.price,
      property.link,
      isRental,
      null,
      "CREATED",
      coords?.lat,
      coords?.lon
    );

    return coords;
  } catch (err) {
    logger.error(`Detail failed → ${property.link}`, err.message || err, pageNum, label);
    return null;
  } finally {
    await detailPage.close().catch(() => {});
  }
}

async function handleListingPage({ page, request, crawler }) {
  const { pageNum, isRental, label, totalPages } = request.userData;

  logger.page(pageNum, label, request.url);

  // Apply headers
  await page.setExtraHTTPHeaders({
    ...browserHeaders,
    "user-agent": getRandomUA(),
  });

  await blockNonEssentialResources(page);

  try {
    await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });
  } catch (err) {
    logger.error(`Navigation failed → ${request.url}`, err.message, pageNum, label);
    return;
  }

  await page.waitForSelector(".grid-box-card", { timeout: 30000 }).catch(() => {
    logger.warn("No .grid-box-card found – possibly empty or blocked", pageNum, label);
  });

  const properties = await page.evaluate(() => {
    return Array.from(document.querySelectorAll(".grid-box-card"))
      .map(el => {
        const link = el.querySelector("a")?.href;
        if (!link) return null;

        const fullLink = link.startsWith("http") ? link : "https://robertholmes.co.uk" + link;

        const title = el.querySelector(".property-archive-title h4")?.textContent?.trim() || "Unknown";

        const priceText = el.querySelector(".property-archive-price")?.textContent?.trim() || "";

        let bedrooms = null;
        const bedLi = Array.from(el.querySelectorAll(".icons-list li")).find(li =>
          li.innerText.toLowerCase().includes("bed")
        );
        if (bedLi) {
          const match = bedLi.innerText.match(/\d+/);
          bedrooms = match ? parseInt(match[0], 10) : null;
        }

        return { link: fullLink, title, priceText, bedrooms, statusText: el.innerText.toLowerCase() };
      })
      .filter(Boolean);
  });

  logger.step(`Found ${properties.length} properties`, pageNum, label);

  if (properties.length === 0) {
    logger.step("Empty page – stopping further pagination for this type", pageNum, label);
    return; // prevents infinite enqueue
  }

  for (const property of properties) {
    if (processedUrls.has(property.link)) continue;
    processedUrls.add(property.link);

    stats.totalProcessed++;

    if (isSoldProperty(property.statusText)) {
      logger.property(pageNum, label, property.title, null, property.link, isRental, null, "SKIPPED_SOLD");
      continue;
    }

    const price = parsePrice(property.priceText);
    if (!price || price === 0) {
      logger.warn(`Invalid price → ${property.link}`, pageNum, label);
      continue;
    }

    const result = await updatePriceByPropertyURLOptimized(
      property.link,
      price,
      property.title,
      property.bedrooms || null,
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
      logger.step(`New → detail scrape ${property.title}`, pageNum, label);
      await scrapePropertyDetail(page.context(), { ...property, price }, isRental, pageNum, label);
      action = "CREATED";
    }

    logger.property(
      pageNum,
      label,
      property.title,
      formatPriceUk?.(price) || price,
      property.link,
      isRental,
      null,
      action
    );

    if (action === "CREATED") {
      await sleep(1500 + Math.random() * 800);
    }
  }

  // Pagination: only enqueue if we found properties
  if (properties.length > 0 && pageNum < totalPages) {
    const nextPage = pageNum + 1;
    const type = PROPERTY_TYPES.find(t => t.label === label);
    const nextUrl = nextPage === 1
      ? `${type.urlBase}?${type.params}`
      : `${type.urlBase}page/${nextPage}/?${type.params}`;

    logger.step(`Enqueue page ${nextPage}`, pageNum, label);
    await crawler.addRequests([{
      url: nextUrl,
      userData: { pageNum: nextPage, totalPages: type.totalPages, isRental, label },
    }]);
  }
}

function createCrawler(browserWSEndpoint) {
  return new PlaywrightCrawler({
    maxConcurrency: 1,
    maxRequestRetries: 3,
    navigationTimeoutSecs: 60,
    requestHandlerTimeoutSecs: 300,

    preNavigationHooks: [
      async ({ page }) => {
        await page.setExtraHTTPHeaders(browserHeaders);
        await blockNonEssentialResources(page);
      },
    ],

    launchContext: {
      launchOptions: {
        browserWSEndpoint,
        args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-blink-features=AutomationControlled"],
      },
    },

    requestHandler: handleListingPage,

    failedRequestHandler: ({ request, error }) => {
      logger.error(`Permanent fail → ${request.url} (${error?.message || 'unknown'})`);
    },
  });
}

async function scrapeRobertHolmes() {
  const scrapeStartTime = new Date();
  logger.step(`Starting Robert Holmes (Agent ${AGENT_ID})`);

  const startPage = Number(process.argv[2]) || 1;
  const isPartial = startPage > 1;

  const browserWSEndpoint = process.env.BROWSERLESS_WS_ENDPOINT ||
    "ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv";

  const crawler = createCrawler(browserWSEndpoint);

  const requests = [];

  for (const type of PROPERTY_TYPES) {
    const fromPage = Math.max(1, startPage);
    for (let p = fromPage; p <= type.totalPages; p++) {
      const url = p === 1
        ? `${type.urlBase}?${type.params}`
        : `${type.urlBase}page/${p}/?${type.params}`;

      requests.push({
        url,
        userData: {
          pageNum: p,
          totalPages: type.totalPages,
          isRental: type.isRental,
          label: type.label,
        },
      });
    }
  }

  if (requests.length === 0) {
    logger.step("Nothing to scrape");
    return;
  }

  await crawler.addRequests(requests);
  await crawler.run();

  logger.step(`Finished – Processed: ${stats.totalProcessed} | Saved: ${stats.totalSaved} (Sales: ${stats.savedSales} | Rentals: ${stats.savedRentals})`);

  if (!isPartial) {
    await updateRemoveStatus(AGENT_ID, scrapeStartTime);
  } else {
    logger.step(`Partial run (from page ${startPage}) – skipping remove status`);
  }
}

(async () => {
  try {
    await scrapeRobertHolmes();
    logger.step("Done");
    process.exit(0);
  } catch (err) {
    logger.error("Fatal", err?.message || err);
    process.exit(1);
  }
})();