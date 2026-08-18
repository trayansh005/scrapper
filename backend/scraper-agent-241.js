// Nest Seekers UK Scraper (CheerioCrawler + Embedded Geo/API extraction)
// Agent ID: 241
// Usage: node backend/scraper-agent-241.js [startPage]

"use strict";

const { CheerioCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
  updatePriceByPropertyURLOptimized,
  processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
  parsePrice,
  formatPriceUk,
  isSoldProperty,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 241;
const logger = createAgentLogger(AGENT_ID);

const counts = {
  totalScraped: 0,
  totalSaved: 0,
  savedSales: 0,
  savedRentals: 0,
  totalSkipped: 0,
};

const processedUrls = new Set();
const scrapeStartTime = new Date();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
  const val = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
  if (!Number.isFinite(val) || val < 1) return 1;
  return Math.floor(val);
}

const startPage = getStartPage();
const isPartialRun = startPage > 1;

function getPageUrl(isRental, pageNum) {
  const section = isRental ? "Rentals" : "Sales";
  return `https://www.nestseekers.com/${section}/united-kingdom/?page=${pageNum}`;
}

const PROPERTY_TYPES = [
  {
    label: "SALES",
    isRental: false,
    baseUrl: getPageUrl(false, startPage),
  },
  {
    label: "RENTALS",
    isRental: true,
    baseUrl: getPageUrl(true, startPage),
  },
];

// ============================================================================
// DETAIL PAGE FETCH FALLBACK (If coordinates / bedrooms missing)
// ============================================================================

async function fetchDetailInfo(propertyId, url) {
  // Try fast JSON API first
  try {
    const apiRes = await fetch(
      `https://www.nestseekers.com/api/public/listings/listing-detail?web_id=${propertyId}`,
      {
        headers: {
          "User-Agent":
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
          Accept: "application/json, text/plain, */*",
        },
      }
    );
    if (apiRes.ok) {
      const data = await apiRes.json();
      if (data && (data.lat || data.lng || data.beds)) {
        return {
          lat: data.lat ? parseFloat(data.lat) : null,
          lng: data.lng ? parseFloat(data.lng) : null,
          bedrooms: data.beds ? parseInt(data.beds, 10) : null,
          html: null,
        };
      }
    }
  } catch (e) {
    // silent fallback
  }

  // Fallback to HTML fetch
  try {
    const htmlRes = await fetch(url, {
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
      },
    });
    if (htmlRes.ok) {
      const html = await htmlRes.text();
      return { lat: null, lng: null, bedrooms: null, html };
    }
  } catch (err) {
    logger.error(`Failed detail fallback for ${url}: ${err.message}`);
  }

  return { lat: null, lng: null, bedrooms: null, html: null };
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================

async function handleListingPage({ $, request, crawler }) {
  const { pageNum, label, isRental } = request.userData;

  logger.page(pageNum, label, `Processing ${request.url}`);

  let totalPages = request.userData.totalPages || null;

  // Extract total results and calculate totalPages on initial page
  if (pageNum === startPage || !totalPages) {
    let totalCount = null;
    $("div, p, span, h1, h2").each((_, el) => {
      const text = $(el).text().trim();
      const m = text.match(/(\d+)\s+found/i);
      if (m && !totalCount) {
        totalCount = parseInt(m[1], 10);
      }
    });

    const pageNumbers = [];
    $('a[href*="page="]').each((_, el) => {
      const href = $(el).attr("href");
      const m = href.match(/page=(\d+)/);
      if (m) pageNumbers.push(parseInt(m[1], 10));
    });

    const maxPageFromLinks =
      pageNumbers.length > 0 ? Math.max(...pageNumbers) : 1;
    totalPages = totalCount ? Math.ceil(totalCount / 39) : maxPageFromLinks;

    logger.page(
      pageNum,
      label,
      `Found ~${totalCount || "?"} total properties across ${totalPages} pages.`,
      totalPages
    );
  }

  // Extract coordinate map from inline script
  const coordsMap = new Map();
  $("script").each((_, el) => {
    const text = $(el).html() || "";
    const trimmed = text.trim();
    if (trimmed.startsWith('[{"id":') && trimmed.endsWith("}]")) {
      try {
        const items = JSON.parse(trimmed);
        for (const item of items) {
          if (item.id) {
            coordsMap.set(String(item.id), {
              lat: item.lat ? parseFloat(item.lat) : null,
              lng: item.lng ? parseFloat(item.lng) : null,
            });
          }
        }
      } catch (e) {}
    }
  });

  const itemsToProcess = [];
  const seenOnPage = new Set();

  $("a[href]").each((_, el) => {
    const $link = $(el);
    const href = $link.attr("href");
    if (!href) return;

    const match = href.match(
      /(?:https:\/\/www\.nestseekers\.com)?\/(\d+)\/([^/?#]+)/
    );
    if (!match) return;

    const propertyId = match[1];
    const fullUrl = href.startsWith("http")
      ? href
      : `https://www.nestseekers.com${href}`;

    if (seenOnPage.has(fullUrl)) return;
    seenOnPage.add(fullUrl);

    // Title
    let title = "";
    const ariaLabel = $link.attr("aria-label");
    if (ariaLabel && ariaLabel.includes("View details for ")) {
      title = ariaLabel.replace("View details for ", "").trim();
    }
    if (!title) {
      title = $link.find("[title]").first().attr("title") || "";
    }
    if (!title) {
      title = $link
        .find(".text-lg.font-semibold, .text-base.font-semibold")
        .first()
        .text()
        .trim();
    }

    // Price
    let priceText = "";
    $link.find(".font-semibold").each((_, elem) => {
      const t = $(elem).text().trim();
      if (
        t.includes("£") ||
        t.includes("$") ||
        t.includes("€") ||
        t.toLowerCase().includes("request")
      ) {
        const gbpMatch = t.match(/£[\d,.]+/);
        if (gbpMatch) {
          priceText = gbpMatch[0];
        } else if (!priceText) {
          priceText = t.split(/[\(\n]/)[0].trim();
        }
      }
    });

    const parsedPrice = parsePrice(priceText);
    const finalPrice = parsedPrice !== null ? parsedPrice : "POA";

    // Bedrooms
    let bedrooms = null;
    const cardText = $link.text();
    const bedMatch =
      cardText.match(/(\d+)\s*(?:bed|bedroom|br)/i) ||
      fullUrl.match(/(\d+)-bed/i);
    if (bedMatch) {
      bedrooms = parseInt(bedMatch[1], 10);
    }

    const coords = coordsMap.get(propertyId);

    itemsToProcess.push({
      id: propertyId,
      link: fullUrl,
      title: title || "Property",
      price: finalPrice,
      bedrooms,
      lat: coords?.lat ?? null,
      lng: coords?.lng ?? null,
      statusText: cardText,
    });
  });

  if (itemsToProcess.length === 0) {
    logger.page(
      pageNum,
      label,
      `No properties found on page ${pageNum}. Ending pagination.`,
      totalPages
    );
    return;
  }

  logger.page(
    pageNum,
    label,
    `Found ${itemsToProcess.length} properties on page ${pageNum}`,
    totalPages
  );

  for (const prop of itemsToProcess) {
    if (isSoldProperty(prop.statusText || "")) {
      counts.totalSkipped++;
      logger.property(
        pageNum,
        label,
        prop.title,
        prop.price,
        prop.link,
        isRental,
        totalPages,
        "SKIPPED"
      );
      continue;
    }

    if (processedUrls.has(prop.link)) {
      counts.totalSkipped++;
      continue;
    }
    processedUrls.add(prop.link);
    counts.totalScraped++;

    try {
      const result = await updatePriceByPropertyURLOptimized(
        prop.link,
        prop.price,
        prop.title,
        prop.bedrooms,
        AGENT_ID,
        isRental
      );

      if (result.isExisting && !result.missingData) {
        if (result.updated) {
          logger.property(
            pageNum,
            label,
            prop.title,
            formatPriceUk(prop.price) || "POA",
            prop.link,
            isRental,
            totalPages,
            "UPDATED"
          );
          counts.totalSaved++;
          if (isRental) counts.savedRentals++;
          else counts.savedSales++;
        } else {
          logger.property(
            pageNum,
            label,
            prop.title,
            formatPriceUk(prop.price) || "POA",
            prop.link,
            isRental,
            totalPages,
            "UNCHANGED"
          );
        }
      } else {
        let lat = prop.lat;
        let lng = prop.lng;
        let bedrooms = prop.bedrooms;
        let htmlForCoords = null;

        // If coordinates or bedrooms missing, fetch details / API
        if (lat === null || lng === null || bedrooms === null) {
          const detailInfo = await fetchDetailInfo(prop.id, prop.link);
          if (lat === null && detailInfo.lat !== null) lat = detailInfo.lat;
          if (lng === null && detailInfo.lng !== null) lng = detailInfo.lng;
          if (bedrooms === null && detailInfo.bedrooms !== null) {
            bedrooms = detailInfo.bedrooms;
          }
          if (detailInfo.html) htmlForCoords = detailInfo.html;
        }

        const coordsResult = await processPropertyWithCoordinates(
          prop.link,
          prop.price,
          prop.title,
          bedrooms,
          AGENT_ID,
          isRental,
          htmlForCoords,
          lat,
          lng
        );

        const finalLat = coordsResult.latitude || lat;
        const finalLng = coordsResult.longitude || lng;

        logger.property(
          pageNum,
          label,
          prop.title,
          formatPriceUk(prop.price) || "POA",
          prop.link,
          isRental,
          totalPages,
          "CREATED",
          finalLat,
          finalLng
        );

        counts.totalSaved++;
        if (isRental) counts.savedRentals++;
        else counts.savedSales++;

        await sleep(200); // Politeness delay ONLY on CREATED
      }
    } catch (err) {
      logger.error(`Error processing property ${prop.link}: ${err.message}`);
    }
  }

  // Enqueue next page if more pages exist
  const nextPageNum = pageNum + 1;
  if (nextPageNum <= totalPages && itemsToProcess.length > 0) {
    const nextUrl = getPageUrl(isRental, nextPageNum);
    await crawler.addRequests([
      {
        url: nextUrl,
        userData: {
          pageNum: nextPageNum,
          totalPages,
          isRental,
          label,
        },
      },
    ]);
  }
}

// ============================================================================
// MAIN RUNNER
// ============================================================================

async function run() {
  logger.step(
    `Starting Nestseekers UK scraper (Agent ${AGENT_ID}) from page ${startPage}`
  );

  const crawler = new CheerioCrawler({
    maxConcurrency: 3,
    requestHandlerTimeoutSecs: 60,
    navigationTimeoutSecs: 30,
    maxRequestRetries: 3,
    additionalMimeTypes: ["text/html", "application/xhtml+xml"],
    requestHandler: handleListingPage,
    failedRequestHandler: ({ request }, error) => {
      logger.error(`Request failed for ${request.url}: ${error.message}`);
    },
  });

  const initialRequests = PROPERTY_TYPES.map((type) => ({
    url: type.baseUrl,
    userData: {
      pageNum: startPage,
      isRental: type.isRental,
      label: type.label,
    },
  }));

  await crawler.run(initialRequests);

  if (!isPartialRun) {
    logger.step(`Cleaning up removed properties for Agent ${AGENT_ID}`);
    await updateRemoveStatus(AGENT_ID, scrapeStartTime);
  } else {
    logger.warn(
      `Partial run from page ${startPage} - skipping updateRemoveStatus for safety.`
    );
  }

  logger.step(`
==================================================
🏁 SCRAPER COMPLETED (Agent ${AGENT_ID})
--------------------------------------------------
Total Scraped: ${counts.totalScraped}
Total Saved:   ${counts.totalSaved}
Saved Sales:   ${counts.savedSales}
Saved Rentals: ${counts.savedRentals}
Skipped:       ${counts.totalSkipped}
==================================================
  `);
}

run().catch((err) => {
  logger.error(`Fatal error in Agent ${AGENT_ID}: ${err.stack || err.message}`);
  process.exit(1);
});