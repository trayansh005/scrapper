// The Agency UK (formerly The Estate Agency) scraper using Playwright
// Agent ID: 111
// Website: theagencyuk.com
// Usage:
// node backend/scraper-agent-111.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
updatePriceByPropertyURLOptimized,
processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 111;
const logger = createAgentLogger(AGENT_ID);

const stats = {
totalScraped: 0,
totalSaved: 0,
savedSales: 0,
savedRentals: 0,
};

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function blockNonEssentialResources(page) {
return page.route("**/*", (route) => {
const resourceType = route.request().resourceType();
if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
return route.abort();
}
return route.continue();
});
}

function getBrowserlessEndpoint() {
return (
process.env.BROWSERLESS_WS_ENDPOINT ||
`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

async function scrapePropertyDetail(browserContext, property, isRental) {
const detailPage = await browserContext.newPage();

try {
await blockNonEssentialResources(detailPage);

await detailPage.goto(property.link, {
waitUntil: "domcontentloaded",
timeout: 60000,
});

// Small delay to ensure content is loaded
await detailPage.waitForTimeout(2000);

const html = await detailPage.content();

// Use helper to extract coordinates from HTML
await processPropertyWithCoordinates(
property.link,
property.price,
property.title,
property.bedrooms,
AGENT_ID,
isRental,
html,
);

stats.totalScraped++;
stats.totalSaved++;
if (isRental) stats.savedRentals++;
else stats.savedSales++;
} catch (error) {
logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
} finally {
await detailPage.close();
}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
const { isRental, label, pageNumber, totalPages } = request.userData;
logger.page(pageNumber, label, request.url, totalPages);

try {
// Wait for property cards on the new Propertystream-based site
// Propertystream sites use different selectors depending on the developer, but these are common
const selectorFound = await Promise.any([
page.waitForSelector(".property-grid .property-card", { timeout: 30000 }).then(() => ".property-grid .property-card"),
page.waitForSelector(".properties-container .property", { timeout: 30000 }).then(() => ".properties-container .property"),
page.waitForSelector("div[class*='property-card']", { timeout: 30000 }).then(() => "div[class*='property-card']"),
page.waitForSelector("article[class*='property']", { timeout: 30000 }).then(() => "article[class*='property']")
]).catch(() => null);

if (!selectorFound) {
logger.error(`No property cards found on page ${pageNumber} after 30s. Listing might be empty or selectors changed.`, null, pageNumber, label);
return;
}

console.log(`[Agent 111] Found selector: ${selectorFound}`);

const properties = await page.evaluate((sel) => {
const cards = Array.from(document.querySelectorAll(sel));
return cards.map((card) => {
const linkEl = card.querySelector("a[href*='/property/']") || card.querySelector("a");
let link = linkEl ? linkEl.getAttribute("href") : null;
if (link && !link.startsWith("http")) {
link = "https://theagencyuk.com" + (link.startsWith("/") ? "" : "/") + link;
}

// Title: usually address or similar
const titleEl = card.querySelector("h3") || card.querySelector("h2") || card.querySelector(".address") || card.querySelector(".title");
const title = titleEl ? titleEl.textContent.trim() : "The Agency UK Property";

// Bedroom: look for ".bedrooms" or common text markers
const cardText = card.innerText || "";
const bedMatch = cardText.match(/(\d+)\s*bedrooms?/i) || cardText.match(/bed:\s*(\d+)/i);
let bedrooms = null;
if (bedMatch) {
bedrooms = parseInt(bedMatch[1]);
}

// Price: ".price" or monetary pattern
const priceEl = card.querySelector(".price") || card.querySelector(".amount") || card.querySelector("[class*='price']");
const priceText = priceEl ? priceEl.textContent.trim() : (cardText.match(/[\d,]+(\d+)?/) || [""])[0];

// Status: Sold or available
const statusText = cardText;

return {
link,
title,
priceText,
bedrooms,
statusText,
};
});
}, selectorFound);

logger.page(pageNumber, label, `Found ${properties.length} properties`, totalPages);

for (const property of properties) {
if (!property.link || !property.priceText) continue;

if (isSoldProperty(property.statusText)) {
continue;
}

const price = parsePrice(property.priceText);
if (!price) {
logger.page(pageNumber, label, `Skipping (no price parsed from "${property.priceText}"): ${property.link}`, totalPages);
continue;
}

const updateResult = await updatePriceByPropertyURLOptimized(
property.link,
price,
property.title,
property.bedrooms,
AGENT_ID,
isRental,
);

let action = "SEEN";

if (updateResult.updated) {
stats.totalSaved++;
action = "UPDATED";
}

if (!updateResult.isExisting && !updateResult.error) {
action = "CREATED";
// New property, need coordinates from detail page
await scrapePropertyDetail(page.context(), { ...property, price }, isRental);
// Small delay to avoid triggering rate limit/blocking
await new Promise((r) => setTimeout(r, 2000));
} else if (updateResult.error) {
action = "ERROR";
}

logger.property(
pageNumber,
label,
property.title.substring(0, 40),
`${price}`,
property.link,
isRental,
totalPages,
action,
);
}
} catch (error) {
logger.error(`Error in handleListingPage: ${error.message}`, error, pageNumber, label);
}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler(browserWSEndpoint) {
return new PlaywrightCrawler({
maxConcurrency: 1,
maxRequestRetries: 2,
navigationTimeoutSecs: 120,
requestHandlerTimeoutSecs: 900,
preNavigationHooks: [
async ({ page }) => {
await blockNonEssentialResources(page);
},
],
launchContext: {
launcher: undefined,
launchOptions: {
browserWSEndpoint,
args: ["--no-sandbox", "--disable-setuid-sandbox"],
},
},
requestHandler: handleListingPage,
failedRequestHandler({ request }) {
logger.error(`Failed listing page: ${request.url}`);
},
});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeTheAgencyUK() {
const args = process.argv.slice(2);
const startPage = args.length > 0 ? parseInt(args[0]) : 1;
const scrapeStartTime = new Date();

logger.step(`Starting The Agency UK Scraper (Agent ${AGENT_ID})...`);

const browserWSEndpoint = getBrowserlessEndpoint();
const crawler = createCrawler(browserWSEndpoint);

const totalPages = 15; // Propertystream sites are categorized by search filters
const requests = [];

// Update to new domain and standard search URL structure
for (let p = Math.max(1, startPage); p <= totalPages; p++) {
const url = `https://theagencyuk.com/property-search/page/${p}/?department=residential-sales`;
requests.push({
url,
userData: {
pageNumber: p,
totalPages,
isRental: false,
label: "SALES",
},
});
}

if (requests.length > 0) {
logger.step(`Queueing ${requests.length} listing pages starting from page ${startPage}...`);
await crawler.run(requests);
}

logger.step(
`Finished The Agency UK - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
);
logger.step(`Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);

// Only update remove status if we did a full run
if (startPage === 1) {
logger.step("Updating remove status for properties not seen in this run...");
await updateRemoveStatus(AGENT_ID, scrapeStartTime);
}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
try {
await scrapeTheAgencyUK();
logger.step("All done!");
process.exit(0);
} catch (err) {
logger.error("Fatal error", err);
process.exit(1);
}
})();
