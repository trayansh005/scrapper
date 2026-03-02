// Hamptons scraper using CheerioCrawler with JSON extraction
// Agent ID: 108
// Website: hamptons.co.uk
// Usage:
// node backend/scraper-agent-108.js [startPage]

const { CheerioCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
updatePriceByPropertyURLOptimized,
processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

// Reduce logging noise
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 108;
const logger = createAgentLogger(AGENT_ID);

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

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ $, request }) {
const { pageNum, isRental, label, totalPages } = request.userData;
logger.page(pageNum, label, request.url, totalPages);

const html = $.html();

// Extract JSON data embedded in the script tag
const jsonMatch =
html.match(/var\s+propertyData\s*=\s*(\{.*?\});/s) ||
html.match(/homeflow\.Properties\s*=\s*(\{.*?\});/s);

if (!jsonMatch) {
logger.warn(`Could not find property JSON on page ${pageNum} (${label})`);
return;
}

let propertyData;
try {
propertyData = JSON.parse(jsonMatch[1]);
} catch (e) {
logger.error(`Error parsing JSON on page ${pageNum} (${label})`, e);
return;
}

const properties = propertyData.properties || [];
logger.page(pageNum, label, `Extracted ${properties.length} properties from JSON`, totalPages);

for (const prop of properties) {
const fullUrl = prop.url ? (prop.url.startsWith("http") ? prop.url : `https://www.hamptons.co.uk${prop.url}`)
                       : (prop.property_url.startsWith("http") ? prop.property_url : `https://www.hamptons.co.uk${prop.property_url}`);

if (isSoldProperty(prop.status || "")) continue;

if (processedUrls.has(fullUrl)) {
logger.page(pageNum, label, `Skipping duplicate: ${fullUrl.substring(0, 60)}...`, totalPages);
continue;
}
processedUrls.add(fullUrl);

const price = parseInt(prop.priceValue || prop.price_value) || 0;
const bedrooms = parseInt(prop.bedrooms) || null;
const title = prop.displayAddress || prop.display_address || prop.address_with_commas || "Property";

if (!price) {
logger.page(pageNum, label, `Skipping (no price): ${fullUrl}`, totalPages);
continue;
}

// Use coordinates from JSON directly
const latitude = prop.lat ? parseFloat(prop.lat) : null;
const longitude = prop.lng ? parseFloat(prop.lng) : null;

const result = await updatePriceByPropertyURLOptimized(
fullUrl,
price,
title,
bedrooms,
AGENT_ID,
isRental,
);

let propertyAction = "UNCHANGED";

if (result && result.updated) {
counts.totalSaved++;
propertyAction = "UPDATED";
}

if (result && !result.isExisting && !result.error) {
// Since we have coordinates in JSON, we can skip detail page visit
await processPropertyWithCoordinates(
fullUrl,
price,
title,
bedrooms,
AGENT_ID,
isRental,
null, // No HTML needed as we have manual coords
latitude,
longitude,
);
counts.totalSaved++;
counts.totalScraped++;
if (isRental) counts.savedRentals++;
else counts.savedSales++;
propertyAction = "CREATED";
} else if (result && result.isExisting && result.updated) {
counts.totalScraped++;
if (isRental) counts.savedRentals++;
else counts.savedSales++;
} else if (!result || result.error) {
propertyAction = "ERROR";
}

logger.property(
pageNum,
label,
title.substring(0, 40),
formatPriceDisplay(price, isRental),
fullUrl,
isRental,
totalPages,
propertyAction,
);

if (propertyAction !== "UNCHANGED") {
await sleep(100);
}
}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

function createCrawler() {
return new CheerioCrawler({
maxConcurrency: 5,
maxRequestRetries: 2,
requestHandler: handleListingPage,
additionalMimeTypes: ["application/json"],
preNavigationHooks: [
async ({ request }) => {
request.headers = {
...request.headers,
"User-Agent":
"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
Accept:
"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
"Accept-Language": "en-GB,en;q=0.9",
"Cache-Control": "no-cache",
Pragma: "no-cache",
};
},
],
failedRequestHandler({ request }) {
logger.error(`Failed listing page: ${request.url}`);
},
});
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
{
urlPath: "properties/sales/status-available",
totalRecords: 2600, // Roughly based on 213 pages * 12 per page
recordsPerPage: 12,
isRental: false,
label: "SALES",
},
{
urlPath: "properties/lettings/status-available",
totalRecords: 1100, // Roughly based on 91 pages * 12 per page
recordsPerPage: 12,
isRental: true,
label: "LETTINGS",
},
];

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeHamptons() {
logger.step("Starting Hamptons (JSON Extraction) scraper...");

const args = process.argv.slice(2);
const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
const isPartialRun = startPage > 1;
const scrapeStartTime = new Date();

const crawler = createCrawler();

const allRequests = [];
for (const type of PROPERTY_TYPES) {
const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
logger.step(`Queueing ${type.label} (${totalPages} pages)`);

for (let pg = Math.max(1, startPage); pg <= totalPages; pg++) {
const url =
pg === 1
? `https://www.hamptons.co.uk/${type.urlPath}`
: `https://www.hamptons.co.uk/${type.urlPath}/page-${pg}`;

allRequests.push({
url,
userData: {
pageNum: pg,
isRental: type.isRental,
label: type.label,
totalPages,
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
`Completed Hamptons - Total scraped: ${counts.totalScraped}, Total saved: ${counts.totalSaved}, New sales: ${counts.savedSales}, New lettings: ${counts.savedRentals}`,
);

if (!isPartialRun) {
logger.step("Updating remove status...");
await updateRemoveStatus(AGENT_ID, scrapeStartTime);
} else {
logger.warn("Partial run detected. Skipping updateRemoveStatus.");
}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

scrapeHamptons()
.then(() => {
logger.step("All done!");
process.exit(0);
})
.catch((error) => {
logger.error("Unhandled scraper error", error);
process.exit(1);
});
