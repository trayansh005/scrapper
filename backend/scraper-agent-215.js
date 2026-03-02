// Harrods Estates scraper with API-first .ljson extraction
// Agent ID: 215
// Website: harrodsestates.com
// Usage:
// node backend/scraper-agent-215.js [startPage]

const { CheerioCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
updatePriceByPropertyURLOptimized,
processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice, formatPriceUk } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 215;
const logger = createAgentLogger(AGENT_ID);

const stats = {
totalFound: 0,
totalScraped: 0,
totalSaved: 0,
totalSkipped: 0,
};

const scrapeStartTime = new Date();
const startPageArgument = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
const isPartialRun = startPageArgument > 1;

async function sleep(ms) {
return new Promise((resolve) => setTimeout(resolve, ms));
}

const PROPERTY_TYPES = [
{
apiBase: "https://www.harrodsestates.com/properties/sales/status-available",
isRental: false,
label: "SALES",
},
{
apiBase: "https://www.harrodsestates.com/properties/lettings/status-available",
isRental: true,
label: "RENTALS",
},
];

function buildApiUrl(apiBase, pageNum) {
if (pageNum <= 1) return `${apiBase}.ljson`;
return `${apiBase}/page-${pageNum}.ljson`;
}

function extractPropertyDataFromScript($) {
const scripts = $("script")
.map((_, script) => $(script).html() || "")
.get();

for (const text of scripts) {
if (!text.includes("var propertyData =")) continue;
const match = text.match(/var\s+propertyData\s*=\s*(\{[\s\S]*?\});/);
if (!match) continue;

try {
return JSON.parse(match[1]);
} catch (err) {
return null;
}
}
return null;
}

function parseListingPayload(body, $) {
if (typeof body === "string") {
const trimmed = body.trim();
if (trimmed.startsWith("{")) {
try {
return JSON.parse(trimmed);
} catch (err) {
// Fall back to script extraction
}
}
}

if ($) {
return extractPropertyDataFromScript($);
}

return null;
}

const crawler = new CheerioCrawler({
maxConcurrency: 2,
maxRequestRetries: 2,
requestHandlerTimeoutSecs: 300,
additionalMimeTypes: ["application/ljson", "application/json"],

async requestHandler({ request, body, $, crawler }) {
const { pageNum, isRental, label, startPage, apiBase } = request.userData;
logger.page(pageNum, label, `Processing ${request.url}`, request.userData.totalPages || null);

const data = parseListingPayload(body, $);
if (!data || !Array.isArray(data.properties)) {
logger.error(`No property payload found on page ${pageNum}`, null, pageNum, label);
return;
}

const properties = data.properties;
const totalCount = data.pagination?.total_count || properties.length;
const pageSize = data.pagination?.page_size || properties.length || 9;
const discoveredTotalPages = Math.max(pageNum, Math.ceil(totalCount / pageSize));

if (!request.userData.totalPages) {
request.userData.totalPages = discoveredTotalPages;
}

// Add next pages if we are at the start page
if (pageNum === startPage && discoveredTotalPages > pageNum) {
const nextRequests = [];
for (let p = pageNum + 1; p <= discoveredTotalPages; p++) {
nextRequests.push({
url: buildApiUrl(apiBase, p),
userData: {
...request.userData,
pageNum: p,
totalPages: discoveredTotalPages,
},
});
}
if (nextRequests.length > 0) {
await crawler.addRequests(nextRequests);
}
}

for (const item of properties) {
const propertyUrl = item?.property_url || "";
if (!propertyUrl) continue;

const link = propertyUrl.startsWith("http")
? propertyUrl
: `https://www.harrodsestates.com${propertyUrl}`;

const status = (item.status || "").toString();
if (isSoldProperty(status)) {
stats.totalSkipped++;
continue;
}

const numericPrice = parsePrice(
item.price_value ?? item.price_without_qualifier ?? item.price,
);
const title = item.display_address || "Harrods Property";
const bedrooms = item.bedrooms || null;
const lat = item.lat || null;
const lng = item.lng || null;

if (!numericPrice) {
logger.property(
pageNum,
label,
title,
"N/A",
link,
isRental,
request.userData.totalPages || discoveredTotalPages,
"ERROR",
);
stats.totalSkipped++;
continue;
}

stats.totalFound++;

let action = "UNCHANGED";
const priceCheck = await updatePriceByPropertyURLOptimized(link, numericPrice, title, bedrooms, AGENT_ID, isRental);

if (priceCheck.isExisting) {
if (priceCheck.updated) {
action = "UPDATED";
stats.totalSaved++;
stats.totalScraped++;
}
} else {
const dbResult = await processPropertyWithCoordinates(
link,
numericPrice,
title,
bedrooms,
AGENT_ID,
isRental,
"", // No HTML needed if we have coords
lat,
lng,
);

if (dbResult.updated || !dbResult.isExisting) {
action = dbResult.updated ? "UPDATED" : "CREATED";
stats.totalSaved++;
stats.totalScraped++;
}
}

logger.property(
pageNum,
label,
title,
`${formatPriceUk(numericPrice)}`,
link,
isRental,
request.userData.totalPages || discoveredTotalPages,
action,
);

if (action !== "UNCHANGED") {
await sleep(100);
}
}
},

failedRequestHandler({ request }) {
const { pageNum, label } = request.userData || {};
logger.error(`Failed API request: ${request.url}`, null, pageNum, label);
},
});

async function run() {
logger.step(`Starting Harrods Estates scraper (Agent ${AGENT_ID})`);
const startPage = Math.max(1, startPageArgument || 1);

const startUrls = PROPERTY_TYPES.map((type) => ({
url: buildApiUrl(type.apiBase, startPage),
userData: {
pageNum: startPage,
startPage,
isRental: type.isRental,
label: type.label,
apiBase: type.apiBase,
},
}));

if (isPartialRun) {
logger.step(
`Partial run detected (startPage=${startPageArgument}). Remove status update will be skipped.`,
);
}

await crawler.run(startUrls);

if (!isPartialRun) {
logger.step("Updating removed status for inactive properties...");
const removedCount = await updateRemoveStatus(AGENT_ID, scrapeStartTime);
logger.step(`Marked ${removedCount} properties as removed`);
} else {
logger.step("Skipping remove status update (Partial run)");
}

logger.step(
`Scrape completed. Found: ${stats.totalFound}, Saved/Updated: ${stats.totalSaved}, Skipped: ${stats.totalSkipped}`,
);
}

run().catch((err) => {
logger.error(`Fatal error: ${err.message}`);
process.exit(1);
});
