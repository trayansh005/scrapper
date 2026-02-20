// Howards scraper using Playwright with Crawlee
// Agent ID: 229
// Website: howards.co.uk
// Usage:
// node backend/scraper-agent-229.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const { 
    formatPriceUk, 
    updatePriceByPropertyURLOptimized,
    processPropertyWithCoordinates 
} = require("./lib/db-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 229;

const stats = {
totalScraped: 0,
totalSaved: 0,
savedSales: 0,
savedRentals: 0,
};

function getBrowserlessEndpoint() {
return (
process.env.BROWSERLESS_WS_ENDPOINT ||
`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
);
}

// Configuration for sales and lettings
const PROPERTY_TYPES = [
{
baseUrl:
"https://howards.co.uk/listings?viewType=gallery&sortby=dateListed-desc&saleOrRental=Sale&rental_period=week&status=available",
isRental: false,
label: "SALES",
},
{
baseUrl:
"https://howards.co.uk/listings?viewType=gallery&sortby=dateListed-desc&saleOrRental=Rental&rental_period=month&status=available",
isRental: true,
label: "RENTALS",
},
];

function createCrawler(browserWSEndpoint) {
return new PlaywrightCrawler({
maxConcurrency: 1,
maxRequestRetries: 2,
requestHandlerTimeoutSecs: 300,
launchContext: {
launcher: undefined,
launchOptions: {
browserWSEndpoint,
args: ["--no-sandbox", "--disable-setuid-sandbox"],
},
},
requestHandler: handleListingPage,
failedRequestHandler({ request }) {
log.error(`Failed listing page: ${request.url}`);
},
});
}

async function handleListingPage({ page, request, crawler }) {
const { isRental, label, pageNumber } = request.userData;

console.log(` Loading: ${request.url}`);

try {
await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });
await page.waitForTimeout(3000);

const result = await extractPropertiesFromPage(page, isRental);
let rawProperties = result.properties;
const hasNextPage = result.hasNextPage;

// De-duplicate by normalized URL to prevent parallel processing of the same property
const urlMap = new Map();
for (const p of rawProperties) {
const normalizedUrl = p.url.trim().toLowerCase().replace(/\/$/, ""); // trim, lowercase, remove trailing slash
if (!urlMap.has(normalizedUrl)) {
urlMap.set(normalizedUrl, p);
}
}
const properties = Array.from(urlMap.values());

if (properties.length === 0) {
console.log(` No ${label.toLowerCase()} found on this page.`);
} else {
console.log(` Found ${properties.length} unique ${label.toLowerCase()}`);
}
stats.totalScraped += properties.length;

// Save properties to database
for (const property of properties) {
try {
const priceNum = property.price
? parseFloat(property.price.replace(/[^0-9.]/g, ""))
: null;

if (priceNum === null) {
console.log(` No price found: ${property.title}`);
continue;
}

const updateResult = await updatePriceByPropertyURLOptimized(
property.url.trim(),
priceNum,
property.title,
property.bedrooms,
AGENT_ID,
isRental,
);

if (!updateResult.isExisting || updateResult.updated) {
// Need coordinates for new property or price change
const coords = await extractCoordsFromDetailsPage(page.context(), property.url);

await processPropertyWithCoordinates(
property.url.trim(),
priceNum,
property.title,
property.bedrooms,
AGENT_ID,
isRental,
null, // htmlContent
coords ? coords.latitude : null,
coords ? coords.longitude : null
);

stats.totalSaved++;
if (isRental) stats.savedRentals++;
else stats.savedSales++;

const priceDisplay = formatPriceUk(priceNum);
console.log(
` ${property.title} - ${priceDisplay}${
coords ? ` - (${coords.latitude}, ${coords.longitude})` : ""
}`,
);
} else {
console.log(`ℹ No change for: ${property.title}`);
}
} catch (err) {
console.error(` Error saving property: ${err.message}`);
}
}

// Queue next page if available
if (hasNextPage) {
const nextPage = (pageNumber || 1) + 1;
const urlObj = new URL(request.url);
urlObj.searchParams.set("page", nextPage.toString());
const nextUrl = urlObj.toString();

await crawler.addRequests([
{
url: nextUrl,
userData: { ...request.userData, pageNumber: nextPage },
},
]);
}
} catch (error) {
console.error(` Error in ${label} scrape: ${error.message}`);
}
}

async function scrapeAllTypes() {
console.log(`\n Starting Howards scraper (Agent ${AGENT_ID})...\n`);

const browserWSEndpoint = getBrowserlessEndpoint();
console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

for (const propertyType of PROPERTY_TYPES) {
const { baseUrl, isRental, label } = propertyType;

console.log(`\n Starting ${label} scrape...\n`);

const crawler = createCrawler(browserWSEndpoint);

await crawler.addRequests([
{
url: baseUrl,
userData: { isRental, label, pageNumber: 1 },
},
]);

await crawler.run();
}

console.log(`\n Scraping complete!`);
console.log(`Total scraped: ${stats.totalScraped}`);
console.log(`Total saved: ${stats.totalSaved}`);
console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}\n`);
}

async function extractPropertiesFromPage(page, isRental) {
const result = await page.evaluate((isRental) => {
const propertyMap = new Map();

// Find links to property listings
const links = Array.from(document.querySelectorAll('a[href*="/listings/"]'));

links.forEach((linkEl) => {
try {
const url = linkEl.href;
if (!url || propertyMap.has(url)) return;

// Find the parent card container
const card = linkEl.closest('div[class*="v2-flex"]') || linkEl.parentElement?.parentElement;
if (!card || !card.textContent.includes("Bed")) return;

// Extract title
const titleEl = card.querySelector("h4");
const title = titleEl ? titleEl.textContent.trim() : "N/A";

// Extract price
let price = null;
const strongEl = card.querySelector("strong");
if (strongEl) {
const priceMatch = strongEl.textContent.match(/([\d,]+)/);
if (priceMatch) {
price = priceMatch[1];
}
}

// If price not found in strong, try broad search in card
if (!price) {
const priceMatch = card.textContent.match(/([\d,]+)/);
if (priceMatch) price = priceMatch[1];
}

// Extract bedrooms
let bedrooms = null;
const pElements = Array.from(card.querySelectorAll("p"));
const bedsP = pElements.find((p) => p.textContent.includes("Bed"));
if (bedsP) {
const bedsMatch = bedsP.textContent.match(/(\d+)/);
if (bedsMatch) {
bedrooms = parseInt(bedsMatch[1], 10);
}
}

if (price) {
propertyMap.set(url, {
url,
title,
price,
bedrooms,
latitude: null,
longitude: null,
});
}
} catch (err) {
console.error(`Error extracting property: ${err.message}`);
}
});

// Check for next page button
const nextLink = Array.from(document.querySelectorAll("a")).find(
(a) => a.textContent.trim() === "Next " || a.textContent.trim() === "Next",
);
const hasNextPage = !!nextLink;

return { properties: Array.from(propertyMap.values()), hasNextPage };
}, isRental);

return result;
}

async function extractCoordsFromDetailsPage(browserContext, propertyUrl) {
let detailPage = null;
const mapRequestUrls = [];
try {
console.log(` Extracting coordinates from: ${propertyUrl}`);

return await Promise.race([
(async () => {
detailPage = await browserContext.newPage({
ignoreHTTPSErrors: true,
});

detailPage.on("request", (req) => {
const reqUrl = req.url();
if (
reqUrl.includes("StaticMapService.GetMapImage") ||
reqUrl.includes("/maps/vt?pb=") ||
reqUrl.includes("tiles.stadiamaps.com")
) {
mapRequestUrls.push(reqUrl);
}
});

await detailPage.goto(propertyUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
await detailPage.waitForTimeout(2000);

// Try fallback: look for embedded map or structured data or TwigReact
let coords = await detailPage.evaluate(() => {
// Try to find coordinates in script tags
const scripts = Array.from(document.querySelectorAll("script"));
for (const script of scripts) {
const text = script.textContent;

// Look for "latitude":"52.48469810","longitude":"1.72075380"
const latMatch = text.match(/"latitude"\s*:\s*"([\d.-]+)"/i);
const lngMatch = text.match(/"longitude"\s*:\s*"([\d.-]+)"/i);
if (latMatch && lngMatch) {
return {
latitude: parseFloat(latMatch[1]),
longitude: parseFloat(lngMatch[1]),
};
}

// Fallback numeric patterns
const latNumMatch = text.match(/latitude['":\s]+([0-9.-]+)/i);
const lngNumMatch = text.match(/longitude['":\s]+([0-9.-]+)/i);
if (latNumMatch && lngNumMatch) {
return {
latitude: parseFloat(latNumMatch[1]),
longitude: parseFloat(lngNumMatch[1]),
};
}
}
return null;
});

if (coords && isUkCoordinate(coords.latitude, coords.longitude)) {
console.log(` Extracted coords: ${coords.latitude}, ${coords.longitude}`);
return coords;
}

// Try to extract coordinates from map network requests
coords = extractCoordsFromMapRequests(mapRequestUrls);
if (coords) {
console.log(` Found coordinates in map requests`);
console.log(` Extracted coords: ${coords.latitude}, ${coords.longitude}`);
return coords;
}

console.log(` No coords extracted`);
return null;
})(),
new Promise((_, reject) =>
setTimeout(
() => reject(new Error("Coordinate extraction timeout after 20 seconds")),
20000,
),
),
]);
} catch (err) {
console.error(`Error extracting coordinates: ${err.message}`);
return null;
} finally {
if (detailPage) {
await detailPage.close().catch(() => {});
}
}
}

function extractCoordsFromMapRequests(requestUrls) {
if (!Array.isArray(requestUrls) || requestUrls.length === 0) {
return null;
}

for (const requestUrl of requestUrls) {
// Pattern 1: Static map request with pixel center at fixed zoom
const staticMatch = requestUrl.match(/[?&]1i=(\d+).*?[?&]2i=(\d+).*?[?&]3u=(\d+)/);
if (staticMatch) {
const pixelX = parseInt(staticMatch[1], 10);
const pixelY = parseInt(staticMatch[2], 10);
const zoom = parseInt(staticMatch[3], 10);

const coords = pixelToLatLng(pixelX, pixelY, zoom);
if (coords && isUkCoordinate(coords.latitude, coords.longitude)) {
return coords;
}
}

// Pattern 2: Vector tile request with E7 integers
const vtMatch = requestUrl.match(/!1x(-?\d+)!2x(-?\d+)/);
if (vtMatch) {
const latE7 = parseInt(vtMatch[1], 10);
const lonRaw = parseInt(vtMatch[2], 10);
const lonE7 = toSigned32(lonRaw);

const latitude = latE7 / 1e7;
const longitude = lonE7 / 1e7;

if (isUkCoordinate(latitude, longitude)) {
return { latitude, longitude };
}
}
}

return null;
}

function pixelToLatLng(pixelX, pixelY, zoom) {
if (!Number.isFinite(pixelX) || !Number.isFinite(pixelY) || !Number.isFinite(zoom)) {
return null;
}

const worldSize = 256 * Math.pow(2, zoom);
const longitude = (pixelX / worldSize) * 360 - 180;
const n = Math.PI - (2 * Math.PI * pixelY) / worldSize;
const latitude = (180 / Math.PI) * Math.atan(Math.sinh(n));

if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
return null;
}

return { latitude, longitude };
}

function toSigned32(value) {
if (!Number.isFinite(value)) return value;
return value > 2147483647 ? value - 4294967296 : value;
}

function isUkCoordinate(latitude, longitude) {
return latitude > 49 && latitude < 61 && longitude > -11 && longitude < 3;
}

(async () => {
try {
await scrapeAllTypes();
await updateRemoveStatus(AGENT_ID);
console.log("\n All done!");
process.exit(0);
} catch (err) {
console.error(" Fatal error:", err?.message || err);
process.exit(1);
}
})();
