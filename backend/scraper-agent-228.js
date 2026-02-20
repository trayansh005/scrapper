// Starkings and Watson scraper using Playwright with Crawlee
// Agent ID: 228
// Website: starkingsandwatson.co.uk
// Usage:
// node backend/scraper-agent-228.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized, processPropertyWithCoordinates } = require("./lib/db-helpers.js");
const { isSoldProperty } = require("./lib/property-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 228;
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

// Configuration
const PROPERTY_TYPES = [
{
baseUrl: "https://www.starkingsandwatson.co.uk/buying/property-search/page/",
params: "/?department=sales&location&lat&lng&radius=3&min-price&max-price&bedrooms",
totalPages: 57,
isRental: false,
label: "SALES",
},
{
baseUrl: "https://www.starkingsandwatson.co.uk/letting/property-search/page/",
params: "",
totalPages: 5,
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

async function handleListingPage({ page, request }) {
const { pageNum, isRental, label } = request.userData;

console.log(` ${label} - Page ${pageNum} - ${request.url}`);

try {
await page.waitForTimeout(2000);
await page.waitForSelector(".card.inview-trigger-animation-fade-in-up-sm", { timeout: 20000 }).catch(() => {
console.log(` No listing container found on page ${pageNum}`);
});

// Extract properties
const properties = await page.evaluate(() => {
try {
const items = Array.from(document.querySelectorAll(".card.inview-trigger-animation-fade-in-up-sm"));
return items.map((el) => {
const statusLabel = el.querySelector(".card__label")?.innerText?.trim() || "";
const imageFlash = el.querySelector(".image-flash")?.innerText?.trim() || "";
const combinedLabel = (statusLabel + " " + imageFlash).toUpperCase();

const linkEl = el.querySelector("a[href*='/property/']");
const link = linkEl ? linkEl.href : null;
const title = el.querySelector(".card__title")?.innerText.trim() || "";
const rawPrice = el.querySelector(".card__text")?.innerText.trim() || "";

const iconItems = Array.from(el.querySelectorAll(".icons__item"));
const bedrooms = iconItems[0]?.querySelector(".icons__text")?.innerText.trim() || null;
                    
return { link, title, rawPrice, bedrooms, combinedLabel };
}).filter((p) => p.link);
} catch (err) { return []; }
});

console.log(` Found ${properties.length} properties on page ${pageNum}`);
stats.totalScraped += properties.length;

const batchSize = 2;
for (let i = 0; i < properties.length; i += batchSize) {
const batch = properties.slice(i, i + batchSize);

await Promise.all(
batch.map(async (property) => {
if (!property.link) return;
                    
                    if (property.combinedLabel.includes("SOLD") || property.combinedLabel.includes("STC") || 
                        property.combinedLabel.includes("AGREED") || property.combinedLabel.includes("UNDER OFFER") ||
                        (property.combinedLabel.includes("LET") && !property.combinedLabel.includes("TO LET"))) {
                        return;
                    }

const priceNum = property.rawPrice ? parseFloat(property.rawPrice.replace(/[^0-9.]/g, "")) : null;
if (priceNum === null) return;

const updateResult = await updatePriceByPropertyURLOptimized(
property.link.trim(),
priceNum,
property.title,
property.bedrooms,
AGENT_ID,
isRental,
);

let persisted = !!updateResult.updated;

if (!updateResult.isExisting) {
                        const detailPage = await page.context().newPage();
                        try {
                            await detailPage.goto(property.link, { waitUntil: "domcontentloaded", timeout: 40000 });
                            const htmlContent = await detailPage.content();
                            await processPropertyWithCoordinates(
                                property.link.trim(),
                                formatPriceUk(priceNum),
                                property.title,
                                property.bedrooms,
                                AGENT_ID,
                                isRental,
                                htmlContent,
                            );
                            persisted = true;
                        } catch (err) {
                        } finally {
                            await detailPage.close();
                        }
}

if (persisted) {
stats.totalSaved++;
if (isRental) stats.savedRentals++;
else stats.savedSales++;
}

console.log(` ${property.title} - ${formatPriceUk(priceNum)}`);
}),
);
await page.waitForTimeout(500);
}
} catch (error) {
console.error(` Error in ${label} page ${pageNum}: ${error.message}`);
}
}

async function scrapeStarkingsWatson() {
console.log(`\n Starting Starkings and Watson scraper (Agent ${AGENT_ID})...\n`);

const browserWSEndpoint = getBrowserlessEndpoint();

for (const propertyType of PROPERTY_TYPES) {
console.log(`\n Processing ${propertyType.label} (${propertyType.totalPages} pages)`);
const crawler = createCrawler(browserWSEndpoint);
const requests = [];
for (let pg = 1; pg <= propertyType.totalPages; pg++) {
requests.push({
url: `${propertyType.baseUrl}${pg}${propertyType.params}`,
userData: { pageNum: pg, isRental: propertyType.isRental, label: propertyType.label },
});
}
await crawler.addRequests(requests);
await crawler.run();
}

console.log(`\n Scraping complete!`);
console.log(`Total scraped: ${stats.totalScraped}`);
console.log(`Total saved: ${stats.totalSaved}`);
}

(async () => {
try {
await scrapeStarkingsWatson();
await updateRemoveStatus(AGENT_ID);
console.log("\n All done!");
process.exit(0);
} catch (err) {
console.error(" Fatal error:", err?.message || err);
process.exit(1);
}
})();
