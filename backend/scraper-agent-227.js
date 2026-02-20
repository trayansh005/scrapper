// Abbey Sales and Lettings Group scraper using Playwright with Crawlee
// Agent ID: 227
// Website: abbeysalesandlettingsgroup.co.uk
// Usage:
// node backend/scraper-agent-227.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus, updatePriceByPropertyURL } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");

// Reduce verbosity
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 227;
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
urlBase: "https://abbeysalesandlettingsgroup.co.uk/search/page/",
totalPages: 14,
isRental: false,
label: "SALES",
suffix: "/?address_keyword&department=residential-sales&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&property_type&officeID&availability=2",
},
{
urlBase: "https://abbeysalesandlettingsgroup.co.uk/search/page/",
totalPages: 7,
isRental: true,
label: "RENTALS",
suffix: "/?address_keyword=&department=residential-lettings&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&minimum_bedrooms=&property_type=&officeID=&availability=6",
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
await page.waitForSelector(".properties-block .grid-box", { timeout: 20000 }).catch(() => {
console.log(` No listing container found on page ${pageNum}`);
});

// Extract properties
const properties = await page.evaluate(() => {
try {
const items = Array.from(document.querySelectorAll(".properties-block .grid-box"));
return items.map((el) => {
const linkEl = el.querySelector("a[href*='/property/']");
const link = linkEl ? linkEl.href : null;
const title = el.querySelector("h4")?.innerText.trim() || "";
const rawPrice = el.querySelector("h5.property-archive-price")?.innerText.trim() || "";
const typeItems = Array.from(el.querySelectorAll("ul.property-types li"));
let bedrooms = null;
typeItems.forEach((li) => {
const span = li.querySelector("span");
const icon = li.querySelector("i");
if (icon && icon.classList.contains("fa-bed")) {
bedrooms = span ? span.innerText.trim() : null;
}
});
return { link, title, rawPrice, bedrooms };
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
                            const latLng = await detailPage.evaluate(() => {
                                const scripts = Array.from(document.querySelectorAll("script"));
                                for (const script of scripts) {
                                    const text = script.textContent || "";
                                    const m1 = text.match(/new google\.maps\.LatLng\(([0-9.-]+),\s*([0-9.-]+)\);/);
                                    if (m1) return { lat: parseFloat(m1[1]), lng: parseFloat(m1[2]) };
                                    const mLat = text.match(/const\s+lat\s*=\s*parseFloat\(['"]([0-9.-]+)['"]\)/);
                                    const mLng = text.match(/const\s+lng\s*=\s*parseFloat\(['"]([0-9.-]+)['"]\)/);
                                    if (mLat && mLng) return { lat: parseFloat(mLat[1]), lng: parseFloat(mLng[1]) };
                                }
                                return null;
                            });

                            await updatePriceByPropertyURL(
                                property.link.trim(),
                                formatPriceUk(priceNum),
                                property.title,
                                property.bedrooms,
                                AGENT_ID,
                                isRental,
                                latLng ? latLng.lat : null,
                                latLng ? latLng.lng : null,
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

async function scrapeAbbeySalesLettings() {
console.log(`\n Starting Abbey Sales and Lettings Group scraper (Agent ${AGENT_ID})...\n`);

const browserWSEndpoint = getBrowserlessEndpoint();

for (const propertyType of PROPERTY_TYPES) {
console.log(`\n Processing ${propertyType.label} (${propertyType.totalPages} pages)`);
const crawler = createCrawler(browserWSEndpoint);
const requests = [];
for (let pg = 1; pg <= propertyType.totalPages; pg++) {
requests.push({
url: `${propertyType.urlBase}${pg}${propertyType.suffix}`,
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
await scrapeAbbeySalesLettings();
await updateRemoveStatus(AGENT_ID);
console.log("\n All done!");
process.exit(0);
} catch (err) {
console.error(" Fatal error:", err?.message || err);
process.exit(1);
}
})();
