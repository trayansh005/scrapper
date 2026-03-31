// ========================================================
// Hi Residential Scraper - Agent 265
// With Latitude & Longitude Extraction + Geocoding Fallback
// ========================================================

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");
const cheerio = require("cheerio");
const https = require("https");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 265;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const BASE_URL = "https://www.hi-residential.com/properties";

// ============================================================================
// GEOCODING FALLBACK (Nominatim)
// ============================================================================
async function geocodeAddress(address) {
	if (!address) return { latitude: null, longitude: null };

	const query = encodeURIComponent(address.trim() + ", UK");
	const url = `https://nominatim.openstreetmap.org/search?q=${query}&format=json&limit=1&addressdetails=1`;

	return new Promise((resolve) => {
		https.get(url, {
			headers: { 'User-Agent': 'HiResidentialScraper/1.0 (Contact: your@email.com)' }
		}, (res) => {
			let data = '';
			res.on('data', chunk => data += chunk);
			res.on('end', () => {
				try {
					const results = JSON.parse(data);
					if (results && results.length > 0) {
						const lat = parseFloat(results[0].lat);
						const lon = parseFloat(results[0].lon);
						logger.step(`✅ Geocoded: ${address} → ${lat.toFixed(6)}, ${lon.toFixed(6)}`);
						resolve({ latitude: lat, longitude: lon });
					} else {
						resolve({ latitude: null, longitude: null });
					}
				} catch (e) {
					resolve({ latitude: null, longitude: null });
				}
			});
		}).on('error', () => resolve({ latitude: null, longitude: null }));
	});
}

// ============================================================================
// EXTRACT COORDINATES FROM DETAIL PAGE
// ============================================================================
async function extractCoordinatesFromDetailPage(page, title) {
	try {
		// ✅ Removed waitForTimeout(4000) here — caller already waits 3000ms after domcontentloaded
		const html = await page.content();
		const $ = cheerio.load(html);

		let latitude = null;
		let longitude = null;

		// Method 1: JSON-LD
		$('script[type="application/ld+json"]').each((_, elem) => {
			try {
				const data = JSON.parse($(elem).html());
				const geo = data.geo ||
					(data['@graph'] && data['@graph'].find(item => item.latitude || item.lat));
				if (geo) {
					latitude = parseFloat(geo.latitude || geo.lat);
					longitude = parseFloat(geo.longitude || geo.lng || geo.lon);
				}
			} catch (_) { }
		});

		// Method 2: JavaScript variable patterns in page source
		if (!latitude || !longitude) {
			const pageText = html;
			const latMatch = pageText.match(/"?latitude"?\s*[:=]\s*([0-9.-]+)/i) ||
				pageText.match(/lat["']\s*[:=]\s*([0-9.-]+)/i);
			const lngMatch = pageText.match(/"?longitude"?\s*[:=]\s*([0-9.-]+)/i) ||
				pageText.match(/lng["']\s*[:=]\s*([0-9.-]+)/i);
			if (latMatch) latitude = parseFloat(latMatch[1]);
			if (lngMatch) longitude = parseFloat(lngMatch[1]);
		}

		// Method 3: Leaflet center pattern
		if (!latitude || !longitude) {
			const centerMatch = html.match(/center\s*:\s*\[\s*([0-9.-]+)\s*,\s*([0-9.-]+)\s*\]/i);
			if (centerMatch) {
				latitude = parseFloat(centerMatch[1]);
				longitude = parseFloat(centerMatch[2]);
			}
		}

		// ✅ Method 4: Google Maps embed URL pattern (common on property sites)
		if (!latitude || !longitude) {
			const mapsMatch = html.match(/maps\.google\.com[^"']*[?&]q=([0-9.-]+),([0-9.-]+)/i) ||
				html.match(/maps\.google\.com[^"']*ll=([0-9.-]+),([0-9.-]+)/i);
			if (mapsMatch) {
				latitude = parseFloat(mapsMatch[1]);
				longitude = parseFloat(mapsMatch[2]);
			}
		}

		// ✅ Method 5: data-lat / data-lng attributes
		if (!latitude || !longitude) {
			const dataLatMatch = html.match(/data-lat=["']([0-9.-]+)["']/i);
			const dataLngMatch = html.match(/data-lng=["']([0-9.-]+)["']/i) ||
				html.match(/data-lon=["']([0-9.-]+)["']/i);
			if (dataLatMatch) latitude = parseFloat(dataLatMatch[1]);
			if (dataLngMatch) longitude = parseFloat(dataLngMatch[1]);
		}

		// Geocoding fallback
		if (!latitude || !longitude) {
			logger.property(0, "GEOCODE", title.substring(0, 40),
				"Direct extraction failed → geocoding", "", false, 0, "FALLBACK");
			const coords = await geocodeAddress(title);
			latitude = coords.latitude;
			longitude = coords.longitude;
		}

		return { latitude, longitude };

	} catch (err) {
		logger.error(`Coordinate extraction failed: ${err.message}`);
		return { latitude: null, longitude: null };
	}
}

// ============================================================================
// EXTRACT PROPERTIES FROM LISTING PAGE
// ============================================================================
function extractPropertiesFromHTML(html, isRental) {
	const $ = cheerio.load(html);
	const properties = [];

	$("li.col-sm-6.col-md-6.qt-property-thumb-wrapper").each((_, element) => {
		try {
			const $prop = $(element);

			let link = $prop.find("a.qt-property-thumb-style-7-gallery-image-wrapper").attr("href");
			if (!link) return;
			if (!link.startsWith("http")) link = "https://www.hi-residential.com" + link;

			const title = $prop.find("h3.qt-property-thumb-style-7-info-title").text().trim();
			if (!title) return;

			const availability = $prop.find("div.qt-property-thumb-style-7-availability").text().trim().toLowerCase();
			if (availability.includes("under offer") || (isRental && availability.includes("let agreed"))) return;

			const priceText = $prop.find("div.qt-property-thumb-style-7-price").text().trim();
			const priceMatch = priceText.match(/[\d,]+/);
			if (!priceMatch) return;
			const price = parseFloat(priceMatch[0].replace(/,/g, ""));

			const bedroomText = $prop.find("span.qt-property-thumb-style-7-info-bedrooms").text().trim();
			const bedrooms = bedroomText.match(/\d+/) ? parseInt(bedroomText.match(/\d+/)[0]) : null;

			properties.push({ link, title, price, bedrooms, isRental });
		} catch (e) { }
	});

	return properties;
}

// ============================================================================
// PROCESS SINGLE PROPERTY
// ============================================================================
async function processProperty(data, index, total, browserContext) {
    const { link, title, price, bedrooms, isRental } = data;
    if (!link || !price) return;

    const result = await updatePriceByPropertyURLOptimized(link, price, title, bedrooms, AGENT_ID, isRental);

    counts.totalScraped++;
    if (!result.error) {
        if (isRental) counts.savedRentals++;
        else counts.savedSales++;
    }

    const action = result.updated ? "UPDATED" : result.isExisting ? "UNCHANGED" : "CREATED";

    // ✅ Fetch coordinates for ALL properties (new, updated, AND unchanged)
    logger.property(index, "DETAIL", title.substring(0, 45),
        `£${price.toLocaleString()} → Fetching coordinates...`, link, isRental, total, "PROCESSING");

    let latitude = null;
    let longitude = null;
    const detailPage = await browserContext.newPage();

    try {
        await detailPage.goto(link, { waitUntil: 'domcontentloaded', timeout: 45000 });
        await detailPage.waitForTimeout(3000);

        const coords = await extractCoordinatesFromDetailPage(detailPage, title);
        latitude = coords.latitude;
        longitude = coords.longitude;

    } catch (err) {
        logger.error(`Failed to load detail page for "${title}": ${err.message}`);

        // Geocode fallback if page load fails
        logger.property(index, "GEOCODE", title.substring(0, 45),
            "Page load failed → falling back to geocoding", link, isRental, total, "FALLBACK");
        const coords = await geocodeAddress(title);
        latitude = coords.latitude;
        longitude = coords.longitude;

    } finally {
        await detailPage.close().catch(() => {});
    }

    // ✅ Log coordinates clearly in console
    const coordStatus = (latitude && longitude)
        ? `Lat: ${latitude.toFixed(6)} | Lng: ${longitude.toFixed(6)}`
        : "⚠️ No coordinates found";

    logger.property(index, "COORDS", title.substring(0, 45),
        `£${price.toLocaleString()} | ${coordStatus}`,
        link, isRental, total, latitude && longitude ? "SUCCESS" : "MISSING");

    // ✅ Always save coordinates to DB regardless of action
    await processPropertyWithCoordinates(
        link, price, title, bedrooms, AGENT_ID, isRental, null, latitude, longitude
    );

    if (!result.isExisting) counts.totalSaved++;

    // Final log
    logger.property(index, "HTML_PAGE", title.substring(0, 45),
        `£${price.toLocaleString()}`, link, isRental, total, action);
}

// ============================================================================
// LISTING PAGE HANDLER
// ============================================================================
async function handleListingPage({ page, request, enqueueLinks }) {
	const { pageNum, isRental, label, totalPages } = request.userData;

	logger.page(pageNum, label, request.url, totalPages);

	try {
		await page.waitForTimeout(2500);
		const html = await page.content();
		const properties = extractPropertiesFromHTML(html, isRental);

		logger.page(pageNum, label, `Found ${properties.length} properties`, totalPages);

		for (let i = 0; i < properties.length; i++) {
			await processProperty(properties[i], i + 1, properties.length, page.context());
		}

		// Next page
		const nextUrl = await page.evaluate(() =>
			document.querySelector("a.next.page-numbers")?.href || null
		);

		if (nextUrl && pageNum < totalPages) {
			await enqueueLinks({
				urls: [nextUrl],
				userData: { pageNum: pageNum + 1, isRental, label, totalPages }
			});
		}
	} catch (err) {
		logger.error(`Error on page ${pageNum}: ${err.message}`);
	}
}

// ============================================================================
// MAIN SCRAPER
// ============================================================================
async function scrapeHiResidential() {
	const scrapeStartTime = new Date();
	logger.step(`Starting scraper at ${scrapeStartTime.toISOString()}`);

	const startPage = process.argv[2] ? parseInt(process.argv[2], 10) : 1;

	const phases = [
		{ isRental: false, label: "SALES", url: `${BASE_URL}/?department=residential-sales&officeID=5` },
		{ isRental: true, label: "RENTALS", url: `${BASE_URL}/?department=residential-lettings&officeID=5` },
	];

	for (const phase of phases) {
		logger.step(`\n=== Processing ${phase.label} ===\n`);

		const crawler = new PlaywrightCrawler({
			launchContext: { launchOptions: { headless: true } },
			maxConcurrency: 1,
			navigationTimeoutSecs: 60,
			requestHandlerTimeoutSecs: 120,
			preNavigationHooks: [blockNonEssentialResources],
			requestHandler: handleListingPage,
		});

		await crawler.run([{
			url: phase.url,
			userData: {
				pageNum: startPage,
				isRental: phase.isRental,
				label: phase.label,
				totalPages: 50
			}
		}]);
	}

	logger.step(`\n✅ Finished → Scraped: ${counts.totalScraped} | Saved: ${counts.totalSaved}`);
	return { scrapeStartTime };
}

// ============================================================================
// RUN THE SCRAPER
// ============================================================================
(async () => {
	try {
		const { scrapeStartTime } = await scrapeHiResidential();
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		logger.step("Agent completed successfully");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error:", err);
		process.exit(1);
	}
})();