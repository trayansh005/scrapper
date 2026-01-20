// Pattinson scraper using Puppeteer with stealth plugin
// This bypasses Cloudflare detection better than Hero
// Install: npm install puppeteer puppeteer-extra puppeteer-extra-plugin-stealth

const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

puppeteer.use(StealthPlugin());

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const AGENT_ID = 125;
let totalScraped = 0;
let totalSaved = 0;

function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	const num = Number(price);
	if (isNaN(num)) return "N/A";
	return "£" + num.toLocaleString("en-GB");
}

const PROPERTY_TYPES = [
	{
		urlBase: "https://www.pattinson.co.uk/buy/property-search",
		totalPages: 100,
		isRental: false,
		label: "SALES",
	},
	{
		urlBase: "https://www.pattinson.co.uk/rent/property-search",
		totalPages: 13,
		isRental: true,
		label: "RENTALS",
	},
];

async function scrapePage(page, url, propertyType, pageNum) {
	try {
		console.log(`\n📋 ${propertyType.label} - Page ${pageNum} - ${url}`);

		// Navigate to page
		await page.goto(url, {
			waitUntil: "networkidle2",
			timeout: 60000,
		});

		// Wait for property cards to load
		try {
			await page.waitForSelector("a.row.m-0.bg-white", { timeout: 15000 });
			console.log("✅ Property cards loaded");
		} catch (e) {
			console.log("⚠️ Property cards selector not found, checking page content...");

			const pageContent = await page.evaluate(() => document.body.innerText);
			if (pageContent.includes("Verifying") || pageContent.includes("Just a moment")) {
				console.log("❌ Stuck on Cloudflare - waiting longer...");
				await delay(10000);

				// Try waiting for selector again
				try {
					await page.waitForSelector("a.row.m-0.bg-white", { timeout: 20000 });
				} catch (e2) {
					console.log("❌ Still no properties found");
					return [];
				}
			}
		}

		// Extract properties from page
		const properties = await page.evaluate(() => {
			const cards = document.querySelectorAll("a.row.m-0.bg-white");
			const results = [];

			cards.forEach((card) => {
				try {
					const href = card.getAttribute("href");
					const link = href ? "https://www.pattinson.co.uk" + href : null;

					const priceEl = card.querySelector("dt.display-5.text-primary");
					const price = priceEl ? priceEl.innerText.trim() : "";

					const titleEl = card.querySelector("div.text-primary-dark.fw-medium");
					const title = titleEl ? titleEl.innerText.trim() : "";

					const specs = card.querySelectorAll("div.d-flex.align-items-center");
					let bedrooms = null;
					if (specs.length > 0) {
						const bedroomEl = specs[0].querySelector("span.lh-1.fs-14");
						if (bedroomEl) {
							bedrooms = bedroomEl.innerText.trim();
						}
					}

					if (link) {
						results.push({ link, price, title, bedrooms });
					}
				} catch (e) {
					console.error("Error extracting card:", e.message);
				}
			});

			return results;
		});

		console.log(`🔗 Found ${properties.length} properties on page ${pageNum}`);
		return properties;
	} catch (err) {
		console.error(`❌ Error on page ${pageNum}: ${err.message}`);
		return [];
	}
}

async function extractCoordinates(page, link) {
	try {
		await page.goto(link, {
			waitUntil: "networkidle2",
			timeout: 30000,
		});

		// Extract coordinates from JSON-LD script
		const coords = await page.evaluate(() => {
			const scripts = document.querySelectorAll('script[type="application/ld+json"]');

			for (const script of scripts) {
				try {
					const data = JSON.parse(script.textContent);

					if (data && data["@type"] === "GeoCoordinates") {
						return { latitude: data.latitude, longitude: data.longitude };
					}
					if (data && data.geo && data.geo.latitude) {
						return { latitude: data.geo.latitude, longitude: data.geo.longitude };
					}
				} catch (e) {}
			}

			return { latitude: null, longitude: null };
		});

		return coords;
	} catch (err) {
		console.error(`⚠️ Error extracting coords: ${err.message}`);
		return { latitude: null, longitude: null };
	}
}

async function scrapePattinson() {
	console.log(`\n🚀 Starting Pattinson scraper with Puppeteer Stealth (Agent ${AGENT_ID})...\n`);

	// Launch browser with realistic settings
	const browser = await puppeteer.launch({
		headless: false, // Set to true once working
		args: [
			"--no-sandbox",
			"--disable-setuid-sandbox",
			"--disable-dev-shm-usage",
			"--disable-blink-features=AutomationControlled",
			"--window-size=1920,1080",
		],
		defaultViewport: {
			width: 1920,
			height: 1080,
		},
	});

	const page = await browser.newPage();

	// Set realistic headers
	await page.setExtraHTTPHeaders({
		"Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
		Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
	});

	// Override navigator properties to avoid detection
	await page.evaluateOnNewDocument(() => {
		Object.defineProperty(navigator, "webdriver", { get: () => undefined });
		Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
		Object.defineProperty(navigator, "languages", { get: () => ["en-GB", "en-US", "en"] });

		window.chrome = {
			runtime: {},
		};
	});

	// Open detail page in new tab for coordinate extraction
	const detailPage = await browser.newPage();
	await detailPage.setExtraHTTPHeaders({
		"Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
	});

	try {
		for (const propertyType of PROPERTY_TYPES) {
			console.log(`\n🏠 Processing ${propertyType.label} (${propertyType.totalPages} pages)\n`);

			for (let pageNum = 1; pageNum <= propertyType.totalPages; pageNum++) {
				const url = `${propertyType.urlBase}?p=${pageNum}`;

				const properties = await scrapePage(page, url, propertyType, pageNum);

				if (properties.length === 0) {
					console.log("⚠️ No properties found - skipping page");
					await delay(5000);
					continue;
				}

				// Process each property
				for (const property of properties) {
					if (!property.link) continue;

					// Extract coordinates from detail page
					const coords = await extractCoordinates(detailPage, property.link);

					// Save to database
					try {
						const priceClean = property.price ? property.price.replace(/[^0-9]/g, "").trim() : null;

						await updatePriceByPropertyURL(
							property.link,
							priceClean,
							property.title,
							property.bedrooms,
							AGENT_ID,
							propertyType.isRental,
							coords.latitude,
							coords.longitude
						);

						totalSaved++;
						totalScraped++;
						console.log(`✅ ${property.title} - ${formatPrice(priceClean)}`);
					} catch (dbErr) {
						console.error(`❌ DB error: ${dbErr.message}`);
					}

					// Human-like delay between properties
					await delay(1000 + Math.random() * 2000);
				}

				// Delay between pages
				await delay(2000 + Math.random() * 3000);

				// Take longer breaks periodically
				if (pageNum % 10 === 0) {
					console.log("☕ Taking a short break...");
					await delay(10000 + Math.random() * 5000);
				}
			}
		}

		console.log(`\n✅ Completed - Total scraped: ${totalScraped}, Total saved: ${totalSaved}`);
	} catch (error) {
		console.error("❌ Fatal error:", error.message);
		throw error;
	} finally {
		await browser.close();
	}
}

(async () => {
	try {
		await scrapePattinson();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n✅ All done!");
		process.exit(0);
	} catch (err) {
		console.error("❌ Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
