const Hero = require("@ulixee/hero");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db.js");

const AGENT_ID = 4; // Marsh & Parsons

const PROPERTY_TYPES = [
	{
		name: "Sales",
		baseUrl:
			"https://www.marshandparsons.co.uk/properties-for-sale/london/?filters=exclude_sold%2Cexclude_under_offer",
		isRent: false,
		totalPages: 30,
	},
];

// Memory monitoring
function logMemoryUsage(label) {
	const used = process.memoryUsage();
	console.log(
		`[${label}] Memory: ${Math.round(used.heapUsed / 1024 / 1024)}MB / ${Math.round(
			used.heapTotal / 1024 / 1024
		)}MB`
	);
}

// Process a single detail page with Hero
async function processDetailPage(hero, property) {
	const { url, title, location, priceRaw, bedrooms, isRent } = property;

	console.log(`Processing detail: ${url}`);

	let priceClean = priceRaw.replace(/[£,]/g, "");
	if (isRent && priceClean.includes("p/w")) {
		priceClean = priceClean.replace("p/w", "").trim();
	}
	const price = parseFloat(priceClean);

	try {
		// Navigate to detail page
		await hero.goto(url);

		// Wait for page to be fully loaded
		await hero.waitForMillis(2000);

		// Get page content
		const html = await hero.document.body.innerHTML;

		let latitude = null;
		let longitude = null;

		// Try to extract coordinates from the page content
		// Pattern 1: Google Maps URL
		const mapsMatch = html.match(/ll=([\d.-]+),([\d.-]+)/);
		// Pattern 2: JavaScript lat/lng
		const scriptMatch = html.match(/lat:\s*([\d.-]+),\s*lng:\s*([\d.-]+)/);
		// Pattern 3: JSON latitude/longitude
		const jsonMatch = html.match(/"latitude":\s*([\d.-]+),\s*"longitude":\s*([\d.-]+)/);

		if (mapsMatch) {
			latitude = parseFloat(mapsMatch[1]);
			longitude = parseFloat(mapsMatch[2]);
		} else if (scriptMatch) {
			latitude = parseFloat(scriptMatch[1]);
			longitude = parseFloat(scriptMatch[2]);
		} else if (jsonMatch) {
			latitude = parseFloat(jsonMatch[1]);
			longitude = parseFloat(jsonMatch[2]);
		}

		const fullTitle = `${title}, ${location}`;

		await updatePriceByPropertyURL(
			url,
			price,
			fullTitle,
			bedrooms,
			AGENT_ID,
			isRent,
			latitude,
			longitude
		);

		console.log(`✓ ${fullTitle} (£${price})`);
	} catch (error) {
		console.error(`✗ Failed ${url}: ${error.message}`);
	}
}

// Scrape a single listing page and extract property URLs
async function scrapeListingPage(hero, listingUrl, isRent) {
	console.log(`\n📋 Scraping listing page: ${listingUrl}`);

	try {
		await hero.goto(listingUrl);

		// Wait for content to load
		await hero.waitForMillis(2000);

		// Extract properties from the page
		const properties = await hero.document.querySelectorAll("div.my-4.shadow-md.rounded-xl");
		const propertyList = [];

		for (const card of properties) {
			try {
				const linkElement = await card.querySelector('a[href*="/property/"]');
				const titleElement = await card.querySelector("h3");
				const locationElement = await card.querySelector("p");

				const textContent = await card.textContent;
				const priceMatch = textContent.match(/£[0-9,]+(p\/w)?/);
				const priceRaw = priceMatch ? priceMatch[0] : null;

				const bedImg = await card.querySelector('img[alt="bed"]');
				let bedrooms = null;
				if (bedImg) {
					const parent = await bedImg.parentElement;
					const bedroomText = await parent.textContent;
					const bedroomMatch = bedroomText.trim().match(/\d+/);
					bedrooms = bedroomMatch ? parseInt(bedroomMatch[0]) : null;
				}

				const url = linkElement ? await linkElement.getAttribute("href") : null;
				const title = titleElement ? await titleElement.textContent : "";
				const location = locationElement ? await locationElement.textContent : "";

				if (url && priceRaw) {
					propertyList.push({
						url: url.startsWith("http") ? url : `https://www.marshandparsons.co.uk${url}`,
						title: title.trim(),
						location: location.trim(),
						priceRaw,
						bedrooms,
					});
				}
			} catch (err) {
				console.error(`Error extracting property card: ${err.message}`);
			}
		}

		console.log(`Found ${propertyList.length} properties`);

		// Process each property detail page
		for (const property of propertyList) {
			await processDetailPage(hero, { ...property, isRent });
		}

		return propertyList.length;
	} catch (error) {
		console.error(`Error scraping listing page: ${error.message}`);
		return 0;
	}
}

async function scrapeMarshParsons() {
	console.log(`Starting Marsh & Parsons Scraper with Hero (Agent ${AGENT_ID})...`);
	logMemoryUsage("START");

	const hero = new Hero({
		showChrome: false, // headless mode
		blockedResourceTypes: ["image", "media", "font", "stylesheet"], // Block unnecessary resources
		userAgent:
			"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
	});

	try {
		// ============================================
		// PROCESS PAGE BY PAGE
		// ============================================
		for (const type of PROPERTY_TYPES) {
			console.log(`\n📦 Processing ${type.name}...`);

			for (let pageNum = 1; pageNum <= type.totalPages; pageNum++) {
				const listingUrl = `${type.baseUrl}&page=${pageNum}`;
				console.log(`\n📄 Page ${pageNum}/${type.totalPages}`);

				try {
					await scrapeListingPage(hero, listingUrl, type.isRent);
					logMemoryUsage(`After page ${pageNum}`);

					// Small delay between listing pages
					await new Promise((resolve) => setTimeout(resolve, 1000));
				} catch (error) {
					console.error(`Error on page ${pageNum}: ${error.message}`);
				}
			}
		}

		console.log("\n✅ Scraping completed.");
		await updateRemoveStatus(AGENT_ID);
		logMemoryUsage("END");
	} catch (error) {
		console.error("❌ Fatal error:", error);
		throw error;
	} finally {
		// Clean up Hero instance
		await hero.close();
	}
}

// Run scraper
scrapeMarshParsons()
	.then(() => {
		console.log("✅ All done!");
		process.exit(0);
	})
	.catch((err) => {
		console.error("❌ Scraper error:", err);
		process.exit(1);
	});
