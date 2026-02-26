// Emoov scraper using Playwright with Crawlee
// Agent ID: 112
// Website: emoov.co.uk
// Usage:
// node backend/scraper-agent-112.js

const { PlaywrightCrawler, log } = require("crawlee");
const { processPropertyWithCoordinates } = require("./lib/db-helpers.js");
const { isSoldProperty, parsePrice } = require("./lib/property-helpers.js");

// Disable Crawlee's verbose logging
log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 112;
const EMOOV_API_BASE = "https://apiv2.emoov.co.uk:8443/api";
const EMOOV_API_KEY = process.env.EMOOV_API_KEY || "b8bcad0edf7c247f5e774b174c5fc452";

const stats = {
	totalScraped: 0,
	totalSaved: 0,
};

async function fetchEmoovPage(isRental, pageNumber, limit = 8) {
	const endpoint = isRental
		? `${EMOOV_API_BASE}/lettings/search`
		: `${EMOOV_API_BASE}/properties/search`;
	const payload = {
		location: "any-location",
		price: "",
		types: "all-types",
		bedrooms: "",
		radius: "this-only",
		orderby: "most-recent",
		page: pageNumber,
		limit,
	};

	const response = await fetch(endpoint, {
		method: "POST",
		headers: {
			"content-type": "application/json",
			apikey: EMOOV_API_KEY,
			referer: "https://emoov.co.uk/",
		},
		body: JSON.stringify(payload),
	});

	if (!response.ok) {
		throw new Error(`Emoov API request failed (${response.status}) on page ${pageNumber}`);
	}

	const result = await response.json();
	const properties = result?.data?.properties || [];
	const pagination = result?.data?.pagination || null;
	return { properties, pagination };
}

// ============================================================================
// BROWSERLESS SETUP
// ============================================================================

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

// ============================================================================
// DETAIL PAGE SCRAPING
// ============================================================================

function stripTags(text) {
	if (!text) return "";
	return text
		.replace(/<[^>]*>/g, " ")
		.replace(/\s+/g, " ")
		.trim();
}

async function scrapePropertyDetail(property, isRental) {
	try {
		const response = await fetch(property.link, {
			headers: {
				referer: "https://emoov.co.uk/",
				"user-agent":
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
			},
		});

		if (!response.ok) {
			throw new Error(`HTTP ${response.status} for ${property.link}`);
		}

		const html = await response.text();
		const latMatch = html.match(/"radius_lat"\s*:\s*"([0-9.-]+)"/);
		const lngMatch = html.match(/"radius_long"\s*:\s*"([0-9.-]+)"/);
		const h1Match = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
		const h2Match = html.match(/<h2[^>]*>([\s\S]*?)<\/h2>/i);
		const statusMatch = html.match(
			/<[^>]*class=["'][^"']*emoov_price_size[^"']*["'][^>]*>([\s\S]*?)<\/[^>]+>/i,
		);

		const detailData = {
			lat: latMatch ? parseFloat(latMatch[1]) : null,
			lng: lngMatch ? parseFloat(lngMatch[1]) : null,
			html,
			title:
				stripTags(h1Match?.[1]) || stripTags(h2Match?.[1]) || property.title || "Emoov Property",
			statusText: stripTags(statusMatch?.[1]).toLowerCase(),
		};

		const status = detailData.statusText || property.statusText || "";
		if (isSoldProperty(status)) {
			console.log(`    ⏭️ Skipping non-available: ${property.link} (${status})`);
			return;
		}

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			detailData.title,
			property.bedrooms || null,
			AGENT_ID,
			isRental,
			detailData.html,
			detailData.lat,
			detailData.lng,
		);

		stats.totalScraped++;
		stats.totalSaved++;
	} catch (error) {
		console.error(` Error scraping detail page ${property.link}:`, error.message);
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { isRental, label, area } = request.userData;
	console.log(`\n Loading [${label}] ${area}: ${request.url}`);

	try {
		await page.goto(request.url, { waitUntil: "domcontentloaded", timeout: 60000 });

		const properties = [];
		let pageNumber = 1;
		let totalPages = 1;

		do {
			const { properties: pageItems, pagination } = await fetchEmoovPage(isRental, pageNumber, 8);
			totalPages = pagination?.totalPages || totalPages;

			for (const item of pageItems) {
				const id = item?.id;
				const slug = item?.property_url;
				if (!id || !slug) continue;

				const link = isRental
					? `https://emoov.co.uk/letting/${id}/${slug}`
					: `https://emoov.co.uk/property/${id}/${slug}`;

				const priceText = isRental
					? item?.new_price_pcm || item?.new_price || ""
					: item?.new_price || item?.new_price_pcm || "";

				const statusText = (
					item?.listing_status_display ||
					item?.listing_status ||
					item?.original_listing_status ||
					""
				)
					.toString()
					.toLowerCase();

				const bedrooms = Number.isFinite(Number(item?.bedrooms)) ? Number(item.bedrooms) : null;

				const title = item?.portal_address || "Emoov Property";

				properties.push({ link, priceText, title, statusText, bedrooms });
			}

			console.log(`    📡 Loaded API page ${pageNumber}/${totalPages} (${pageItems.length} items)`);
			pageNumber++;
		} while (pageNumber <= totalPages);

		console.log(`    ✅ Finished API pagination - total properties found: ${properties.length}`);

		// De-duplicate
		const uniqueProperties = [];
		const seenLinks = new Set();
		for (const p of properties) {
			if (!seenLinks.has(p.link)) {
				seenLinks.add(p.link);
				uniqueProperties.push(p);
			}
		}

		console.log(`    Found ${uniqueProperties.length} unique properties on Emoov list for ${area}`);

		// Batch processing (sequential inside each batch) for stability on Browserless
		const batchSize = 10;
		for (let i = 0; i < uniqueProperties.length; i += batchSize) {
			const batch = uniqueProperties.slice(i, i + batchSize);
			console.log(
				`    🚀 Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(uniqueProperties.length / batchSize)}...`,
			);

			for (const property of batch) {
				try {
					if (isSoldProperty(property.statusText)) {
						continue;
					}

					const jitter = Math.floor(Math.random() * 1200) + 300;
					await new Promise((resolve) => setTimeout(resolve, jitter));

					const price = parsePrice(property.priceText);
					await scrapePropertyDetail({ ...property, price }, isRental);
				} catch (err) {
					console.error(`    ⚠️ Error processing ${property.link}: ${err.message}`);
				}
			}

			await page.waitForTimeout(500);
		}

		// Emoov doesn't seem to have standard pagination buttons (observed for London)
		// If it did, we would enqueue next page here.
	} catch (error) {
		console.error(`    ❌ Error handling listing page: ${error.message}`);
		if (error.stack)
			console.error(`    Stack: ${error.stack.split("\n").slice(0, 3).join("\n    ")}`);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	console.log(`\n Starting Agent ${AGENT_ID} - Emoov Scraper`);
	await markAllPropertiesRemovedForAgent(AGENT_ID);


	const crawler = new PlaywrightCrawler({
		requestHandler: handleListingPage,
		maxConcurrency: 1, // Stay subtle
		maxRequestRetries: 0,
		requestHandlerTimeoutSecs: 7200,
		browserPoolOptions: {
			useFingerprints: true,
		},
		launchContext: {
			launcher: require("playwright").chromium,
			launchOptions: {
				headless: true,
				// Use Browserless if available
				wsEndpoint: getBrowserlessEndpoint(),
			},
		},
	});

	// All properties across all locations
	const startUrls = [
		{
			url: "https://emoov.co.uk/find-a-property/any-location",
			userData: { label: "Sales", area: "All", isRental: false },
		},
		{
			url: "https://emoov.co.uk/find-a-letting/any-location",
			userData: { label: "Rentals", area: "All", isRental: true },
		},
	];

	await crawler.run(startUrls);

	console.log(`\n================================================================`);
	console.log(`🏁 AGENT ${AGENT_ID} FINISHED`);
	console.log(`✅ Total Scraped: ${stats.totalScraped}`);
	console.log(`✅ Total Saved: ${stats.totalSaved}`);
	console.log(`================================================================\n`);
	process.exit(0);
})();
