// Emoov scraper using API with native fetch
// Agent ID: 112
// Website: emoov.co.uk
// Usage:
// node backend/scraper-agent-112.js [startPage]

const { isSoldProperty, parsePrice, formatPriceUk } = require("./lib/property-helpers.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { updateRemoveStatus } = require("./db.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

const AGENT_ID = 112;
const EMOOV_API_BASE = "https://apiv2.emoov.co.uk:8443/api";
const EMOOV_API_KEY = process.env.EMOOV_API_KEY || "b8bcad0edf7c247f5e774b174c5fc452";

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

async function fetchEmoovPage(isRental, pageNumber, limit = 20) {
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

function stripTags(text) {
	if (!text) return "";
	return text
		.replace(/<[^>]*>/g, " ")
		.replace(/\s+/g, " ")
		.trim();
}

async function scrapePropertyDetail(property, isRental) {
	try {
		logger.step(`[Detail] Scraping coordinates: ${property.link}`);
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
			statusText: stripTags(statusMatch?.[1]) || "",
		};

		const status = (detailData.statusText || property.statusText || "").toLowerCase();
		if (isSoldProperty(status)) {
			logger.property(null, "DETAIL", property.title, property.price, property.link, isRental, null, "SKIPPED");
			stats.totalSkipped++;
			return;
		}

		const dbResult = await processPropertyWithCoordinates(
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

		logger.property(
			null,
			"DETAIL",
			detailData.title,
			formatPriceDisplay(property.price, isRental),
			property.link,
			isRental,
			null,
			dbResult.updated ? "UPDATED" : (dbResult.isExisting ? "UNCHANGED" : "CREATED"),
			detailData.lat,
			detailData.lng
		);
		stats.totalScraped++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}: ${error.message}`);
	}
}

async function run() {
	logger.step(`Starting Emoov API scraper (Agent ${AGENT_ID})`);
	if (isPartialRun) {
		logger.step(`Partial run starting from page ${startPageArgument}`);
	}

	const modes = [
		{ isRental: false, label: "SALES" },
		{ isRental: true, label: "RENTALS" },
	];

	for (const mode of modes) {
		logger.step(`Processing ${mode.label}...`);
		let pageNumber = startPageArgument;
		let totalPages = 1;

		do {
			logger.page(pageNumber, mode.label, `Fetching API page`, totalPages);
			try {
				const { properties: pageItems, pagination } = await fetchEmoovPage(
					mode.isRental,
					pageNumber,
					20,
				);
				totalPages = pagination?.totalPages || totalPages;

				for (const item of pageItems) {
					const id = item?.id;
					const slug = item?.property_url;
					if (!id || !slug) continue;

					const link = mode.isRental
						? `https://emoov.co.uk/letting/${id}/${slug}`
						: `https://emoov.co.uk/property/${id}/${slug}`;

					const priceText = mode.isRental
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
					const numericPrice = parsePrice(priceText);

					stats.totalFound++;

					if (isSoldProperty(statusText)) {
						logger.property(pageNumber, mode.label, title, formatPriceDisplay(numericPrice, mode.isRental), link, mode.isRental, totalPages, "SKIPPED");
						stats.totalSkipped++;
						continue;
					}

					// Optimized price check
					const priceCheck = await updatePriceByPropertyURLOptimized(link, numericPrice, title, null, AGENT_ID, mode.isRental);
					if (priceCheck.isExisting) {
						if (priceCheck.updated) {
							logger.property(pageNumber, mode.label, title, formatPriceDisplay(numericPrice, mode.isRental), link, mode.isRental, totalPages, "UPDATED");
							stats.totalSaved++;
							await sleep(50);
						} else {
							logger.property(pageNumber, mode.label, title, formatPriceDisplay(numericPrice, mode.isRental), link, mode.isRental, totalPages, "UNCHANGED");
						}
					} else {
						// New property, scrape details
						await scrapePropertyDetail(
							{ link, price: numericPrice, title, statusText, bedrooms },
							mode.isRental,
						);
					}
				}

				pageNumber++;
			} catch (err) {
				logger.error(`Failed to process page ${pageNumber}: ${err.message}`);
				break;
			}
		} while (pageNumber <= totalPages);
	}

	// Remove status update
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
