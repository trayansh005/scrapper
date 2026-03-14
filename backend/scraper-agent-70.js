const { PlaywrightCrawler, sleep, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const { isSoldProperty, formatPriceUk, formatPriceDisplay } = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 70;
const logger = createAgentLogger(AGENT_ID);

const BASE_URL = "https://www.fineandcountry.co.uk";

const PROPERTY_TYPES = [
	{
		urlPath: "sales/property-for-sale",
		isRental: false,
		label: "SALES",
		totalPages: 355,
	},
	{
		urlPath: "lettings/property-to-rent",
		isRental: true,
		label: "LETTINGS",
		totalPages: 21,
	},
];

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
};

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

async function run() {
	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) || 1 : 1;
	const isPartialRun = startPage > 1;
	const scrapeStartTime = new Date();

	logger.step(`Starting Fine & Country Scraper (Agent ${AGENT_ID})...`);

	const browserWSEndpoint = getBrowserlessEndpoint();
	
	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,

		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
				viewport: { width: 1920, height: 1080 },
			},
		},

		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);
			},
		],

		requestHandler: async (context) => {
			const { request } = context;
			const { label } = request.userData;

			if (label.includes("_LIST")) {
				await handleListingPage(context);
			} else if (label.includes("_DETAIL")) {
				await handleDetailPage(context);
			}
		},

		failedRequestHandler: ({ request }) => {
			logger.error(`Request ${request.url} failed after ${request.retryCount} retries.`);
		},
	});

	const initialRequests = [];
	for (const type of PROPERTY_TYPES) {
		const pageToStart = Math.max(1, startPage);
		const url = `${BASE_URL}/${type.urlPath}/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${pageToStart}`;
		
		initialRequests.push({
			url,
			userData: {
				label: `${type.label}_LIST`,
				pageNum: pageToStart,
				totalPages: type.totalPages,
				isRental: type.isRental,
			},
		});
	}

	await crawler.run(initialRequests);

	logger.step(`Completed Fine & Country - Found: ${counts.totalFound}, Saved: ${counts.totalSaved}, Skipped: ${counts.totalSkipped}`);

	if (!isPartialRun) {
		logger.step("Finalizing maintenance (updateRemoveStatus)...");
		await updateRemoveStatus(AGENT_ID, scrapeStartTime);
	} else {
		logger.warn("Partial run detected. Bypassing updateRemoveStatus.");
	}

	logger.step("Scraper execution finished.");
}

async function handleListingPage({ request, page, crawler }) {
	const { label, pageNum, totalPages, isRental } = request.userData;
	logger.page(pageNum, label, request.url, totalPages);

	await page.waitForSelector(".cards-properties", { timeout: 30000 }).catch(() => null);

	const properties = await page.evaluate(() => {
		const cards = Array.from(document.querySelectorAll(".card-property"));
		return cards.map((card) => {
			const linkEl = card.querySelector(".property-title-link");
			const titleEl = card.querySelector(".property-title-link span");
			const priceEl = card.querySelector(".property-price .text-gold");
			
			const rooms = Array.from(card.querySelectorAll(".card__list-rooms li p")).map(p => p.textContent.trim());
			const bedroomsText = rooms.find(r => /^\s*\d+\s*$/.test(r) || r.toLowerCase().includes("bed"));

			let priceText = priceEl ? priceEl.textContent.trim() : "";
			if (priceEl && priceEl.querySelector(".converted_price")) {
				const temp = priceEl.cloneNode(true);
				const converted = temp.querySelector(".converted_price");
				if (converted) converted.remove();
				priceText = temp.textContent.trim();
			}

			return {
				link: linkEl ? linkEl.href : "",
				title: titleEl ? titleEl.textContent.trim() : "",
				priceText: priceText,
				bedrooms: bedroomsText || "",
			};
		});
	});

	logger.page(pageNum, label, `Found ${properties.length} properties on page ${pageNum}`, totalPages);

	for (const prop of properties) {
		if (!prop.link || !prop.title) continue;
		counts.totalFound++;

		if (isSoldProperty(prop.title)) {
			logger.property(pageNum, label, prop.title, prop.priceText, prop.link, isRental, totalPages, "SKIPPED");
			counts.totalSkipped++;
			continue;
		}

		const formattedPrice = formatPriceUk(prop.priceText);
		if (!formattedPrice) {
			logger.error(`Failed to parse price "${prop.priceText}" for ${prop.link}`, null, pageNum, label);
			counts.totalSkipped++;
			continue;
		}

		const result = await updatePriceByPropertyURLOptimized(
			prop.link,
			formattedPrice,
			prop.title,
			prop.bedrooms,
			AGENT_ID,
			isRental
		);

		let action = "UNCHANGED";
		if (result.updated) {
			action = "UPDATED";
			counts.totalSaved++;
		} else if (result.isExisting) {
			action = "UNCHANGED";
		}

		if (!result.isExisting && !result.error) {
			await crawler.addRequests([{
				url: prop.link,
				userData: {
					label: label.replace("_LIST", "_DETAIL"),
					pageNum,
					totalPages,
					isRental,
					propertyData: { ...prop, formattedPrice },
				},
			}]);
			action = "PENDING"; // Coordinate extraction will mark it CREATED
			await sleep(500); 
		} else if (result.error) {
			action = "ERROR";
			counts.totalSkipped++;
		}

		if (action !== "PENDING") {
			logger.property(pageNum, label, prop.title.substring(0, 50), formatPriceDisplay(formattedPrice, isRental), prop.link, isRental, totalPages, action);
		}
	}

	const hasNextPage = await page.evaluate(() => {
		const nextBtn = document.querySelector(".pagination .next, a[aria-label='Next']");
		return !!nextBtn;
	});

	if (hasNextPage) {
		const nextUrl = new URL(page.url());
		nextUrl.searchParams.set("page", (pageNum + 1).toString());
		await crawler.addRequests([{
			url: nextUrl.toString(),
			userData: { label, pageNum: pageNum + 1, totalPages, isRental },
		}]);
	}
}

async function handleDetailPage({ request, page }) {
	const { propertyData, label, pageNum, totalPages, isRental } = request.userData;
	logger.step(`[Detail] Scraping coordinates for: ${propertyData.title}`);

	const coords = await page.evaluate(() => {
		const scripts = Array.from(document.querySelectorAll('script[type="application/ld+json"]'));
		for (const script of scripts) {
			try {
				const data = JSON.parse(script.textContent);
				const findGeo = (obj) => {
					if (!obj || typeof obj !== "object") return null;
					if (obj["@type"] === "GeoCoordinates" && obj.latitude && obj.longitude) {
						return { latitude: parseFloat(obj.latitude), longitude: parseFloat(obj.longitude) };
					}
					if (Array.isArray(obj)) {
						for (const item of obj) {
							const res = findGeo(item);
							if (res) return res;
						}
					} else {
						for (const key in obj) {
							const res = findGeo(obj[key]);
							if (res) return res;
						}
					}
					return null;
				};
				const res = findGeo(data);
				if (res) return res;
			} catch (e) {}
		}
		return { latitude: null, longitude: null };
	});

	if (coords.latitude && coords.longitude) {
		logger.step(`[Detail] Found coordinates: ${coords.latitude}, ${coords.longitude}`);
	}

	await processPropertyWithCoordinates(
		propertyData.link,
		propertyData.formattedPrice,
		propertyData.title,
		propertyData.bedrooms,
		AGENT_ID,
		isRental,
		await page.content(), // Fallback to HTML extraction if JSON-LD failed or partially useful
		coords.latitude,
		coords.longitude
	);

	counts.totalSaved++;
	counts.totalScraped++;

	logger.property(
		pageNum,
		label.replace("_DETAIL", ""),
		propertyData.title.substring(0, 50),
		formatPriceDisplay(propertyData.formattedPrice, isRental),
		propertyData.link,
		isRental,
		totalPages,
		"CREATED",
		coords.latitude,
		coords.longitude
	);
}

run().catch((err) => {
	logger.error("Fatal error during scraper execution", err);
	process.exit(1);
});
