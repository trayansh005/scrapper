// Harrods Estates scraper using Playwright with Crawlee
// Agent ID: 215
// Website: harrodsestates.com
// Usage:
// node backend/scraper-agent-215.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
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

function getBrowserlessEndpoint() {
	return (
		process.env.BROWSERLESS_WS_ENDPOINT ||
		`ws://browserless-e44co4wws040gcokws8k0c00:3000?token=ssl0sRD6GX2dLgT69SlhLh25XREd17tv`
	);
}

function blockNonEssentialResources(page) {
	return page.route("**/*", (route) => {
		const resourceType = route.request().resourceType();
		if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
			return route.abort();
		}
		return route.continue();
	});
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

// Page 1 = base URL, page 2+ = /page-{n} (HTML pages, .ljson endpoint is dead)
function buildPageUrl(apiBase, pageNum) {
	if (pageNum <= 1) return apiBase;
	return `${apiBase}/page-${pageNum}`;
}

// ============================================================================
// CRAWLER
// ============================================================================

const crawler = new PlaywrightCrawler({
	maxConcurrency: 1,
	maxRequestRetries: 2,
	navigationTimeoutSecs: 90,
	requestHandlerTimeoutSecs: 600,
	preNavigationHooks: [
		async ({ page }) => {
			await blockNonEssentialResources(page);
			await page.setExtraHTTPHeaders({
				"User-Agent":
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
			});
		},
	],
	launchContext: {
		launcher: undefined,
		launchOptions: {
			browserWSEndpoint: getBrowserlessEndpoint(),
			args: ["--no-sandbox", "--disable-setuid-sandbox"],
		},
	},

	async requestHandler({ page, request, crawler }) {
		const { pageNum, isRental, label, startPage, apiBase } = request.userData;
		logger.page(pageNum, label, `Processing ${request.url}`, request.userData.totalPages || null);

		// Wait for JS to inject var propertyData into the page
		await new Promise((r) => setTimeout(r, 2000));

		// Extract var propertyData = {...} using balanced-brace parsing
		// (simple regex fails on deeply nested JSON)
		const data = await page.evaluate(() => {
			const scripts = Array.from(document.querySelectorAll("script"));
			for (const s of scripts) {
				const text = s.textContent || "";
				const startIdx = text.indexOf("var propertyData = {");
				if (startIdx < 0) continue;
				const braceStart = text.indexOf("{", startIdx);
				let depth = 0,
					i = braceStart,
					inStr = false,
					escape = false;
				for (; i < text.length; i++) {
					const c = text[i];
					if (escape) {
						escape = false;
						continue;
					}
					if (c === "\\" && inStr) {
						escape = true;
						continue;
					}
					if (c === '"' && !escape) {
						inStr = !inStr;
						continue;
					}
					if (inStr) continue;
					if (c === "{") depth++;
					else if (c === "}") {
						depth--;
						if (depth === 0) break;
					}
				}
				try {
					return JSON.parse(text.substring(braceStart, i + 1));
				} catch (e) {
					return null;
				}
			}
			return null;
		});

		if (!data || !Array.isArray(data.properties)) {
			logger.error(`No property payload found on page ${pageNum}`, null, pageNum, label);
			return;
		}

		const properties = data.properties;
		const totalCount = data.pagination?.total_count || properties.length;
		const pageSize = properties.length || 9;
		const discoveredTotalPages = Math.max(pageNum, Math.ceil(totalCount / pageSize));

		if (!request.userData.totalPages) {
			request.userData.totalPages = discoveredTotalPages;
		}

		// Queue remaining pages on first visit
		if (pageNum === startPage && discoveredTotalPages > pageNum) {
			const nextRequests = [];
			for (let p = pageNum + 1; p <= discoveredTotalPages; p++) {
				nextRequests.push({
					url: buildPageUrl(apiBase, p),
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

		logger.page(
			pageNum,
			label,
			`Found ${properties.length} properties (total: ${totalCount})`,
			discoveredTotalPages,
		);

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
				(item.price_value ?? item.price_without_qualifier ?? item.price ?? "").toString(),
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
			const priceCheck = await updatePriceByPropertyURLOptimized(
				link,
				numericPrice,
				title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			if (priceCheck.isExisting) {
				if (priceCheck.updated) {
					action = "UPDATED";
					stats.totalSaved++;
					stats.totalScraped++;
				}
			} else {
				await processPropertyWithCoordinates(
					link,
					numericPrice,
					title,
					bedrooms,
					AGENT_ID,
					isRental,
					"", // coords supplied directly from embedded JSON
					lat,
					lng,
				);
				action = "CREATED";
				stats.totalSaved++;
				stats.totalScraped++;
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
		logger.error(`Failed request: ${request.url}`, null, pageNum, label);
	},
});

// ============================================================================
// MAIN
// ============================================================================

async function run() {
	logger.step(`Starting Harrods Estates scraper (Agent ${AGENT_ID})`);
	const startPage = Math.max(1, startPageArgument || 1);

	const startUrls = PROPERTY_TYPES.map((type) => ({
		url: buildPageUrl(type.apiBase, startPage),
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
