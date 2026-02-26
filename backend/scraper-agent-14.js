// Chestertons scraper using Playwright with Crawlee
// Agent ID: 14
// Usage:
// node backend/scraper-agent-14.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus, markAllPropertiesRemovedForAgent } = require("./db.js");
const { formatPriceUk, updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { extractCoordinatesFromHTML, isSoldProperty } = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 14;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const recentPageSignatures = new Map();
const processedUrls = new Set();

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildPagedUrl(urlBase, pageNum) {
	if (pageNum <= 1) return urlBase;
	return `${urlBase}?page=${pageNum}`;
}

function formatPriceDisplay(price, isRental) {
	if (!price) return isRental ? "£0 pcm" : "£0";
	return `£${price}${isRental ? " pcm" : ""}`;
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

async function scrapePropertyDetail(browserContext, property) {
	await sleep(700);

	const detailPage = await browserContext.newPage();

	try {
		await detailPage.route("**/*", (route) => {
			const resourceType = route.request().resourceType();
			if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
				route.abort();
			} else {
				route.continue();
			}
		});

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 90000,
		});

		await detailPage.waitForTimeout(1500);

		const htmlContent = await detailPage.content();
		const coords = await extractCoordinatesFromHTML(htmlContent);

		return {
			coords: {
				latitude: coords.latitude || null,
				longitude: coords.longitude || null,
			},
		};
	} catch (error) {
		console.log(` Error scraping detail page ${property.link}: ${error.message}`);
		return null;
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request }) {
	const { pageNum, isRental, label } = request.userData;
	console.log(` [${label}] Page ${pageNum} - ${request.url}`);

	if (pageNum > 1) {
		const finalUrl = page.url();
		const expectedPageToken = `page=${pageNum}`;
		if (!finalUrl.includes(expectedPageToken)) {
			console.log(
				` Warning: requested page ${pageNum} but landed on ${finalUrl}. Pagination may be redirecting.`,
			);
		}
	}

	try {
		await page.waitForSelector(".pegasus-property-card", { timeout: 15000 });
	} catch (e) {
		console.log(` Listing container not found on page ${pageNum}`);
	}

	const properties = await page.evaluate((rentalMode) => {
		try {
			const results = [];
			const seenLinks = new Set();
			const expectedPathPart = rentalMode ? "/lettings/" : "/sales/";

			const cards = Array.from(document.querySelectorAll(".pegasus-property-card"));

			for (const card of cards) {
				const linkEl = card.querySelector("a[href*='/properties/']");
				let href = linkEl?.getAttribute("href");
				if (!href) continue;

				const link = href.startsWith("http") ? href : new URL(href, window.location.origin).href;
				if (!link.includes(expectedPathPart)) continue;
				if (seenLinks.has(link)) continue;
				seenLinks.add(link);

				const title = linkEl.getAttribute("title") || linkEl.textContent?.trim() || "Property";

				let priceRaw = "";
				const spanEls = Array.from(card.querySelectorAll("span"));
				for (const span of spanEls) {
					if (span.textContent.includes("£")) {
						priceRaw = span.textContent.trim();
						break;
					}
				}

				let bedText = "";
				const svgEls = Array.from(card.querySelectorAll("svg"));
				for (const svg of svgEls) {
					const titleTag = svg.querySelector("title");
					if (titleTag && titleTag.textContent === "Bedrooms") {
						bedText = svg.parentElement?.textContent?.trim() || "";
						break;
					}
				}

				const statusText = card.innerText || "";

				results.push({ link, title, priceRaw, bedText, statusText });
			}
			return results;
		} catch (e) {
			return [];
		}
	}, isRental);

	console.log(` Found ${properties.length} properties on page ${pageNum}`);

	const pageSignature = properties
		.map((p) => p.link)
		.slice(0, 5)
		.join("|");
	const signatureKey = isRental ? "LETTINGS" : "SALES";
	const previousSignature = recentPageSignatures.get(signatureKey);
	if (pageSignature && previousSignature === pageSignature) {
		console.log(
			` Warning: ${signatureKey} page ${pageNum} has the same leading links as previous page.`,
		);
	}
	recentPageSignatures.set(signatureKey, pageSignature);

	const batchSize = 2;
	for (let i = 0; i < properties.length; i += batchSize) {
		const batch = properties.slice(i, i + batchSize);

		await Promise.all(
			batch.map(async (property) => {
				if (!property.link) return;

				if (isSoldProperty(property.statusText || "")) return;

				if (processedUrls.has(property.link)) return;
				processedUrls.add(property.link);

				const price = formatPriceUk(property.priceRaw);
				let bedrooms = null;
				const bedMatch = property.bedText.match(/\d+/);
				if (bedMatch) bedrooms = parseInt(bedMatch[0]);

				if (!price) {
					console.log(` Skipping update (no price found): ${property.link}`);
					return;
				}

				const result = await updatePriceByPropertyURLOptimized(
					property.link,
					price,
					property.title,
					bedrooms,
					AGENT_ID,
					isRental,
				);

				if (result.updated) {
					stats.totalSaved++;
				}

				if (!result.isExisting && !result.error) {
					const detail = await scrapePropertyDetail(page.context(), property);

					await updatePriceByPropertyURL(
						property.link.trim(),
						price,
						property.title,
						bedrooms,
						AGENT_ID,
						isRental,
						detail?.coords?.latitude || null,
						detail?.coords?.longitude || null,
					);

					stats.totalSaved++;
					stats.totalScraped++;
					if (isRental) stats.savedRentals++;
					else stats.savedSales++;
				}

				const categoryLabel = isRental ? "LETTINGS" : "SALES";
				console.log(
					` [${categoryLabel}] ${property.title.substring(0, 40)} - ${formatPriceDisplay(
						price,
						isRental,
					)} - ${property.link}`,
				);
			}),
		);
		await sleep(500);
	}
}

// ============================================================================
// CRAWLER SETUP
// ============================================================================

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
			console.error(` Failed listing page: ${request.url}`);
		},
	});
}

// ============================================================================
// MAIN SCRAPER LOGIC
// ============================================================================

async function scrapeChestertons() {
	console.log(`\n Starting Chestertons scraper (Agent ${AGENT_ID})...\n`);

	const args = process.argv.slice(2);
	const startPage = args.length > 0 ? parseInt(args[0]) : 1;

	const PROPERTY_TYPES = [
		{
			urlBase: "https://www.chestertons.co.uk/properties/sales/status-available",
			isRental: false,
			label: "SALES",
			totalRecords: 1747,
			recordsPerPage: 12,
		},
		{
			urlBase: "https://www.chestertons.co.uk/properties/lettings/status-available",
			isRental: true,
			label: "LETTINGS",
			totalRecords: 1132,
			recordsPerPage: 12,
		},
	];

	const browserWSEndpoint = getBrowserlessEndpoint();
	console.log(` Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

	const crawler = createCrawler(browserWSEndpoint);

	const allRequests = [];

	for (const type of PROPERTY_TYPES) {
		const totalPages = Math.ceil(type.totalRecords / type.recordsPerPage);
		const effectiveStartPage = Math.max(1, startPage);

		for (let pg = effectiveStartPage; pg <= totalPages; pg++) {
			allRequests.push({
				url: buildPagedUrl(type.urlBase, pg),
				userData: {
					pageNum: pg,
					isRental: type.isRental,
					label: `${type.label}_PAGE_${pg}`,
				},
			});
		}
	}

	if (allRequests.length === 0) {
		console.log(" No pages to scrape with current arguments.");
		return;
	}

	console.log(` Queueing ${allRequests.length} listing pages starting from page ${startPage}...`);
	await crawler.addRequests(allRequests);
	await crawler.run();

	console.log(
		`\n Completed Chestertons - Total scraped: ${stats.totalScraped}, Total saved: ${stats.totalSaved}`,
	);
	console.log(` Breakdown - SALES: ${stats.savedSales}, LETTINGS: ${stats.savedRentals}`);
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		await scrapeChestertons();
		await updateRemoveStatus(AGENT_ID);
		console.log("\n All done!");
		process.exit(0);
	} catch (err) {
		console.error(" Fatal error:", err?.message || err);
		process.exit(1);
	}
})();
