// Parry & Drewett scraper using Playwright with Crawlee
// Agent ID: 218
// Usage:
// node backend/scraper-agent-218.js [startPage]

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
} = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	formatPriceDisplay,
	extractCoordinatesFromHTML,
	extractBedroomsFromHTML,
} = require("./lib/property-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");
const { blockNonEssentialResources } = require("./lib/scraper-utils.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 218;
const logger = createAgentLogger(AGENT_ID);

const counts = {
	totalFound: 0,
	totalScraped: 0,
	totalSaved: 0,
	totalSkipped: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

function sleep(ms) {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

function getStartPage() {
	const value = process.argv[2] ? parseInt(process.argv[2], 10) : 1;
	if (!Number.isFinite(value) || value < 1) return 1;
	return Math.floor(value);
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

function extractLatLngFromGoogleUrl(url) {
	if (!url) return { latitude: null, longitude: null };

	const atMatch = url.match(/@(-?\d+\.\d+),(-?\d+\.\d+)/);
	if (atMatch) {
		return { latitude: parseFloat(atMatch[1]), longitude: parseFloat(atMatch[2]) };
	}

	const dMatch = url.match(/!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)/);
	if (dMatch) {
		return { latitude: parseFloat(dMatch[1]), longitude: parseFloat(dMatch[2]) };
	}

	return { latitude: null, longitude: null };
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

async function scrapePropertyDetail(browserContext, property, isRental) {
	const detailPage = await browserContext.newPage();

	try {
		await blockNonEssentialResources(detailPage);

		await detailPage.goto(property.link, {
			waitUntil: "domcontentloaded",
			timeout: 90000,
		});

		await detailPage.waitForTimeout(1000);

		const detailData = await detailPage.evaluate(() => {
			try {
				const descriptionEl = document.querySelector(
					"#_ctl7_lblPropertyDescription, .propertyDescription",
				);
				const description = descriptionEl ? descriptionEl.innerText.trim() : "";

				const images = Array.from(document.querySelectorAll('a[href*="agencies/"] img'))
					.map((img) => {
						const parent = img.parentElement;
						return parent && parent.href ? parent.href : img.src;
					})
					.filter((src) => src && (src.includes("/main/") || src.endsWith(".jpg")));

				const floorplans = Array.from(document.querySelectorAll('a[href*="showFloorPlan"]'))
					.map((a) => {
						const match = a.getAttribute("href").match(/'([^']+)'\s*,\s*'([^']+)'/);
						if (match) {
							return `http://powering2.expertagent.co.uk/Candidate/showFloorPlan.aspx?aid=${match[1]}&pid=${match[2]}`;
						}
						return null;
					})
					.filter(Boolean);

				const features = Array.from(
					document.querySelectorAll(".bulletPoints li, .propertyBulletPoints li"),
				)
					.map((li) => li.innerText.trim())
					.filter((t) => t);

				const mapLinkEl = document.querySelector("a[href*='maps.google']");
				const mapHref = mapLinkEl ? mapLinkEl.getAttribute("href") : "";
				const mapUrlMatch = mapHref.match(/window\.open\('([^']+)'/);
				const mapUrl = mapUrlMatch ? mapUrlMatch[1] : mapHref;

				const latitudeEl = document.querySelector("#hdnLatitude");
				const longitudeEl = document.querySelector("#hdnLongitude");
				const latitude = latitudeEl ? parseFloat(latitudeEl.value) : null;
				const longitude = longitudeEl ? parseFloat(longitudeEl.value) : null;

				return {
					description,
					images,
					floorplans,
					features,
					mapUrl,
					latitude,
					longitude,
					html: document.documentElement.innerHTML,
				};
			} catch (e) {
				return { error: e.message };
			}
		});

		if (detailData.error) {
			logger.warn(`Error on detail page: ${detailData.error}`);
			return;
		}

		let latitude = detailData.latitude;
		let longitude = detailData.longitude;

		if (latitude === null || longitude === null) {
			const coords = await extractCoordinatesFromHTML(detailData.html);
			latitude = coords.latitude;
			longitude = coords.longitude;
		}

		if ((latitude === null || longitude === null) && detailData.mapUrl) {
			let mapPage;
			try {
				mapPage = await browserContext.newPage();
				await blockNonEssentialResources(mapPage);

				await mapPage.goto(detailData.mapUrl, {
					waitUntil: "domcontentloaded",
					timeout: 45000,
				});

				try {
					await mapPage.waitForURL(/(@-?\d+\.\d+,-?\d+\.\d+|!3d-?\d+\.\d+!4d-?\d+\.\d+)/, {
						timeout: 10000,
					});
				} catch (e) {
					// Ignore and fall back to whatever URL we have
				}

				const finalUrl = mapPage.url();
				const coords = extractLatLngFromGoogleUrl(finalUrl);
				if (coords.latitude !== null && coords.longitude !== null) {
					latitude = coords.latitude;
					longitude = coords.longitude;
				}
			} catch (err) {
				logger.warn(`Map lookup failed: ${err?.message || err}`);
			} finally {
				if (mapPage) await mapPage.close();
			}
		}

		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			isRental,
			detailData.html,
			latitude,
			longitude,
		);

		counts.totalSaved++;
		if (isRental) counts.savedRentals++;
		else counts.savedSales++;
	} catch (error) {
		logger.error(`Error scraping detail page ${property.link}`, error);
	} finally {
		await detailPage.close();
	}
}

// ============================================================================
// CRAWLER CONFIGURATION
// ============================================================================

function createCrawler(browserWSEndpoint) {
	return new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 2,
		navigationTimeoutSecs: 90,
		requestHandlerTimeoutSecs: 300,
		sessionPoolOptions: { blockedStatusCodes: [] },
		launchContext: {
			launcher: undefined,
			launchOptions: {
				browserWSEndpoint,
				args: ["--no-sandbox", "--disable-setuid-sandbox"],
			},
		},
		preNavigationHooks: [
			async ({ page }) => {
				await blockNonEssentialResources(page);

				await page.setExtraHTTPHeaders({
					"user-agent":
						"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
				});
			},
		],
		requestHandler: handleListingPage,
		failedRequestHandler({ request }) {
			const { pageNum, label, isRental } = request.userData || {};
			logger.error(`Failed listing page: ${request.url}`, null, pageNum, label);
		},
	});
}

// ============================================================================
// REQUEST HANDLER
// ============================================================================

// ============================================================================
// REQUEST HANDLER
// ============================================================================

async function handleListingPage({ page, request, crawler }) {
	// isRental must come from userData
	const isRental =
		request.userData.isRental !== undefined
			? request.userData.isRental
			: request.url.includes("search-2") || request.url.includes("dep=2");

	const { pageNum = 1, label = "EXPERT_AGENT" } = request.userData || {};
	logger.page(pageNum, label, request.url, null);

	try {
		// Establish session if needed
		if (request.url.includes("parryanddrewett.com")) {
			await page.goto(request.url, { waitUntil: "domcontentloaded" });
			const iframeSrc = await page.evaluate(() => {
				const iframe = document.querySelector("iframe");
				return iframe ? iframe.src : null;
			});

			if (iframeSrc) {
				logger.page(pageNum, label, `Found iframe: ${iframeSrc}`, null);
				await crawler.addRequests([{ url: iframeSrc, userData: { isStart: true, isRental } }]);
				return;
			}
		}

		// Handle Expert Agent direct navigation
		if (request.userData.isStart) {
			logger.page(pageNum, label, `Establishing session on Expert Agent...`, null);
			await page.goto(request.url, { waitUntil: "domcontentloaded" });

			const searchBtn = page.getByRole("button", { name: "Search", exact: true });
			if ((await searchBtn.count()) > 0) {
				await searchBtn.click();
				await page.waitForLoadState("domcontentloaded");
			}
		}

		// Now we should be on the results page
		await page.waitForTimeout(2000);

		const properties = await page.evaluate((isRental) => {
			const items = Array.from(document.querySelectorAll("ul[id*='List'] li, .propertyListItem"));
			return items.map((item) => {
				const linkEl = item.querySelector("a[href*='propertyDetails2.aspx']");
				const titleEl = item.querySelector(
					".propListItemTemplateAdvertHeader, div[id*='lblAddress'], .propertyAddress",
				);
				const priceEl = item.querySelector(
					".propListItemTemplatePriceText, div[id*='lblPrice'], .propertyPrice",
				);
				const statusEl = item.querySelector(
					".propListItemTemplatePriority, div[id*='lblStatus'], .propertyStatus",
				);
				const descriptionEl = item.querySelector(".propListItemTemplateDescription");

				const href = linkEl ? linkEl.getAttribute("href") : null;
				const link = href ? new URL(href, window.location.href).href : null;
				const title = titleEl ? titleEl.innerText.trim() : "";
				const priceText = priceEl ? priceEl.innerText.trim() : "";
				const status = statusEl ? statusEl.innerText.trim() : "";
				const description = descriptionEl ? descriptionEl.innerText.trim() : "";

				return { link, title, priceText, status, description, isRental };
			});
		}, isRental);

		logger.page(pageNum, label, `Found ${properties.length} properties`, null);

		for (const property of properties) {
			if (!property.link || processedUrls.has(property.link)) continue;
			processedUrls.add(property.link);

			counts.totalFound++;

			// Skip sold properties
			if (isSoldProperty(property.status)) {
				logger.property(
					pageNum,
					label,
					property.title.substring(0, 40),
					formatPriceDisplay(null, isRental),
					property.link,
					isRental,
					null,
					"SKIPPED",
				);
				counts.totalSkipped++;
				continue;
			}

			const price = parsePrice(property.priceText);
			if (price === null) {
				counts.totalSkipped++;
				continue;
			}

			const bedrooms = extractBedroomsFromHTML(`${property.title} ${property.description || ""}`);

			const result = await updatePriceByPropertyURLOptimized(
				property.link,
				price,
				property.title,
				bedrooms,
				AGENT_ID,
				isRental,
			);

			let action = "UNCHANGED";

			if (result.updated) {
				action = "UPDATED";
				counts.totalSaved++;
				counts.totalScraped++;
				if (isRental) counts.savedRentals++;
				else counts.savedSales++;
			}

			// If new property, scrape full details immediately
			if (!result.isExisting && !result.error) {
				action = "CREATED";
				await scrapePropertyDetail(page.context(), 
					{
						...property,
						price,
						bedrooms,
					},
					isRental
				);
				counts.totalScraped++;
			} else if (result.error) {
				action = "ERROR";
				counts.totalSkipped++;
			} else if (result.isExisting) {
				counts.totalScraped++;
			}

			logger.property(
				pageNum,
				label,
				property.title.substring(0, 40),
				formatPriceDisplay(price, isRental),
				property.link,
				isRental,
				null,
				action,
			);

			if (action !== "UNCHANGED") {
				await sleep(500);
			}
		}

		// Handle pagination
		const nextLink = await page.evaluate(() => {
			const next = document.querySelector("a[id*='lnkNext']");
			if (next) return next.href;

			const allLinks = Array.from(document.querySelectorAll("a"));
			const nextByText = allLinks.find((a) => a.innerText.toLowerCase().includes("next page"));
			return nextByText ? nextByText.href : null;
		});

		if (nextLink && !nextLink.includes("javascript:")) {
			logger.page(pageNum, label, `Found next page link: ${nextLink}`, null);
			await crawler.addRequests([
				{ url: nextLink, userData: { isRental, pageNum: pageNum + 1, label } },
			]);
		} else if (nextLink && nextLink.includes("javascript:")) {
			logger.page(pageNum, label, `Found JS pagination. Clicking Next...`, null);
			try {
				const nextFound = (await page.locator("a[id*='lnkNext']").count()) > 0;
				if (nextFound) {
					await page.click("a[id*='lnkNext']");
				} else {
					await page.click("text=Next Page");
				}
				await page.waitForLoadState("domcontentloaded");
				await page.waitForTimeout(1000);

				const currentUrl = page.url();
				await crawler.addRequests([
					{
						url: currentUrl,
						uniqueKey: Math.random().toString(),
						userData: { isRental, pageNum: pageNum + 1, label },
					},
				]);
			} catch (e) {
				logger.warn(`Failed to click next: ${e.message}`);
			}
		}
	} catch (error) {
		logger.error(`Error processing page ${pageNum} for ${label}`, error);
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	try {
		logger.step(`Starting Parry & Drewett scraper (Agent ${AGENT_ID})...`);

		const startPage = getStartPage();
		const isPartialRun = startPage > 1;
		const scrapeStartTime = new Date();

		if (isPartialRun) {
			logger.step(
				`Partial run detected (startPage=${startPage}). Remove status update will be skipped.`,
			);
		}

		const browserWSEndpoint = getBrowserlessEndpoint();
		logger.step(`Connecting to browserless: ${browserWSEndpoint.split("?")[0]}`);

		const crawler = createCrawler(browserWSEndpoint);

		await crawler.run([
			{
				url: "http://www.parryanddrewett.com/index.php?page=property-search",
				userData: { isRental: false, pageNum: 1, label: "SALES" },
			},
			{
				url: "http://www.parryanddrewett.com/index.php?page=property-search-2",
				userData: { isRental: true, pageNum: 1, label: "RENTALS" },
			},
		]);

		logger.step(
			`Completed Parry & Drewett - Found: ${counts.totalFound}, Scraped: ${counts.totalScraped}, Saved: ${counts.totalSaved}, Skipped: ${counts.totalSkipped}, New sales: ${counts.savedSales}, New rentals: ${counts.savedRentals}`,
		);

		if (!isPartialRun) {
			logger.step("Updating remove status...");
			await updateRemoveStatus(AGENT_ID, scrapeStartTime);
		} else {
			logger.warn("Partial run detected. Skipping updateRemoveStatus.");
		}

		logger.step("All done!");
		process.exit(0);
	} catch (err) {
		logger.error("Fatal error", err);
		process.exit(1);
	}
})();
