// Parry & Drewett scraper using Playwright with Crawlee
// Agent ID: 218
// Usage:
// node backend/scraper-agent-218.js

const { PlaywrightCrawler, log } = require("crawlee");
const { updateRemoveStatus } = require("./db.js");
const { updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const {
	isSoldProperty,
	parsePrice,
	extractBedroomsFromHTML,
} = require("./lib/property-helpers.js");

log.setLevel(log.LEVELS.ERROR);

const AGENT_ID = 218;

const stats = {
	totalScraped: 0,
	totalSaved: 0,
	savedSales: 0,
	savedRentals: 0,
};

const processedUrls = new Set();

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

async function scrapePropertyDetail(browserContext, property) {
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
							// Expert Agent floorplan URL pattern:
							// showFloorPlan.aspx?aid={aid}&pid={pid}
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
			console.log(` Error on detail page: ${detailData.error}`);
			await detailPage.close();
			return;
		}

		let latitude = detailData.latitude;
		let longitude = detailData.longitude;

		if (latitude === null || longitude === null) {
			const { extractCoordinatesFromHTML } = require("./lib/property-helpers.js");
			const coords = await extractCoordinatesFromHTML(detailData.html);
			latitude = coords.latitude;
			longitude = coords.longitude;
		}

		if ((latitude === null || longitude === null) && detailData.mapUrl) {
			let mapPage;
			try {
				mapPage = await browserContext.newPage();
				await mapPage.route("**/*", (route) => {
					const resourceType = route.request().resourceType();
					if (["image", "font", "stylesheet", "media"].includes(resourceType)) {
						route.abort();
					} else {
						route.continue();
					}
				});

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
			} catch (e) {
				console.log(` Map lookup failed: ${e?.message || e}`);
			} finally {
				if (mapPage) await mapPage.close();
			}
		}

		const { processPropertyWithCoordinates } = require("./lib/db-helpers.js");
		await processPropertyWithCoordinates(
			property.link,
			property.price,
			property.title,
			property.bedrooms,
			AGENT_ID,
			property.isRent,
			detailData.html,
			latitude,
			longitude,
		);

		stats.totalSaved++;
		if (property.isRent) stats.savedRentals++;
		else stats.savedSales++;
	} catch (error) {
		console.error(` Error scraping detail page ${property.link}:`, error.message);
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
			console.error(`Request ${request.url} failed too many times.`);
		},
	});
}

async function handleListingPage({ page, request, crawler }) {
	// isRent must come from userData, which is set at the start and passed through all crawler.addRequests
	const isRent =
		request.userData.isRent !== undefined
			? request.userData.isRent
			: request.url.includes("search-2") || request.url.includes("dep=2");

	console.log(`\n🔍 Processing: ${request.url} (${isRent ? "Rent" : "Sale"})`);

	// Establish session if needed
	if (request.url.includes("parryanddrewett.com")) {
		await page.goto(request.url, { waitUntil: "domcontentloaded" });
		// The Expert Agent search is in an iframe on the main site, but we can't easily interact with it across origins.
		// So we'll navigate to the Expert Agent URL directly.
		const iframeSrc = await page.evaluate(() => {
			const iframe = document.querySelector("iframe");
			return iframe ? iframe.src : null;
		});

		if (iframeSrc) {
			console.log(` Found iframe: ${iframeSrc}`);
			await crawler.addRequests([{ url: iframeSrc, userData: { isStart: true, isRent } }]);
			return;
		}
	}

	// Handle Expert Agent direct navigation
	if (request.userData.isStart) {
		console.log(` Establishing session on Expert Agent...`);
		await page.goto(request.url, { waitUntil: "domcontentloaded" });

		// Wait for Search button
		const searchBtn = page.getByRole("button", { name: "Search", exact: true });
		if ((await searchBtn.count()) > 0) {
			await searchBtn.click();
			await page.waitForLoadState("domcontentloaded");
		}
	}

	// Now we should be on the results page
	await page.waitForTimeout(2000);

	const properties = await page.evaluate((isRent) => {
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

			return { link, title, priceText, status, description, isRent };
		});
	}, isRent);

	console.log(` Found ${properties.length} properties on page`);

	for (const property of properties) {
		if (!property.link || processedUrls.has(property.link)) continue;
		processedUrls.add(property.link);

		if (isSoldProperty(property.status)) {
			console.log(` Skipping sold/let property: ${property.title}`);
			continue;
		}

		const priceNum = parsePrice(property.priceText);
		if (priceNum === null) {
			console.log(` Skipping (no price): ${property.link}`);
			continue;
		}

		const bedrooms = extractBedroomsFromHTML(`${property.title} ${property.description || ""}`);

		const result = await updatePriceByPropertyURLOptimized(
			property.link,
			priceNum,
			property.title,
			bedrooms,
			AGENT_ID,
			isRent,
		);

		if (result.isExisting) {
			stats.totalScraped++;
			continue;
		}

		await scrapePropertyDetail(page.context(), {
			...property,
			price: priceNum,
			bedrooms: bedrooms,
		});
		stats.totalScraped++;
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
		console.log(` Found next page link: ${nextLink}`);
		await crawler.addRequests([{ url: nextLink, userData: { isRent } }]);
	} else if (nextLink && nextLink.includes("javascript:")) {
		// Some Expert Agent sites use __doPostBack
		console.log(` Found JS pagination. Clicking Next...`);
		try {
			const nextFound = (await page.locator("a[id*='lnkNext']").count()) > 0;
			if (nextFound) {
				await page.click("a[id*='lnkNext']");
			} else {
				await page.click("text=Next Page");
			}
			await page.waitForLoadState("domcontentloaded");
			await page.waitForTimeout(1000);
			// After click, the URL might not change, but content does.
			// We should re-trigger the handler's logic.
			// In Crawlee, we can't easily "re-run" the same request handler on the same page state without adding a new request.
			// But we can just loop here or add a dummy request.
			// For simplicity, let's see if we can get a direct URL from the page.
			const currentUrl = page.url();
			await crawler.addRequests([
				{ url: currentUrl, uniqueKey: Math.random().toString(), userData: { isRent } },
			]);
		} catch (e) {
			console.log(` Failed to click next: ${e.message}`);
		}
	}
}

// ============================================================================
// MAIN EXECUTION
// ============================================================================

(async () => {
	console.log(`\n🚀 Starting scraper for Agent ${AGENT_ID} (Parry & Drewett)`);
	const browserWSEndpoint = getBrowserlessEndpoint();
	const crawler = createCrawler(browserWSEndpoint);

	await crawler.run([
		{
			url: "http://www.parryanddrewett.com/index.php?page=property-search",
			userData: { isRent: false },
		},
		{
			url: "http://www.parryanddrewett.com/index.php?page=property-search-2",
			userData: { isRent: true },
		},
	]);

	await updateRemoveStatus(AGENT_ID);

	console.log(`\n✨ Scraping complete for Agent ${AGENT_ID}`);
	console.log(`📊 Stats:`);
	console.log(`   - Total Scraped: ${stats.totalScraped}`);
	console.log(
		`   - Total Saved:   ${stats.totalSaved} (Sales: ${stats.savedSales}, Rentals: ${stats.savedRentals})`,
	);
	process.exit(0);
})();
