const { PlaywrightCrawler } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db");

const AGENT_ID = 113;

const PROPERTY_TYPES = [
	// {
	// 	name: "residential-sales",
	// 	url: "https://www.carterjonas.co.uk/property-search?division=Homes&radius=10&area=GreaterLondon&searchTerm=Greater+London&toBuy=true&includeSoldOrSoldSTC=true&includeLetAgreedOrUnderOffer=true&sortOrder=HighestPriceFirst&page=1",
	// 	isRental: false,
	// },
	{
		name: "residential-lettings",
		url: "https://www.carterjonas.co.uk/property-search?division=Homes&radius=10&area=GreaterLondon&searchTerm=Greater+London&toBuy=false&includeSoldOrSoldSTC=true&includeLetAgreedOrUnderOffer=true&sortOrder=HighestPriceFirst&page=1",
		isRental: true,
	},
];

async function scrapeCarterJonas() {
	const crawler = new PlaywrightCrawler({
		maxConcurrency: 3,
		requestHandlerTimeoutSecs: 180,
		async requestHandler({ page, request }) {
			const { isRental, category } = request.userData;
			console.log(`Processing ${category}: ${request.url}`);

			// Handle cookie consent if visible
			const cookieButton = page.getByRole('button', { name: 'Accept All Cookies' });
			if (await cookieButton.isVisible()) {
				await cookieButton.click();
				await page.waitForTimeout(1000);
			}

			// Wait for results to load
			await page.waitForSelector("li .property-card, li [data-property-id], li h3", { timeout: 15000 }).catch(() => console.log("No properties found on this page."));

			const properties = await page.evaluate((isRent) => {
				// Each property seems to be in a listitem within a list
				const items = Array.from(document.querySelectorAll('ul li')).filter(li => li.querySelector('h3') && li.querySelector('h4'));
				
				return items.map((item) => {
					const titleEl = item.querySelector("h3");
					const priceEl = item.querySelector("h4");
					const linkEl = item.querySelector("h3 a");
					
					// Bedrooms are usually in a list with an icon
					// We'll look for the list item that has an icon and a number
					const specs = Array.from(item.querySelectorAll('ul li')).map(li => li.innerText.trim());
					let bedrooms = "";
					// Typically the first or second spec is bedrooms if it's just a number
					// Let's try to find a number that might be bedrooms
					// In the snapshot it was <li><img>text: "4"</li>
					const bedsMatch = specs.find(s => /^\d+$/.test(s));
					if (bedsMatch) bedrooms = bedsMatch;

					let price = "";
					if (priceEl) {
						const priceText = priceEl.textContent.trim();
						const match = priceText.replace(/,/g, "").match(/(\d+)/);
						if (match) price = match[1];
					}

					return {
						title: titleEl ? titleEl.textContent.trim() : "",
						link: linkEl ? linkEl.href : "",
						price: price,
						bedrooms: bedrooms,
						isRental: isRent,
					};
				});
			}, isRental);

			console.log(`Found ${properties.length} properties.`);

			for (const property of properties) {
				if (property.link) {
					let coords = { latitude: null, longitude: null };

					// Coordinate extraction from details page
					const detailPage = await page.context().newPage();
					try {
						await detailPage.goto(property.link, {
							waitUntil: "domcontentloaded",
							timeout: 30000,
						});

						// Wait a bit for meta tags or dynamic content
						await detailPage.waitForTimeout(1000);

						const geo = await detailPage.evaluate(() => {
							// Check meta tags first as they were very clear in the snapshot
							const latMeta = document.querySelector('meta[property="place:location:latitude"]');
							const lonMeta = document.querySelector('meta[property="place:location:longitude"]');
							
							if (latMeta && lonMeta) {
								return { lat: latMeta.content, lon: lonMeta.content };
							}

							// Fallback to regex on HTML if meta tags not found
							const html = document.documentElement.innerHTML;
							const latMatch = html.match(/"latitude":\s*(-?\d+\.\d+)/i);
							const lonMatch = html.match(/"longitude":\s*(-?\d+\.\d+)/i);
							
							return {
								lat: latMatch ? latMatch[1] : null,
								lon: lonMatch ? lonMatch[1] : null,
							};
						});

						if (geo.lat && geo.lon) {
							coords.latitude = parseFloat(geo.lat);
							coords.longitude = parseFloat(geo.lon);
							console.log(`  📍 Found coords for ${property.title}: ${coords.latitude}, ${coords.longitude}`);
						}
					} catch (err) {
						console.error(`  ⚠️ Error loading detail page for ${property.link}: ${err.message}`);
					} finally {
						await detailPage.close();
					}

					try {
						await updatePriceByPropertyURL(
							property.link,
							property.price,
							property.title,
							property.bedrooms,
							AGENT_ID,
							property.isRental,
							coords.latitude,
							coords.longitude
						);
					} catch (error) {
						console.error(`Error updating property ${property.link}:`, error.message);
					}
				}
			}

			// Pagination
			const nextButton = page.getByRole('button', { name: 'Next' });
			if (await nextButton.isVisible()) {
				// Click next button and wait for the URL to change or the next page to load
				// Actually, simpler to check if there is a next page link or just increment the URL
				const currentUrl = new URL(request.url);
				let pageNum = parseInt(currentUrl.searchParams.get("page") || "1");
				
				// Optional: Check if we are at the last page
				// The snapshot showed "1-12 of 227", so we can calculate
				const paginationInfo = await page.evaluate(() => {
					const el = document.querySelector('nav[aria-label="Pagination"] + div');
					if (el) {
						const match = el.innerText.match(/of\s+(\d+)/);
						return match ? parseInt(match[1]) : 0;
					}
					return 0;
				});

				const totalResults = paginationInfo;
				const resultsPerPage = properties.length || 12;
				const totalPages = Math.ceil(totalResults / resultsPerPage);

				if (pageNum < totalPages) {
					pageNum++;
					currentUrl.searchParams.set("page", pageNum.toString());
					const nextUrl = currentUrl.toString();
					console.log(`Heading to next page: ${nextUrl} (Page ${pageNum} of ${totalPages})`);
					await crawler.addRequests([
						{
							url: nextUrl,
							userData: { isRental, category },
						},
					]);
				}
			}
		},
	});

	for (const type of PROPERTY_TYPES) {
		await crawler.addRequests([
			{
				url: type.url,
				userData: { isRental: type.isRental, category: type.name },
			},
		]);
	}

	await crawler.run();
}

(async () => {
	try {
		console.log(`Starting Agent ${AGENT_ID} (Carter Jonas)...`);
		await scrapeCarterJonas();
		console.log("Updating remove status...");
		await updateRemoveStatus(AGENT_ID);
		console.log("Done.");
	} catch (error) {
		console.error("Fatal error:", error);
	} finally {
		process.exit(0);
	}
})();
