const { PlaywrightCrawler } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db");

const AGENT_ID = 242;

const PROPERTY_TYPES = [
	// {
	// 	name: "residential-sales",
	// 	url: "https://www.fennwright.co.uk/property-search/?address_keyword=&radius=&property_type=&officeID=&minimum_price=&maximum_price=&minimum_bedrooms=0&department=residential-sales",
	// 	isRental: false,
	// },
	{
		name: "residential-lettings",
		url: "https://www.fennwright.co.uk/property-search/?address_keyword=&radius=&property_type=&officeID=&minimum_rent=&maximum_rent=&minimum_bedrooms=0&department=residential-lettings",
		isRental: true,
	},
];

async function scrapeFennWright() {
	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		requestHandlerTimeoutSecs: 60,
		async requestHandler({ page, request }) {
			const { isRental, category } = request.userData;
			console.log(`Processing ${category}: ${request.url}`);

			await page
				.waitForSelector(".info-item", { timeout: 10000 })
				.catch(() => console.log("No .info-item found."));

			const properties = await page.evaluate((isRent) => {
				const items = Array.from(document.querySelectorAll(".info-item"));
				return items.map((item) => {
					const linkEl = item.querySelector("a.caption");
					const titleEl = item.querySelector("h3");
					const priceEl = item.querySelector(".price");
					const bedroomsEl = item.querySelector("figure");

					let bedrooms = "";
					if (bedroomsEl) {
						const match = bedroomsEl.textContent.match(/(\d+)/);
						if (match) bedrooms = match[1];
					}

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

						const geo = await detailPage.evaluate(() => {
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
							console.log(`  📍 Found coords: ${coords.latitude}, ${coords.longitude}`);
						}
					} catch (err) {
						console.error(`  ⚠️ Error loading detail page: ${err.message}`);
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

			// Pagination using Next button
			const nextButton = await page.$("a.next.page-numbers");
			if (nextButton) {
				const nextUrl = await nextButton.getAttribute("href");
				if (nextUrl) {
					console.log(`Heading to next page: ${nextUrl}`);
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
		console.log(`Starting Agent ${AGENT_ID} (Fenn Wright)...`);
		await scrapeFennWright();
		console.log("Updating remove status...");
		await updateRemoveStatus(AGENT_ID);
		console.log("Done.");
	} catch (error) {
		console.error("Fatal error:", error);
	} finally {
		process.exit(0);
	}
})();
