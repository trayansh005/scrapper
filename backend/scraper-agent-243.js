const { PlaywrightCrawler } = require("crawlee");
const { updatePriceByPropertyURL, updateRemoveStatus } = require("./db");

const AGENT_ID = 243;
const BASE_URL = "https://www.dixonsestateagents.co.uk";

const PROPERTY_TYPES = [
	{
		type: "sales",
		url: "https://www.dixonsestateagents.co.uk/properties/sales/status-available/most-recent-first/",
		totalRecords: 710,
	},
	{
		type: "lettings",
		url: "https://www.dixonsestateagents.co.uk/properties/lettings/status-available/most-recent-first/",
		totalRecords: 170,
	},
];

const RECORDS_PER_PAGE = 10;

async function run() {
	const crawler = new PlaywrightCrawler({
		maxConcurrency: 1,
		maxRequestRetries: 5,
		requestHandlerTimeoutSecs: 120,
		navigationTimeoutSecs: 60,
		async requestHandler({ page, request, log, crawler }) {
			const { type, isPropertyPage, propertyData, pageNumber, totalPages, listBaseUrl } =
				request.userData;

			const delay = 1000;
			log.info(`Waiting ${delay}ms before processing ${request.url}`);
			await page.waitForTimeout(delay);

			if (isPropertyPage) {
				log.info(`Scraping details: ${request.url}`);
				const html = await page.content();

				const latMatch = html.match(/"latitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);
				const lonMatch = html.match(/"longitude":\s*([-+]?[0-9]*\.?[0-9]+)/i);

				const lat = latMatch ? parseFloat(latMatch[1]) : null;
				const lon = lonMatch ? parseFloat(lonMatch[1]) : null;

				log.info(`Extracted coordinates: ${lat}, ${lon}`);

				try {
					await updatePriceByPropertyURL(
						request.url,
						propertyData.price,
						propertyData.address,
						propertyData.bedrooms,
						AGENT_ID,
						type === "lettings",
						lat,
						lon
					);
				} catch (err) {
					log.warn(`DB update failed: ${request.url} (${err.message})`);
				}
				return;
			}

			log.info(
				`Processing list page: ${request.url}${
					pageNumber && totalPages ? ` (Page ${pageNumber}/${totalPages})` : ""
				}`
			);

			await page
				.waitForSelector(".card", { timeout: 15000 })
				.catch(() => log.warn("Cards not found on page"));

			const properties = await page.evaluate(
				({ agentId, type, baseUrl }) => {
					const results = [];
					const cards = document.querySelectorAll(".card");

					cards.forEach((card) => {
						const priceText = card.querySelector(".card__heading")?.innerText || "";
						if (!priceText) return;

						const address = card.querySelector(".card__text-content")?.innerText || "";

						const priceMatch = priceText.replace(/,/g, "").match(/\d+/);
						const price = priceMatch ? parseInt(priceMatch[0], 10) : 0;

						const specs = card.querySelectorAll(".card-content__spec-list-item");
						let bedrooms = null;
						specs.forEach((spec) => {
							if (spec.querySelector(".icon-bedroom")) {
								const val = spec.querySelector(".card-content__spec-list-number")?.innerText;
								if (val) bedrooms = parseInt(val, 10);
							}
						});

						const relativeUrl = card.querySelector("a.card__link")?.getAttribute("href");
						const propertyUrl = relativeUrl
							? relativeUrl.startsWith("http")
								? relativeUrl
								: baseUrl + relativeUrl
							: null;

						if (propertyUrl) {
							results.push({
								agent_id: agentId,
								property_url: propertyUrl,
								price,
								address,
								bedrooms,
								type,
							});
						}
					});
					return results;
				},
				{ agentId: AGENT_ID, type, baseUrl: BASE_URL }
			);

			log.info(`Found ${properties.length} properties on page.`);

			// Enqueue all properties found on this page
			for (const propertyData of properties) {
				await crawler.addRequests([
					{
						url: propertyData.property_url,
						uniqueKey: propertyData.property_url,
						userData: {
							isPropertyPage: true,
							type,
							propertyData,
						},
					},
				]);
			}

			// After enqueuing properties, enqueue the next list page
			if (
				Number.isInteger(pageNumber) &&
				Number.isInteger(totalPages) &&
				pageNumber < totalPages &&
				listBaseUrl
			) {
				const nextPage = pageNumber + 1;
				const nextUrl = `${listBaseUrl}page-${nextPage}#/`;
				await crawler.addRequests([
					{
						url: nextUrl,
						uniqueKey: `${type}-page-${nextPage}`,
						userData: {
							type,
							pageNumber: nextPage,
							totalPages,
							listBaseUrl,
						},
					},
				]);
			}
		},
	});

	for (const config of PROPERTY_TYPES) {
		console.log(`Starting crawl for ${config.type}...`);

		const totalPages = Math.ceil(config.totalRecords / RECORDS_PER_PAGE);
		const startPage = 55;

		await crawler.addRequests([
			{
				url: `${config.url}page-${startPage}#/`,
				uniqueKey: `${config.type}-page-${startPage}`,
				userData: {
					type: config.type,
					pageNumber: startPage,
					totalPages,
					listBaseUrl: config.url,
				},
			},
		]);
	}

	await crawler.run();

	for (const config of PROPERTY_TYPES) {
		await updateRemoveStatus(AGENT_ID, config.type === "lettings" ? 1 : 0);
	}

	console.log("Crawl finished.");
}

run().catch(console.error);
