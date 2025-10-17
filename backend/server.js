const express = require("express");
const cors = require("cors");
const puppeteer = require("puppeteer-extra");
const StealthPlugin = require("puppeteer-extra-plugin-stealth");
puppeteer.use(StealthPlugin());
const pool = require("./db");

const axios = require("axios");
const cheerio = require("cheerio");
const vm = require("vm");

const app = express();

// Enable CORS (allow all origins) and handle preflight requests
app.use(cors({ origin: true, credentials: false }));
app.options("*", cors({ origin: true, credentials: false }));

app.use(express.json());

const PORT = 4080;

// In-memory runtime controls and logs (simple implementation)
const scraperState = {
	// agentId -> { running: boolean, stopRequested: boolean, logs: string[] }
};

function ensureAgentState(agentId) {
	if (!scraperState[agentId]) {
		scraperState[agentId] = { running: false, stopRequested: false, logs: [] };
	}
	return scraperState[agentId];
}

function agentLog(agentId, ...args) {
	const state = ensureAgentState(agentId);
	const msg = args.map((a) => (typeof a === "string" ? a : JSON.stringify(a))).join(" ");
	const ts = new Date().toISOString();
	const line = `[${ts}] ${msg}`;
	// keep last 1000 lines
	state.logs.push(line);
	if (state.logs.length > 1000) state.logs.shift();
	// also print to server console
	console.log(`AGENT-${agentId}: ${msg}`);
}

// Expose endpoints to read logs and request stop
app.get("/scraper-logs/:agent_id", (req, res) => {
	const id = parseInt(req.params.agent_id);
	if (isNaN(id)) return res.status(400).json({ error: "Invalid agent id" });
	const state = ensureAgentState(id);
	res.json({
		agent: id,
		running: state.running,
		stopRequested: state.stopRequested,
		logs: state.logs.slice(-200),
	});
});

app.post("/stop-scraper/:agent_id", (req, res) => {
	const id = parseInt(req.params.agent_id);
	if (isNaN(id)) return res.status(400).json({ error: "Invalid agent id" });
	const state = ensureAgentState(id);
	state.stopRequested = true;
	agentLog(id, "Stop requested via API");
	res.json({ agent: id, stopRequested: true });
});

// Function to fetch property price from URL
async function fetchPropertyPrice(page, url, agent_id) {
	try {
		console.log(url);
		//const url = "https://www.patrickgardner.com/property/beechcroft-ashtead-kt21/";
		const response = await page.goto(url, {
			waitUntil: "networkidle2",
			timeout: 800000,
		});
		// const finalUrl = page.url();
		// const wasRedirected = finalUrl !== url;
		// const status = response.status();
		// const isNotFound = [404, 410].includes(status);

		// if (wasRedirected || isNotFound) {
		//     console.log(`Page is not accessible. Status: ${status}, Redirected: ${wasRedirected}`);
		//     return { remove_status: 1, price: null };
		// }

		const pageContent = await page.content();
		const keywords = /offers over|sold stc|sold/i;
		//const hasKeyword = keywords.test(pageContent);
		const hasKeyword = null;

		if (hasKeyword) {
			console.log("Matched remove keywords: Offers Over, Sold STC, Sold");
			return { remove_status: 1, price: null };
		} else {
			// const match = pageContent.match(/£([\d,]+)/);
			// const price = match ? match[1] : null;
			const $ = cheerio.load(pageContent);
			if (agent_id == 5) {
				const matchText = $(".for-sale .sale").first().text();
				const match = matchText.match(/£([\d,]+)/);
				var price = match ? match[1] : null;

				var property_title = $(".sub_title h2").first().text();

				//const cleanPrice = price.replace('£', '');

				const bedroomSpan = $('label:contains("BEDROOMS:")').next("span").clone();
				bedroomSpan.find("i").remove();
				let bedroomsText = bedroomSpan.text().trim();
				// Clean to keep only numbers
				const bedroomsMatch = bedroomsText.match(/\d+/);
				var bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

				const match_map = pageContent.match(
					/new google\.maps\.LatLng\(\s*([-+]?\d*\.?\d+),\s*([-+]?\d*\.?\d+)\s*\)/
				);

				var latitude = null;
				var longitude = null;
				if (match_map) {
					latitude = match_map[1];
					longitude = match_map[2];
				}
			}

			if (agent_id == 3) {
				//console.log(url);
				//get price
				const matchText = $(".price-qualifier").first().text();
				const match = matchText.match(/£([\d,]+)/);
				var price = match ? match[1] : null;

				//Get title
				var property_title_first = $(".section-title").text();
				var property_title_second = $(".section-title .address-area-post").first().text();

				var property_title = property_title_first + " " + property_title_second;

				//Get bedroom
				var bedrooms_str = $(".list-info").find(".Bedrooms").first().text();
				var bedrooms = bedrooms_str.replace(" Bedrooms", "");

				// get lat & lng
				const latMatch = pageContent.match(/lat:\s*(-?\d+\.\d+)/);
				const lngMatch = pageContent.match(/lng:\s*(-?\d+\.\d+)/);

				var latitude = null;
				var longitude = null;
				if (latMatch && lngMatch) {
					latitude = parseFloat(latMatch[1]);
					longitude = parseFloat(lngMatch[1]);
				}
			}

			if (agent_id == 12) {
				//get price
				const matchText = $(".secondary .price").find("span").first().text();
				const matchPrice = matchText.match(/£([\d,]+)/);
				var price = matchPrice ? matchPrice[1] : null;

				//Get title
				var property_title = $("title").text();

				//Get bedroom
				var text_bedroom = $(".listing-heading .type").text(); // "3 bedroom terraced house"
				var bedrooms = parseInt(text_bedroom);

				// get lat & lng
				const href = $('a[title="Open this area in Google Maps (opens a new window)"]').attr(
					"href"
				);

				// Extract lat and lng using regex
				var match = href.match(/ll=([-.\d]+),([-.\d]+)/);

				var latitude = null;
				var longitude = null;
				if (match) {
					latitude = parseFloat(match[1]);
					longitude = parseFloat(match[2]);
				}
			}

			if (agent_id == 42) {
				//get price
				const matchText = $(".details__head-title").find(".price-qualifier").first().text();
				const matchPrice = matchText.match(/£([\d,]+)/);
				var price = matchPrice ? matchPrice[1] : null;

				//Get title
				var property_title = $(".details__head-title").find("h1").text().trim();

				//Get bedroom
				const bedroomText = $("li:has(i.icon-bedroom) span").first().text().trim();
				var bedrooms = parseInt(bedroomText, 10);

				// get lat & lng
				const href = $('a[title="Open this area in Google Maps (opens a new window)"]').attr(
					"href"
				);

				// Extract lat and lng using regex
				var match = href.match(/ll=([-.\d]+),([-.\d]+)/);

				var latitude = null;
				var longitude = null;
				if (match) {
					latitude = parseFloat(match[1]);
					longitude = parseFloat(match[2]);
				}
			}

			if (agent_id == 4) {
				//get price
				const matchText = $(".propertyTitleContainer").find(".big").first().text();
				const matchPrice = matchText.match(/£([\d,]+)/);
				var price = matchPrice ? matchPrice[1] : null;

				//Get title
				var property_title_1 = $(".propertyTitleContainer .col-sm-7").find("h1").text().trim();
				var property_title_2 = $(".propertyTitleContainer .col-sm-7").find("h4").text().trim();
				var property_title = property_title_1 + " " + property_title_2;

				//Get bedroom
				var bedrooms = $(".beds img").first()[0].nextSibling.nodeValue.trim();

				// get lat & lng
				const href = $('a[title="Open this area in Google Maps (opens a new window)"]').attr(
					"href"
				);

				// Extract lat and lng using regex
				var match = href.match(/ll=([-.\d]+),([-.\d]+)/);

				var latitude = null;
				var longitude = null;
				if (match) {
					latitude = parseFloat(match[1]);
					longitude = parseFloat(match[2]);
				}
			}

			if (agent_id == 71) {
				// Hawes & Co property scraping
				const priceElement = $(".property-price, .price").first();
				const matchPrice = priceElement.text().match(/£([\d,]+)/);
				var price = matchPrice ? matchPrice[1] : null;

				// Get title
				var property_title = $("h1, .property-title").first().text().trim();
				if (!property_title) {
					property_title = $("title").text().replace(" | Hawes & Co", "").trim();
				}

				// Get bedrooms
				const bedroomText = $(
					".property-features li:contains('bedroom'), .bedrooms, .property-info:contains('bedroom')"
				)
					.first()
					.text();
				const bedroomMatch = bedroomText.match(/(\d+)/);
				var bedrooms = bedroomMatch ? bedroomMatch[1] : null;

				// Get coordinates
				var latitude = null;
				var longitude = null;

				// Try to find coordinates in script tags or data attributes
				const coordMatch = pageContent.match(
					/lat['":\s]+([-+]?\d*\.?\d+)[,\s]+lng['":\s]+([-+]?\d*\.?\d+)/i
				);
				if (coordMatch) {
					latitude = parseFloat(coordMatch[1]);
					longitude = parseFloat(coordMatch[2]);
				} else {
					// Try alternative coordinate patterns
					const altCoordMatch = pageContent.match(
						/latitude['":\s]+([-+]?\d*\.?\d+)[,\s]+longitude['":\s]+([-+]?\d*\.?\d+)/i
					);
					if (altCoordMatch) {
						latitude = parseFloat(altCoordMatch[1]);
						longitude = parseFloat(altCoordMatch[2]);
					}
				}
			}

			if (agent_id == 13) {
				//get price
				const matchText = $(".details-panel__detail")
					.find(".details-panel__details-text-primary")
					.first()
					.text();
				const matchPrice = matchText.match(/£([\d,]+)/);
				var price = matchPrice ? matchPrice[1] : null;

				//Get title
				var property_title = $(".details-panel__title")
					.find(".details-panel__title-main")
					.first()
					.text();

				//Get bedroom
				var text_bedroom = $(".details-panel__title-sub").text(); // "3 bedroom terraced house"
				var bedrooms = parseInt(text_bedroom);

				// get lat & lng
				//const href = $('a[title="Open this area in Google Maps (opens a new window)"]').attr('href');

				const match_map = pageContent.match(
					/https:\/\/www\.google\.com\/maps\/search\/\?api=1&query=([-0-9.]+),([-0-9.]+)/g
				);

				// Extract lat and lng using regex
				let found = false;
				var latitude = (longitude = null);
				$('script[type="application/ld+json"]').each((i, el) => {
					const jsonText = $(el).html();
					const data = JSON.parse(jsonText);

					// Handle arrays or single objects
					const entries = Array.isArray(data) ? data : [data];

					for (const entry of entries) {
						if (entry.geo && entry.geo.latitude && entry.geo.longitude) {
							latitude = entry.geo.latitude;
							longitude = entry.geo.longitude;
							// console.log('Latitude:', entry.geo.latitude);
							// console.log('Longitude:', entry.geo.longitude);
							found = true;
							return false; // stop .each loop
						}
					}
				});
			}

			return {
				remove_status: 0,
				price,
				property_title,
				bedrooms,
				latitude,
				longitude,
			};
		}
	} catch (error) {
		console.error("Error fetching property price:", error);
		return { remove_status: 0, price: null };
	}
}

app.put("/update-property-price/:agent_id", async (req, res) => {
	try {
		const agent_id = req.params.agent_id;

		console.log("Fetching properties... of Agent - " + agent_id);

		// const [properties] = await pool.query(
		//     "SELECT id, property_url FROM property_for_sale WHERE agent_id > '2' AND is_price_not_found = '0' AND updated_at < NOW() - INTERVAL 2 DAY"
		// );

		const [properties] = await pool.query(
			"SELECT id, property_url FROM property_for_sale WHERE agent_id > '2' AND is_price_not_found = '0' AND updated_at < NOW() - INTERVAL 2 DAY"
		);

		console.log(`Found ${properties.length} properties.`);

		const browser = await puppeteer.launch({
			headless: "new",
			args: ["--no-sandbox", "--disable-setuid-sandbox"],
		});

		const page = await browser.newPage();

		for (const property of properties) {
			try {
				console.log(`Processing property ID: ${property.id}`);

				const result = await fetchPropertyPrice(page, property.property_url);

				if (result.remove_status === 1) {
					await pool.query(
						"UPDATE property_for_sale SET remove_status = '1', updated_at = NOW() WHERE id = ?",
						[property.id]
					);
					console.log(`Marked property ${property.id} as removed.`);
				} else if (result.price !== null) {
					await pool.query(
						"UPDATE property_for_sale SET price = ?, price_update_status = '1', is_price_not_found = '1', updated_at = NOW() WHERE id = ?",
						[result.price, property.id]
					);
					console.log(`Updated property ${property.id} with price ${result.price}`);
				} else {
					console.log(`No price found for property ${property.id}`);
				}
			} catch (error) {
				console.error(`Error processing property ID ${property.id}:`, error.message);
			}
		}

		await browser.close();

		res.status(200).json({ message: "Prices updated successfully!" });
	} catch (error) {
		console.error("Server error:", error);
		// clear running flags for all agents to avoid stale running state
		Object.keys(scraperState).forEach((k) => {
			scraperState[k].running = false;
			scraperState[k].stopRequested = false;
			scraperState[k].logs.push(`[${new Date().toISOString()}] Server error: ${error.message}`);
		});
		res.status(500).json({ error: error.message });
	}
});

async function createPropertyURL(link, agent_id) {
	try {
		if (link) {
			const [properties_url_rows] = await pool.query(
				"SELECT COUNT(*) as count FROM properties_url WHERE property_url = ?",
				[link]
			);

			if (properties_url_rows[0].count > 0) {
				console.log(`✅ URL already exists: ${link}`);
			} else {
				const insertQuery = "INSERT INTO properties_url (agent_id, property_url) VALUES (?, ?)";
				await pool.query(insertQuery, [agent_id, link]);
				console.log(`🆕 Created property of agent_id ${agent_id} Link - ${link}`);
			}
		}
	} catch (error) {
		console.error("Error :", error);
		//return { url: null };
	}
}

app.put("/get-property-url-by-listing-page/:agent_id", async (req, res) => {
	try {
		const agent_id = req.params.agent_id;

		if (agent_id == 5) {
			let page_no = 20;
			for (let i = 1; i <= page_no; i++) {
				const listing_url =
					"https://www.patrickgardner.com/property-search/page/" +
					i +
					"/?radius=1&availability[]=2&availability[]=6";

				const { data } = await axios.get(listing_url);
				const $ = cheerio.load(data);

				const links = [];

				$(".property-cols .work_box").each(async (index, element) => {
					const link = $(element).find("a").first().attr("href");

					const result = await createPropertyURL(link, agent_id);

					// const [properties_url_rows] = await pool.query(
					//     "SELECT COUNT(*) as count FROM properties_url WHERE property_url = ?",
					//     [link]
					// );
					// if (properties_url_rows[0].count > 0) {
					//     console.log(`✅ URL already exists: ${property_url}`);
					//     // URL already exists, no insert needed
					//     //return;
					// } else {
					//     await query = "INSERT INTO properties_url (agent_id, property_url) VALUES ('"+agent_id+"', '"+link+"')"
					//      pool.query(query);
					//     console.log(`Created property of agent_id ${agent_id}`);
					// }
				});
			}
		}

		if (agent_id == 3) {
			let page_no = 257;
			for (let i = 1; i <= page_no; i++) {
				const listing_url =
					"https://www.dexters.co.uk/property-sales/properties-available-for-sale-in-london/page-" +
					i;
				//const { data } = await axios.get(listing_url);

				const { data } = await axios.get(
					"https://www.dexters.co.uk/property-sales/properties-available-for-sale-in-london/page-" +
						i,
					{
						headers: {
							"User-Agent":
								"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
						},
					}
				);

				const $ = cheerio.load(data);

				const links = [];

				$(".result .result-content").each(async (index, element) => {
					const link = "https://www.dexters.co.uk" + $(element).find("a").first().attr("href");

					const result = await createPropertyURL(link, agent_id);
				});
			}
		}

		//Agent - Purple Bricks
		if (agent_id == 12) {
			let page_no = 92;
			for (let i = 1; i <= page_no; i++) {
				const listing_url =
					"https://www.purplebricks.co.uk/search/property-for-sale/greater-london/london?page=" +
					i +
					"&sortBy=2&searchType=ForSale&searchRadius=2&soldOrLet=false&location=london&latitude=51.5072178&longitude=-0.1275862&betasearch=true";
				//const { data } = await axios.get(listing_url);

				const { data } = await axios.get(listing_url, {
					headers: {
						"User-Agent":
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
					},
				});

				const $ = cheerio.load(data);

				const links = [];

				$(".search-resultsstyled__StyledSearchResultsContainer-krg5hu-1 li").each(
					async (index, element) => {
						const link =
							"https://www.purplebricks.co.uk" +
							$(element).find("a").first().attr("href") +
							"#/view/map";
						const result = await createPropertyURL(link, agent_id);
					}
				);
			}
		}

		//Agent - Acorn
		if (agent_id == 42) {
			let page_no = 98;
			for (let i = 1; i <= page_no; i++) {
				const listing_url =
					"https://www.acorngroup.co.uk/property-search/properties-available-for-sale-more-than-25-miles-of-london/page-" +
					i;

				const { data } = await axios.get(listing_url, {
					headers: {
						"User-Agent":
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
					},
				});

				const $ = cheerio.load(data);

				const links = [];

				$(".card .images").each(async (index, element) => {
					const link = "https://www.acorngroup.co.uk" + $(element).find("a").first().attr("href");
					const result = await createPropertyURL(link, agent_id);
				});
			}
		}

		//Agent - Marsh & Parsons
		if (agent_id == 4) {
			let page_no = 20;
			for (let i = 1; i <= page_no; i++) {
				const listing_url =
					"https://www.marshandparsons.co.uk/properties-for-sale/london/?filters=exclude_sold%2Cexclude_under_offer&page=" +
					i;

				const { data } = await axios.get(listing_url, {
					headers: {
						"User-Agent":
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
					},
				});

				const $ = cheerio.load(data);

				const links = [];

				$(".flex-none .my-4").each(async (index, element) => {
					const link = $(element).find("a").first().attr("href");
					const result = await createPropertyURL(link, agent_id);
				});
			}
		}

		//Agent - Bairstow Eves
		if (agent_id == 13) {
			let page_no = 55;
			for (let i = 1; i <= page_no; i++) {
				const listing_url =
					"https://www.bairstoweves.co.uk/properties/sales/status-available/page-" + i + "#/";

				const { data } = await axios.get(listing_url, {
					headers: {
						"User-Agent":
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
					},
				});

				const browser = await puppeteer.launch();
				const page = await browser.newPage();

				await page.goto(listing_url, { waitUntil: "networkidle2" }); // change to your URL

				const content = await page.content();
				const $ = cheerio.load(content);

				const cards = $(".hf-property-results .card");
				console.log("Total cards found:", cards.length);

				cards.each(async (index, element) => {
					const link = "https://www.bairstoweves.co.uk" + $(element).find("a").first().attr("href");
					const result = await createPropertyURL(link, agent_id);
				});

				await browser.close();
			}
		}

		//Agent - Marsh & Parsons
		if (agent_id == 4) {
			let page_no = 20;
			for (let i = 1; i <= page_no; i++) {
				const listing_url =
					"https://www.marshandparsons.co.uk/properties-for-sale/london/?filters=exclude_sold%2Cexclude_under_offer&page=" +
					i;

				const { data } = await axios.get(listing_url, {
					headers: {
						"User-Agent":
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
					},
				});

				const $ = cheerio.load(data);

				const links = [];

				$(".flex-none .my-4").each(async (index, element) => {
					const link = $(element).find("a").first().attr("href");
					const result = await createPropertyURL(link, agent_id);
				});
			}
		}

		console.log("Done all " + agent_id);
	} catch (error) {
		console.error("Server error:", error);
		res.status(500).json({ error: error.message });
	}
});

app.put("/get-and-update-property-delail/:agent_id", async (req, res) => {
	try {
		const agent_id = req.params.agent_id;
		console.log("Now Start processing.. property data scrapping & saving..");

		const [properties] = await pool.query(
			"SELECT id, property_url FROM properties_url WHERE agent_id = '" + agent_id + "'"
		);

		console.log(`Found ${properties.length} properties.`);

		const browser = await puppeteer.launch({
			headless: "new",
			args: ["--no-sandbox", "--disable-setuid-sandbox"],
		});

		const page = await browser.newPage();

		for (const property of properties) {
			try {
				console.log(`Processing property ID: ${property.id}`);

				const [properties_url_rows] = await pool.query(
					"SELECT COUNT(*) as count FROM property_for_sale_new WHERE property_url = ?",
					[property.property_url]
				);

				if (properties_url_rows[0].count > 0) {
					console.log(`✅ URL already exists: ${property.property_url}`);
				} else {
					const result = await fetchPropertyPrice(page, property.property_url, agent_id);
					console.log(result);
					if (result.price !== null) {
						var logo = "property_for_sale/logo.png";
						const insertQuery =
							"INSERT INTO property_for_sale_new (property_name, agent_id, price, bedrooms, latitude, longitude, property_url, logo) VALUES (?, ? ,?, ?, ?, ?, ?, ?)";
						await pool.query(insertQuery, [
							result.property_title,
							agent_id,
							result.price,
							result.bedrooms,
							result.latitude,
							result.longitude,
							property.property_url,
							logo,
						]);

						//console.log(`Created property of agent_id ${agent_id}`);
						console.log(`Created property ${property.id} with price ${result.price}`);
					} else {
						console.log(`No price found for property ${property.id}`);
					}
				}
			} catch (error) {
				console.error(`Error processing property ID ${property.id}:`, error.message);
			}
		}

		await browser.close();

		res.status(200).json({ message: "Prices updated successfully!" });
	} catch (error) {
		console.error("Server error:", error);
		res.status(500).json({ error: error.message });
	}

	console.log("Done all");
});

async function updatePriceByPropertyURL(
	link,
	price,
	title,
	bedrooms,
	agent_id,
	is_rent = false,
	latitude = null,
	longitude = null
) {
	try {
		if (link) {
			//console.log(title, bedrooms);
			// const [result] = await pool.query(
			//   `UPDATE property_for_sale
			//    SET price = ?, updated_at = NOW()
			//    WHERE property_url = ?`,
			//   [price, link.trim()]
			// );

			// if (result.affectedRows > 0) {
			//   console.log(`✅ Successfully updated property: ${link} with price: ${price}`);
			// } else {
			//   console.log(`⚠️ No property updated for URL: ${link} — maybe it doesn't exist or already has same data.`);
			// }
			let tableName = "property_for_sale";
			if (is_rent) {
				tableName = "property_for_rent";
			}
			console.log(tableName);
			var link = link.trim();
			const [properties_url_rows] = await pool.query(
				`SELECT COUNT(*) as count FROM ${tableName} WHERE property_url = ?`,
				[link]
			);

			if (properties_url_rows[0].count > 0) {
				const [result] = await pool.query(
					`UPDATE ${tableName}
                    SET price = ?, latitude = ?, longitude = ?, updated_at = NOW()
                    WHERE property_url = ?`,
					[price, latitude, longitude, link.trim()]
				);

				if (result.affectedRows > 0) {
					console.log(
						`✅ Successfully updated property: ${link} with price: ${price} latitude: ${latitude}longitude: ${longitude}`
					);
				} else {
					console.log(
						`⚠️ No property updated for URL: ${link} — maybe it doesn't exist or already has same data.`
					);
				}
			} else {
				const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price,  bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

				var logo = "property_for_sale/logo.png"; // set logo static
				const currentTime = new Date(); // get current timestamp

				await pool.query(insertQuery, [
					title,
					agent_id,
					price,
					bedrooms,
					link,
					logo,
					latitude,
					longitude,
					currentTime,
					currentTime,
				]);
				console.log(`🆕 Created property of agent_id ${agent_id} Link - ${link}`);
			}
		}
	} catch (error) {
		console.error("Error :", error);
		//return { url: null };
	}
}

async function updateRemoveStatus(agent_id) {
	try {
		const remove_status = 1;
		const [result] = await pool.query(
			`UPDATE property_for_sale
            SET remove_status = ? WHERE agent_id = ? AND updated_at < NOW() - INTERVAL 1 DAY`,
			[remove_status, agent_id]
		);
		console.log(`Removed sold or disable property`);
	} catch (error) {
		console.error("Error :", error);
		//return { url: null };
	}
}

app.put("/get-property-url-by-listing-page-and-update-price/:agent_id", async (req, res) => {
	try {
		const agent_id = parseInt(req.params.agent_id);

		// Validate agent_id
		if (!agent_id || isNaN(agent_id)) {
			return res.status(400).json({ error: "Invalid agent_id provided" });
		}

		// Supported agents with working code
		const supportedAgents = [
			5, 3, 12, 42, 4, 13, 71, 111, 63, 103, 116, 118, 134, 135, 107, 70, 208, 207,
		];

		if (!supportedAgents.includes(agent_id)) {
			return res.status(400).json({
				error: `Agent ID ${agent_id} is not supported or has no implementation`,
				supportedAgents: supportedAgents,
			});
		}

		const agent_ids = [agent_id];

		console.log(`Start script for agent ID: ${agent_id}`);
		for (const agent_id of agent_ids) {
			// initialize agent runtime state
			const state = ensureAgentState(agent_id);
			state.running = true;
			state.stopRequested = false; // clear previous stop requests
			agentLog(agent_id, "Starting scraper for agent");
			if (agent_id == 71) {
				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link) => {
					try {
						const { data } = await axios.get(link, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});
						const $ = cheerio.load(data);
						const html = $.html();
						const latMatch = html.match(/<!--property-latitude:"([^"]+)"-->/);
						const lngMatch = html.match(/<!--property-longitude:"([^"]+)"-->/);
						return {
							latitude: latMatch ? parseFloat(latMatch[1]) : null,
							longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
						};
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape Hawes & Co properties
				const scrapeHawesAndCoProperties = async (
					is_rent,
					totalRecords,
					recordsPerPage,
					urlPath,
					typeLabel
				) => {
					const totalPages = Math.ceil(totalRecords / recordsPerPage);

					console.log(
						`🏠 Scraping Hawes & Co ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					for (let i = 1; i <= totalPages; i++) {
						const listing_url = `https://www.hawesandco.co.uk/${urlPath}/all-properties/!/page/${i}`;

						try {
							const { data } = await axios.get(listing_url, {
								headers: {
									"User-Agent":
										"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
								},
							});
							const $ = cheerio.load(data);

							const properties = $(".property");
							console.log(
								`✅ Found ${
									properties.length
								} ${typeLabel.toLowerCase()} properties on page ${i}/${totalPages}`
							);

							for (let index = 0; index < properties.length; index++) {
								const element = properties.eq(index);
								try {
									// Extract link from data-link attribute
									const dataLink = $(element).find(".inner_wrapper").attr("data-link");
									let link = null;
									if (dataLink) {
										link = dataLink.startsWith("http")
											? dataLink
											: `https://www.hawesandco.co.uk${dataLink}`;
									}

									// Extract price from .sale_price
									const priceElement = $(element).find(".sale_price");
									let price = null;
									if (priceElement.length > 0) {
										const priceText = priceElement.text().trim();
										const priceMatch = priceText.match(/£([\d,]+)/);
										if (priceMatch) {
											price = priceMatch[1].replace(/,/g, "");
										}
									}

									// Extract title from .blurb
									let title = $(element).find(".blurb").text().trim();
									if (!title) {
										title = $(element).find(".info_section__header__left a").text().trim();
									}

									// Extract bedrooms from .info_section__room.beds
									let bedrooms = null;
									const bedroomElement = $(element).find(".info_section__room.beds");
									if (bedroomElement.length > 0) {
										const bedroomText = bedroomElement.text().trim();
										const bedroomMatch = bedroomText.match(/(\d+)/);
										if (bedroomMatch) {
											bedrooms = bedroomMatch[1];
										}
									}

									// Get latitude and longitude from detail page
									let latitude = null;
									let longitude = null;
									if (link) {
										const coords = await getLatLongFromDetail(link);
										latitude = coords.latitude;
										longitude = coords.longitude;
										// Delay to avoid rate limiting
										await new Promise((resolve) => setTimeout(resolve, 1000));
									}

									if (price && title && link) {
										await updatePriceByPropertyURL(
											link,
											price,
											title,
											bedrooms,
											agent_id,
											is_rent,
											latitude,
											longitude
										);
										console.log(`✅ Processed: ${title} - £${price}`);
									} else {
										console.log(`⚠️ Missing data for property: ${title || "Unknown"}`);
									}

									// Extract address from .address
									let address = $(element).find(".address").text().trim();

									// Just log for testing
									// console.log(`\n🏠 ${typeLabel.toUpperCase()} PROPERTY #${index + 1}:`);
									// console.log(`📍 Title: ${title || "Title not found"}`);
									// console.log(`🏠 Address: ${address || "Address not found"}`);
									// console.log(`💰 Price: ${price ? "£" + price : "Price not found"}`);
									// console.log(`🛏️ Bedrooms: ${bedrooms || "Bedrooms not found"}`);
									// console.log(`🔗 URL: ${link || "Link not found"}`);
									// console.log(`🏢 Agent ID: ${agent_id}`);
									// console.log(`📊 Type: ${typeLabel}`);
									// console.log(`Latitude: ${latitude || "Not found"}`);
									// console.log(`Longitude: ${longitude || "Not found"}`);
									// console.log("=====================================\n");
								} catch (error) {
									console.error(`Error processing individual property:`, error.message);
								}
							}

							console.log(`✅ Completed page ${i} for agent ${agent_id}`);

							// Add delay between requests
							await new Promise((resolve) => setTimeout(resolve, 500));
						} catch (error) {
							console.error(`Error fetching page ${i} for agent ${agent_id}:`, error.message);
						}
					}
				};

				// Separate functions for sales and rentals
				const scrapeSalesProperties = async () => {
					await scrapeHawesAndCoProperties(false, 200, 20, "properties-for-sale", "SALE");
					console.log(`✅ Completed sales scraping for agent ${agent_id}`);
				};

				const scrapeRentalProperties = async () => {
					await scrapeHawesAndCoProperties(true, 36, 12, "properties-to-rent", "RENTAL");
					console.log(`✅ Completed rental scraping for agent ${agent_id}`);
				};

				// Call both functions (you can comment out one to run separately)
				// await scrapeSalesProperties();
				await scrapeRentalProperties();

				// await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - Hawes & Co (Sales + Rentals)`);
				agentLog(agent_id, "Completed agent Hawes & Co (Sales + Rentals)");
			}

			if (agent_id == 111) {
				// The Agency UK - Refactored to handle both Sales and Lettings
				const browser = await puppeteer.launch({
					headless: "new",
					args: ["--no-sandbox", "--disable-setuid-sandbox"],
				});

				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link) => {
					try {
						const { data } = await axios.get(link, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});
						const $ = cheerio.load(data);
						const scriptTag = $("#gatsby-script-loader");
						if (scriptTag.length > 0) {
							const scriptContent = scriptTag.html();
							const latMatch =
								scriptContent.match(/"latitude":\s*"([^"]+)"/) ||
								scriptContent.match(/"latitude":\s*([0-9.-]+)/);
							const lngMatch =
								scriptContent.match(/"longitude":\s*"([^"]+)"/) ||
								scriptContent.match(/"longitude":\s*([0-9.-]+)/);
							return {
								latitude: latMatch ? parseFloat(latMatch[1]) : null,
								longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
							};
						}
						// Fallback to HTML comments
						const html = $.html();
						const latMatch = html.match(/<!--property-latitude:"([^"]+)"-->/);
						const lngMatch = html.match(/<!--property-longitude:"([^"]+)"-->/);
						return {
							latitude: latMatch ? parseFloat(latMatch[1]) : null,
							longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
						};
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape properties
				const scrapeAgencyUKProperties = async (department, totalRecords, is_rent) => {
					const recordsPerPage = 12;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const departmentLabel = department === "residential-sales" ? "SALES" : "LETTINGS";

					console.log(
						`🏠 Scraping The Agency UK ${departmentLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Process pages in concurrent batches to speed up scraping
					const batchSize = 5;
					const pageNumbers = Array.from({ length: totalPages }, (_, i) => i + 1);

					for (let batchStart = 0; batchStart < totalPages; batchStart += batchSize) {
						const batch = pageNumbers.slice(batchStart, batchStart + batchSize);
						console.log(`📦 Processing batch of pages: ${batch.join(", ")}`);

						await Promise.all(
							batch.map(async (i) => {
								// Create a new page instance for each concurrent request
								const page = await browser.newPage();

								const listing_url = `https://theagencyuk.com/property-search/?department=${department}&page=${i}&radius=3&per_page=12&address_keyword=&view=list&maximum_price=999999999999&minimum_price=0&new_homes=&include_stc=&hydrated=true`;

								try {
									console.log(
										`📄 Scraping ${departmentLabel.toLowerCase()} page ${i}/${totalPages}`
									);

									await page.goto(listing_url, {
										waitUntil: "networkidle2",
										timeout: 30000,
									});

									await page.waitForSelector(".property", { timeout: 10000 });
									await page.waitForTimeout(3000);

									const properties = await page.evaluate(() => {
										const propertyElements = document.querySelectorAll(".property");
										const results = [];

										propertyElements.forEach((element, index) => {
											try {
												const isShimmer = element.querySelector(".shimmer__effect");
												if (isShimmer) return;

												const linkElement = element.querySelector("a");
												const link = linkElement ? linkElement.href : null;

												const priceSelectors = [".property__price", ".price"];
												let price = null;
												for (const selector of priceSelectors) {
													const priceEl = element.querySelector(selector);
													if (priceEl) {
														const priceText = priceEl.textContent.trim();
														const priceMatch = priceText.match(/£([\d,]+)/);
														if (priceMatch) {
															price = priceMatch[1].replace(/,/g, "");
															break;
														}
													}
												}

												const titleSelectors = ["h1", "h2", "h3", ".property__title"];
												let title = null;
												for (const selector of titleSelectors) {
													const titleEl = element.querySelector(selector);
													if (titleEl && titleEl.textContent.trim()) {
														title = titleEl.textContent.trim();
														break;
													}
												}

												let bedrooms = null;
												const facilityList = element.querySelector(".property__facility");
												if (facilityList) {
													const firstLabel = facilityList.querySelector("li:first-child .label");
													if (firstLabel) {
														bedrooms = firstLabel.textContent.trim();
													}
												}

												if (link && price && title) {
													results.push({ link, price, title, bedrooms });
												}
											} catch (error) {
												console.error(`Error processing property ${index + 1}:`, error.message);
											}
										});

										return results;
									});

									console.log(
										`✅ Found ${
											properties.length
										} ${departmentLabel.toLowerCase()} properties on page ${i}/${totalPages}`
									);

									for (let j = 0; j < properties.length; j++) {
										const property = properties[j];
										try {
											// Get latitude and longitude from detail page
											let latitude = null;
											let longitude = null;
											if (property.link) {
												const coords = await getLatLongFromDetail(property.link);
												latitude = coords.latitude;
												longitude = coords.longitude;
												// Delay to avoid rate limiting
												await new Promise((resolve) => setTimeout(resolve, 1000));
											}

											await updatePriceByPropertyURL(
												property.link,
												property.price,
												property.title,
												property.bedrooms,
												agent_id,
												is_rent,
												latitude,
												longitude
											);

											// console.log(`Latitude: ${latitude}`);
											// console.log(`Longitude: ${longitude}`);
											console.log(`✅ Processed: ${property.title} - £${property.price}`);
										} catch (error) {
											console.error(`❌ Error saving property:`, error.message);
										}
									}
								} catch (error) {
									console.error(
										`Error fetching ${departmentLabel.toLowerCase()} page ${i}:`,
										error.message
									);
								} finally {
									// Close the page instance after use to free up resources
									await page.close();
								}
							})
						);

						// Reduced delay between batches from 2000ms to 1000ms for stability
						if (batchStart + batchSize < totalPages) {
							await new Promise((resolve) => setTimeout(resolve, 1000));
						}
					}
				};

				// Scrape sales properties
				await scrapeAgencyUKProperties("residential-sales", 1217, false);

				// Scrape lettings properties
				await scrapeAgencyUKProperties("residential-lettings", 47, true);

				await browser.close();
				await updateRemoveStatus(agent_id);
				agentLog(agent_id, `✅ Completed agent ${agent_id} - The Agency UK (Sales + Lettings)`);
			}

			if (agent_id == 63) {
				// BHHS London Properties - Refactored to handle both Sales and Lettings

				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link) => {
					try {
						const { data } = await axios.get(link, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});
						const $ = cheerio.load(data);
						const mapView = $("#mapView");
						if (mapView.length > 0) {
							const lat = mapView.attr("data-lat");
							const lon = mapView.attr("data-lon");
							return {
								latitude: lat ? parseFloat(lat) : null,
								longitude: lon ? parseFloat(lon) : null,
							};
						}
						return { latitude: null, longitude: null };
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape BHHS properties
				const scrapeBHHSProperties = async (propertyType, totalRecords, is_rent) => {
					const recordsPerPage = 20;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sale" ? "SALES" : "LETTINGS";
					const urlPath = propertyType === "sale" ? "properties-for-sale" : "properties-for-rent";

					console.log(
						`🏠 Scraping BHHS London ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Process pages in concurrent batches to speed up scraping
					const batchSize = 3;
					const pageNumbers = Array.from({ length: totalPages }, (_, i) => i + 1);

					for (let batchStart = 0; batchStart < totalPages; batchStart += batchSize) {
						const batch = pageNumbers.slice(batchStart, batchStart + batchSize);
						console.log(`📦 Processing batch of pages: ${batch.join(", ")}`);

						await Promise.all(
							batch.map(async (i) => {
								const listing_url = `https://www.bhhslondonproperties.com/${urlPath}?location=&page=${i}`;

								try {
									console.log(`📄 Scraping ${typeLabel.toLowerCase()} page ${i}/${totalPages}`);

									const { data } = await axios.get(listing_url, {
										headers: {
											"User-Agent":
												"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
										},
									});

									const $ = cheerio.load(data);
									const propertyCards = $(".property-card");

									console.log(
										`✅ Found ${
											propertyCards.length
										} ${typeLabel.toLowerCase()} properties on page ${i}/${totalPages}`
									);

									for (let j = 0; j < propertyCards.length; j++) {
										const element = propertyCards.eq(j);

										try {
											const link = element.find("a").attr("href");
											const title = element.find("h3.md-heading").text().trim();

											const bedroomsText = element
												.find("p.text-sm.text-white")
												.first()
												.text()
												.trim();
											const bedroomsMatch = bedroomsText.match(/(\d+)\s*Bedrooms/);
											const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

											let price = null;
											const priceEl = element.find(".price");
											if (priceEl.length > 0) {
												const priceText = priceEl.text().trim();
												const priceMatch = priceText.match(/£([\d,]+)/);
												price = priceMatch ? priceMatch[1].replace(/,/g, "") : null;
											} else {
												const priceText = element.find("p.md-heading").last().text().trim();
												if (priceText.includes("POA")) {
													price = "POA";
												}
											}

											if (link && title && price) {
												// Get latitude and longitude from detail page
												let latitude = null;
												let longitude = null;
												if (link) {
													const coords = await getLatLongFromDetail(link);
													latitude = coords.latitude;
													longitude = coords.longitude;
													// Delay to avoid rate limiting
													await new Promise((resolve) => setTimeout(resolve, 1000));
												}

												await updatePriceByPropertyURL(
													link,
													price,
													title,
													bedrooms,
													agent_id,
													is_rent,
													latitude,
													longitude
												);
												console.log(
													`✅ Processed: ${title} - £${price} - ${latitude},${longitude}`
												);
											}
										} catch (error) {
											console.error(`❌ Error processing property ${j + 1}:`, error.message);
										}
									}
								} catch (error) {
									console.error(
										`Error fetching ${typeLabel.toLowerCase()} page ${i}:`,
										error.message
									);
								}
							})
						);

						// Reduced delay between batches from 1000ms to 500ms
						if (batchStart + batchSize < totalPages) {
							await new Promise((resolve) => setTimeout(resolve, 500));
						}
					}
				};

				// Separate functions for sales and lettings
				const scrapeSalesProperties = async () => {
					await scrapeBHHSProperties("sale", 116, false);
					console.log(`✅ Completed sales scraping for agent ${agent_id}`);
				};

				const scrapeLettingsProperties = async () => {
					await scrapeBHHSProperties("rent", 74, true);
					console.log(`✅ Completed lettings scraping for agent ${agent_id}`);
				};

				// Execute scraping
				// await scrapeSalesProperties();
				await scrapeLettingsProperties();

				await updateRemoveStatus(agent_id);
				agentLog(
					agent_id,
					`✅ Completed agent ${agent_id} - BHHS London Properties (Sales + Lettings)`
				);
			}

			if (agent_id == 103) {
				// Alan de Maid - Uses Puppeteer (site requires JS rendering)

				// Create a single browser instance to reuse
				const browser = await puppeteer.launch({
					headless: "new",
					args: ["--no-sandbox", "--disable-setuid-sandbox"],
				});

				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link, page) => {
					try {
						await page.goto(link, { waitUntil: "networkidle2", timeout: 30000 });
						const content = await page.content();
						const $ = cheerio.load(content);
						const scriptTags = $("script");
						for (let i = 0; i < scriptTags.length; i++) {
							const scriptContent = $(scriptTags[i]).html();
							if (scriptContent && scriptContent.includes("propertyObject")) {
								const latMatch = scriptContent.match(/ga4_property_latitude:\s*([0-9.-]+)/);
								const lngMatch = scriptContent.match(/ga4_property_longitude:\s*([0-9.-]+)/);
								return {
									latitude: latMatch ? parseFloat(latMatch[1]) : null,
									longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
								};
							}
						}
						return { latitude: null, longitude: null };
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape Alan de Maid properties
				const scrapeAlanDeMaidProperties = async (propertyType, totalRecords, is_rent) => {
					const recordsPerPage = 10;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sales" ? "SALES" : "LETTINGS";
					const urlPath =
						propertyType === "sales"
							? "properties/sales/status-available/most-recent-first"
							: "properties/lettings/status-available/most-recent-first";

					console.log(
						`🏠 Scraping Alan de Maid ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Process pages sequentially to avoid issues
					for (let i = 1; i <= totalPages; i++) {
						const listing_url =
							propertyType === "sales"
								? `https://www.alandemaid.co.uk/${urlPath}/page-${i}#/`
								: `https://www.alandemaid.co.uk/${urlPath}#/`;

						const page = await browser.newPage();
						await page.setUserAgent(
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
						);

						try {
							console.log(`📄 Scraping ${typeLabel.toLowerCase()} page ${i}/${totalPages}`);

							await page.goto(listing_url, { waitUntil: "networkidle2", timeout: 60000 });
							await page.waitForTimeout(3000);

							const content = await page.content();
							const $ = cheerio.load(content);
							const propertyCards = $(".card");

							console.log(
								`✅ Found ${
									propertyCards.length
								} ${typeLabel.toLowerCase()} properties on page ${i}/${totalPages}`
							);

							// Collect all properties from this page first
							const properties = [];
							propertyCards.each((_index, element) => {
								try {
									const $element = $(element);
									const link = $element.find("a").attr("href")
										? "https://www.alandemaid.co.uk" + $element.find("a").attr("href")
										: null;
									const title = $element.find(".card__text-content").text().trim();
									const bedroomsText = $element
										.find(".card-content__spec-list-number")
										.first()
										.text()
										.trim();
									const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
									const priceText = $element.find(".card__heading").text().trim();
									const price = priceText.match(/£([\d,]+)/)?.[1] || null;

									if (link && title && price) {
										properties.push({ link, title, bedrooms, price });
									}
								} catch (err) {
									console.error("Error processing card:", err.message);
								}
							});

							await page.close();

							// Process each property with coordinates
							for (const property of properties) {
								const detailPage = await browser.newPage();
								await detailPage.setUserAgent(
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
								);

								try {
									const coords = await getLatLongFromDetail(property.link, detailPage);
									await updatePriceByPropertyURL(
										property.link,
										property.price,
										property.title,
										property.bedrooms,
										agent_id,
										is_rent,
										coords.latitude,
										coords.longitude
									);
									console.log(
										`✅ Processed: ${property.title} - £${property.price} - ${coords.latitude},${coords.longitude}`
									);
								} catch (error) {
									console.error(`❌ Error processing property:`, error.message);
								} finally {
									await detailPage.close();
								}

								// Small delay between properties
								await new Promise((resolve) => setTimeout(resolve, 500));
							}
						} catch (error) {
							console.error(`Error fetching ${typeLabel.toLowerCase()} page ${i}:`, error.message);
							await page.close();
						}

						// For lettings, only scrape first page
						if (propertyType === "lettings") {
							break;
						}

						// Delay between pages
						await new Promise((resolve) => setTimeout(resolve, 500));
					}
				};

				// Scrape sales properties (376 total, 38 pages)
				await scrapeAlanDeMaidProperties("sales", 376, false);

				// Scrape lettings properties (12 total, 2 pages)
				await scrapeAlanDeMaidProperties("lettings", 12, true);

				await browser.close();
				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - Alan de Maid (Sales + Lettings)`);
			}

			if (agent_id == 13) {
				// Bairstow Eves - Uses Puppeteer with Stealth (site requires JS rendering + Cloudflare bypass)

				// Create a single browser instance with enhanced Cloudflare bypass settings
				const browser = await puppeteer.launch({
					headless: "new",
					args: [
						"--no-sandbox",
						"--disable-setuid-sandbox",
						"--disable-blink-features=AutomationControlled",
						"--disable-dev-shm-usage",
						"--disable-accelerated-2d-canvas",
						"--no-first-run",
						"--no-zygote",
						"--disable-gpu",
						"--window-size=1920,1080",
						"--disable-web-security",
						"--disable-features=IsolateOrigins,site-per-process",
					],
				});

				// Enhanced page setup to bypass Cloudflare
				const setupPage = async (page) => {
					await page.setViewport({ width: 1920, height: 1080 });

					// Override navigator properties
					await page.evaluateOnNewDocument(() => {
						Object.defineProperty(navigator, "webdriver", { get: () => false });
						Object.defineProperty(navigator, "plugins", { get: () => [1, 2, 3, 4, 5] });
						Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
						window.chrome = { runtime: {} };
					});

					// Set realistic headers
					await page.setExtraHTTPHeaders({
						"Accept-Language": "en-US,en;q=0.9",
						Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
						"Accept-Encoding": "gzip, deflate, br",
						Connection: "keep-alive",
						"Upgrade-Insecure-Requests": "1",
						"Sec-Fetch-Dest": "document",
						"Sec-Fetch-Mode": "navigate",
						"Sec-Fetch-Site": "none",
						"Cache-Control": "max-age=0",
					});
				};

				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link, page) => {
					try {
						await page.goto(link, {
							waitUntil: "domcontentloaded",
							timeout: 20000,
						});

						const coords = await page.evaluate(() => {
							const html = document.documentElement.innerHTML;
							const latMatch = html.match(/<!--property-latitude:"([0-9.-]+)"-->/);
							const lngMatch = html.match(/<!--property-longitude:"([0-9.-]+)"-->/);

							return {
								latitude: latMatch ? parseFloat(latMatch[1]) : null,
								longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
							};
						});

						return coords;
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape Bairstow Eves properties
				const scrapeBairstowEvesProperties = async (propertyType, totalRecords, is_rent) => {
					const recordsPerPage = 50;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sales" ? "SALES" : "LETTINGS";
					const urlPath =
						propertyType === "sales"
							? "properties/sales/status-available/most-recent-first"
							: "properties/lettings/status-available/most-recent-first";

					console.log(
						`🏠 Scraping Bairstow Eves ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Process pages sequentially to avoid rate limiting
					for (let i = 1; i <= totalPages; i++) {
						const listing_url = `https://www.bairstoweves.co.uk/${urlPath}/page-${i}#/`;
						const page = await browser.newPage();

						try {
							console.log(`📄 Scraping ${typeLabel.toLowerCase()} page ${i}/${totalPages}`);

							// Set viewport for consistency
							await page.setViewport({ width: 1920, height: 1080 });

							await page.goto(listing_url, {
								waitUntil: "domcontentloaded",
								timeout: 30000,
							});

							// Wait for property cards to load
							await page.waitForSelector(".hf-property-results .card", { timeout: 10000 });
							await page.waitForTimeout(2000);

							// Extract properties directly in the browser context
							const properties = await page.evaluate(() => {
								const cards = document.querySelectorAll(".hf-property-results .card");
								const results = [];

								cards.forEach((card) => {
									try {
										const linkEl = card.querySelector("a");
										const link = linkEl?.href || null;
										const title =
											card.querySelector(".card__text-content")?.textContent.trim() || null;
										const bedroomsText =
											card.querySelector(".card-content__spec-list-number")?.textContent.trim() ||
											"";
										const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
										const priceText =
											card.querySelector(".card__heading")?.textContent.trim() || "";
										const price = priceText.match(/£([\d,]+)/)?.[1] || null;

										if (link && title && price) {
											results.push({ link, title, bedrooms, price });
										}
									} catch (err) {
										console.error("Error processing card:", err.message);
									}
								});

								return results;
							});

							console.log(
								`✅ Found ${
									properties.length
								} ${typeLabel.toLowerCase()} properties on page ${i}/${totalPages}`
							);

							await page.close();

							// Process properties in batches of 5 for optimal speed
							const batchSize = 5;
							for (let j = 0; j < properties.length; j += batchSize) {
								const batch = properties.slice(j, j + batchSize);

								await Promise.all(
									batch.map(async (property) => {
										const detailPage = await browser.newPage();

										try {
											await detailPage.setViewport({ width: 1920, height: 1080 });
											const coords = await getLatLongFromDetail(property.link, detailPage);

											await updatePriceByPropertyURL(
												property.link,
												property.price,
												property.title,
												property.bedrooms,
												agent_id,
												is_rent,
												coords.latitude,
												coords.longitude
											);

											console.log(
												`✅ Processed: ${property.title} - £${property.price} - ${coords.latitude},${coords.longitude}`
											);
										} catch (error) {
											console.error(`❌ Error processing property:`, error.message);
										} finally {
											await detailPage.close();
										}
									})
								);

								// Small delay between batches to avoid rate limiting
								if (j + batchSize < properties.length) {
									await new Promise((resolve) => setTimeout(resolve, 300));
								}
							}
						} catch (error) {
							console.error(`Error fetching ${typeLabel.toLowerCase()} page ${i}:`, error.message);
							if (page && !page.isClosed()) await page.close();
						}

						// Delay between pages
						await new Promise((resolve) => setTimeout(resolve, 400));
					}
				};

				// Scrape sales properties (2825 total, 57 pages)
				await scrapeBairstowEvesProperties("sales", 2825, false);

				// Scrape lettings properties (634 total, 13 pages)
				await scrapeBairstowEvesProperties("lettings", 634, true);

				await browser.close();
				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - Bairstow Eves (Sales + Lettings)`);
			}

			if (agent_id == 127) {
				// BridgFords - Refactored to handle both Sales and Lettings (with stealth)

				// Create a single browser instance to reuse with stealth plugin
				const browser = await puppeteer.launch({
					headless: "new",
					args: [
						"--no-sandbox",
						"--disable-setuid-sandbox",
						"--disable-blink-features=AutomationControlled",
					],
				});

				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link, page) => {
					try {
						await page.goto(link, { waitUntil: "networkidle2", timeout: 30000 });
						const content = await page.content();

						// Extract coordinates from HTML comments
						const latMatch = content.match(/<!--property-latitude:"([0-9.-]+)"-->/);
						const lngMatch = content.match(/<!--property-longitude:"([0-9.-]+)"-->/);

						return {
							latitude: latMatch ? parseFloat(latMatch[1]) : null,
							longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
						};
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape BridgFords properties
				const scrapeBridgeFordsProperties = async (propertyType, totalRecords, is_rent) => {
					const recordsPerPage = 10;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sales" ? "SALES" : "LETTINGS";
					const urlPath =
						propertyType === "sales"
							? "properties/sales/status-available/most-recent-first"
							: "properties/lettings/status-available/most-recent-first";

					console.log(
						`🏠 Scraping BridgeFords ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Process pages sequentially to avoid issues
					for (let i = 1; i <= totalPages; i++) {
						const listing_url = `https://www.bridgfords.co.uk/${urlPath}/page-${i}#/`;

						const page = await browser.newPage();
						await page.setUserAgent(
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
						);

						try {
							console.log(`📄 Scraping ${typeLabel.toLowerCase()} page ${i}/${totalPages}`);

							await page.goto(listing_url, { waitUntil: "networkidle2", timeout: 60000 });
							await page.waitForTimeout(5000); // Increased wait time

							const content = await page.content();

							// Check for blocking/error messages
							if (
								content.includes("Access Denied") ||
								content.includes("blocked") ||
								content.includes("captcha")
							) {
								console.error(`⚠️ Page ${i} appears to be blocked or requires captcha`);
								await page.close();
								break;
							}

							const $ = cheerio.load(content);
							const propertyCards = $(".hf-property-results .card");

							console.log(
								`✅ Found ${
									propertyCards.length
								} ${typeLabel.toLowerCase()} properties on page ${i}/${totalPages}`
							);

							// If no properties found, stop pagination
							if (propertyCards.length === 0) {
								console.log(`⚠️ No properties found on page ${i}, stopping pagination`);
								await page.close();
								break;
							}

							// Collect all properties from this page first
							const properties = [];
							propertyCards.each((index, element) => {
								try {
									const $element = $(element);
									const link = $element.find("a").attr("href")
										? "https://www.bridgfords.co.uk" + $element.find("a").attr("href")
										: null;
									const title = $element.find(".card__text-content").text().trim();
									const bedroomsText = $element
										.find(".card-content__spec-list-number")
										.first()
										.text()
										.trim();
									const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
									const priceText = $element.find(".card__heading").text().trim();
									const price = priceText.match(/£([\d,]+)/)?.[1] || null;

									if (link && title && price) {
										properties.push({ link, title, bedrooms, price });
									}
								} catch (err) {
									console.error("Error processing card:", err.message);
								}
							});

							await page.close();

							// Process properties in batches of 3 for better speed
							const batchSize = 3;
							for (let j = 0; j < properties.length; j += batchSize) {
								const batch = properties.slice(j, j + batchSize);

								await Promise.all(
									batch.map(async (property) => {
										const detailPage = await browser.newPage();
										await detailPage.setUserAgent(
											"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
										);

										try {
											const coords = await getLatLongFromDetail(property.link, detailPage);
											await updatePriceByPropertyURL(
												property.link,
												property.price,
												property.title,
												property.bedrooms,
												agent_id,
												is_rent,
												coords.latitude,
												coords.longitude
											);
											console.log(
												`✅ Processed: ${property.title} - £${property.price} - ${coords.latitude},${coords.longitude}`
											);
										} catch (error) {
											console.error(`❌ Error processing property:`, error.message);
										} finally {
											await detailPage.close();
										}
									})
								);

								// Small delay between batches
								if (j + batchSize < properties.length) {
									await new Promise((resolve) => setTimeout(resolve, 500));
								}
							}
						} catch (error) {
							console.error(`Error fetching ${typeLabel.toLowerCase()} page ${i}:`, error.message);
							await page.close();
						}

						// Delay between pages
						await new Promise((resolve) => setTimeout(resolve, 500));
					}
				};

				// await scrapeBridgeFordsProperties("sales", 2145, false);

				await scrapeBridgeFordsProperties("lettings", 409, true);

				await browser.close();
				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - BridgFords (Sales + Lettings)`);
			}

			if (agent_id == 116) {
				// Gascoigne Pees - Uses Puppeteer (site requires JS rendering)

				// Create a single browser instance to reuse
				const browser = await puppeteer.launch({
					headless: "new",
					args: ["--no-sandbox", "--disable-setuid-sandbox"],
				});

				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link, page) => {
					try {
						await page.goto(link, { waitUntil: "networkidle2", timeout: 30000 });
						const content = await page.content();

						// Extract coordinates from HTML comments
						const latMatch = content.match(/<!--property-latitude:"([0-9.-]+)"-->/);
						const lngMatch = content.match(/<!--property-longitude:"([0-9.-]+)"-->/);

						return {
							latitude: latMatch ? parseFloat(latMatch[1]) : null,
							longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
						};
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape Gascoigne Pees properties
				const scrapeGasocignePeesProperties = async (propertyType, totalRecords, is_rent) => {
					const recordsPerPage = 10;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sales" ? "SALES" : "LETTINGS";
					const urlPath =
						propertyType === "sales"
							? "properties/sales/status-available/most-recent-first"
							: "properties/lettings/status-available/most-recent-first";

					console.log(
						`🏠 Scraping Gascoigne Pees ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Process pages sequentially to avoid issues
					for (let i = 1; i <= totalPages; i++) {
						const listing_url = `https://www.gpees.co.uk/${urlPath}/page-${i}#/`;

						const page = await browser.newPage();
						await page.setUserAgent(
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
						);

						try {
							console.log(`📄 Scraping ${typeLabel.toLowerCase()} page ${i}/${totalPages}`);

							await page.goto(listing_url, { waitUntil: "networkidle2", timeout: 60000 });
							await page.waitForTimeout(3000);

							const content = await page.content();
							const $ = cheerio.load(content);
							const propertyCards = $(".hf-property-results .card");

							console.log(
								`✅ Found ${
									propertyCards.length
								} ${typeLabel.toLowerCase()} properties on page ${i}/${totalPages}`
							);

							// Collect all properties from this page first
							const properties = [];
							propertyCards.each((_index, element) => {
								try {
									const $element = $(element);
									const link = $element.find("a").attr("href")
										? "https://www.gpees.co.uk" + $element.find("a").attr("href")
										: null;
									const title = $element.find(".card__text-content").text().trim();
									const bedroomsText = $element
										.find(".card-content__spec-list-number")
										.first()
										.text()
										.trim();
									const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
									const priceText = $element.find(".card__heading").text().trim();
									const price = priceText.match(/£([\d,]+)/)?.[1] || null;

									if (link && title && price) {
										properties.push({ link, title, bedrooms, price });
									}
								} catch (err) {
									console.error("Error processing card:", err.message);
								}
							});

							// Process all properties from this page
							for (let j = 0; j < properties.length; j++) {
								const property = properties[j];
								try {
									// Get latitude and longitude from detail page
									const { latitude, longitude } = await getLatLongFromDetail(property.link, page);

									await updatePriceByPropertyURL(
										property.link,
										property.price,
										property.title,
										property.bedrooms,
										agent_id,
										is_rent,
										latitude,
										longitude
									);
								} catch (error) {
									console.error(`❌ Error saving property:`, error.message);
								}
							}

							await page.close();

							// Delay between requests to avoid rate limiting
							if (i < totalPages) {
								const delay = 3000 + Math.random() * 2000;
								console.log(`⏱️ Waiting ${Math.round(delay / 1000)}s before next page...`);
								await new Promise((resolve) => setTimeout(resolve, delay));
							}
						} catch (error) {
							await page.close();
							console.error(`Error fetching ${typeLabel.toLowerCase()} page ${i}:`, error.message);
						}
					}
				};

				// Scrape sales properties (512 total, 52 pages)
				await scrapeGasocignePeesProperties("sales", 512, false);

				// Scrape lettings properties (70 total, 7 pages)
				await scrapeGasocignePeesProperties("lettings", 70, true);

				await browser.close();
				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - Gascoigne Pees (Sales + Lettings)`);
			}

			if (agent_id == 118) {
				// Countrywide - With stealth mode and sequential processing to avoid rate limiting

				const puppeteerExtra = require("puppeteer-extra");
				const StealthPlugin = require("puppeteer-extra-plugin-stealth");
				puppeteerExtra.use(StealthPlugin());

				const scrapeCountrywideProperties = async (
					propertyType,
					totalRecords,
					is_rent,
					browser
				) => {
					const recordsPerPage = 10;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sales" ? "SALES" : "LETTINGS";
					const urlPath =
						propertyType === "sales"
							? "properties/sales/status-available/most-recent-first"
							: "properties/lettings/status-available/most-recent-first";

					console.log(
						`🏠 Scraping Countrywide ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Helper function to extract coordinates from detail page
					const extractCoordinates = async (propertyUrl) => {
						const page = await browser.newPage();
						try {
							await page.goto(propertyUrl, {
								waitUntil: "domcontentloaded",
								timeout: 60000,
							});
							await page.waitForTimeout(2000 + Math.random() * 2000); // Random delay 2-4s

							const content = await page.content();
							const cheerio = require("cheerio");
							const $ = cheerio.load(content);

							let latitude = null;
							let longitude = null;

							// Extract coordinates from HTML comments
							const htmlComments = content.match(/<!--[\s\S]*?-->/g) || [];
							for (const comment of htmlComments) {
								const latMatch = comment.match(/latitude[:\s=]+([+-]?\d+\.\d+)/i);
								const lngMatch = comment.match(/longitude[:\s=]+([+-]?\d+\.\d+)/i);

								if (latMatch) latitude = latMatch[1];
								if (lngMatch) longitude = lngMatch[1];

								if (latitude && longitude) break;
							}

							// Also check for coordinates in script tags or data attributes
							if (!latitude || !longitude) {
								$("script").each((i, elem) => {
									const scriptContent = $(elem).html();
									if (scriptContent) {
										const latMatch = scriptContent.match(/latitude[:\s=]+([+-]?\d+\.\d+)/i);
										const lngMatch = scriptContent.match(/longitude[:\s=]+([+-]?\d+\.\d+)/i);

										if (latMatch) latitude = latMatch[1];
										if (lngMatch) longitude = lngMatch[1];

										if (latitude && longitude) return false; // break
									}
								});
							}

							await page.close();
							return { latitude, longitude };
						} catch (error) {
							await page.close();
							console.error(`Error extracting coordinates from ${propertyUrl}:`, error.message);
							return { latitude: null, longitude: null };
						}
					};

					// Helper function to scrape a single page
					const scrapePage = async (pageNum) => {
						const listing_url = `https://www.countrywidescotland.co.uk/${urlPath}/page-${pageNum}#/`;
						const page = await browser.newPage();

						try {
							await page.goto(listing_url, {
								waitUntil: "domcontentloaded",
								timeout: 60000,
							});
							await page.waitForTimeout(3000 + Math.random() * 2000); // Random delay 3-5s

							const content = await page.content();
							const cheerio = require("cheerio");
							const $ = cheerio.load(content);
							const properties = [];

							$(".hf-property-results .card").each((index, element) => {
								try {
									const $element = $(element);
									const link = $element.find("a").attr("href")
										? "https://www.countrywidescotland.co.uk" + $element.find("a").attr("href")
										: null;
									const title = $element.find(".card__text-content").text().trim();
									const bedroomsText = $element
										.find(".card-content__spec-list-number")
										.first()
										.text()
										.trim();
									const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
									const priceText = $element.find(".card__heading").text().trim();
									const price = priceText.match(/£([\d,]+)/)?.[1] || null;

									if (link && title && price) {
										properties.push({ link, title, bedrooms, price });
									}
								} catch (err) {
									console.error("Error processing card:", err.message);
								}
							});

							await page.close();

							console.log(
								`📄 Page ${pageNum}/${totalPages}: Found ${
									properties.length
								} ${typeLabel.toLowerCase()} properties`
							);

							return properties;
						} catch (error) {
							await page.close();
							console.error(
								`Error fetching ${typeLabel.toLowerCase()} page ${pageNum}:`,
								error.message
							);
							return [];
						}
					};

					// Process pages ONE BY ONE to avoid rate limiting
					for (let pageNum = 1; pageNum <= totalPages; pageNum++) {
						const properties = await scrapePage(pageNum);

						// Process each property ONE BY ONE
						for (const property of properties) {
							try {
								// Extract coordinates from detail page
								const { latitude, longitude } = await extractCoordinates(property.link);

								// Update property with coordinates
								await updatePriceByPropertyURL(
									property.link,
									property.price,
									property.title,
									property.bedrooms,
									agent_id,
									is_rent,
									latitude,
									longitude
								);

								if (latitude && longitude) {
									console.log(`📍 ${property.title}: ${latitude}, ${longitude}`);
								} else {
									console.log(`⚠️  ${property.title}: No coordinates found`);
								}

								// Delay between properties to avoid rate limiting
								await new Promise((resolve) => setTimeout(resolve, 3000 + Math.random() * 2000)); // 3-5s delay
							} catch (error) {
								console.error(`❌ Error saving property:`, error.message);
							}
						}

						// Delay between pages
						if (pageNum < totalPages) {
							console.log(`⏳ Waiting before next page...`);
							await new Promise((resolve) => setTimeout(resolve, 5000)); // 5s delay between pages
						}
					}
				};

				// Create browser with stealth mode
				const browser = await puppeteerExtra.launch({
					headless: "new",
					args: [
						"--no-sandbox",
						"--disable-setuid-sandbox",
						"--disable-blink-features=AutomationControlled",
					],
				});

				try {
					// Scrape sales first, then lettings (sequential to avoid rate limiting)
					await scrapeCountrywideProperties("sales", 157, false, browser);
					console.log(`⏳ Waiting before lettings...`);
					await new Promise((resolve) => setTimeout(resolve, 10000)); // 10s delay between types
					await scrapeCountrywideProperties("lettings", 25, true, browser);
				} finally {
					await browser.close();
				}

				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - Countrywide (Sales + Lettings)`);
			}

			if (agent_id == 134) {
				// Stratton Creber - Uses Puppeteer with stealth plugin

				const puppeteerExtra = require("puppeteer-extra");
				const StealthPlugin = require("puppeteer-extra-plugin-stealth");
				puppeteerExtra.use(StealthPlugin());

				// Create a single browser instance to reuse
				const browser = await puppeteerExtra.launch({
					headless: "new",
					args: ["--no-sandbox", "--disable-setuid-sandbox"],
				});

				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link, page) => {
					try {
						await page.goto(link, { waitUntil: "networkidle2", timeout: 30000 });
						const content = await page.content();
						const $ = cheerio.load(content);
						const scriptTags = $("script");
						for (let i = 0; i < scriptTags.length; i++) {
							const scriptContent = $(scriptTags[i]).html();
							if (scriptContent && scriptContent.includes("propertyObject")) {
								const latMatch = scriptContent.match(/ga4_property_latitude:\s*([0-9.-]+)/);
								const lngMatch = scriptContent.match(/ga4_property_longitude:\s*([0-9.-]+)/);
								return {
									latitude: latMatch ? parseFloat(latMatch[1]) : null,
									longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
								};
							}
						}
						return { latitude: null, longitude: null };
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape Stratton Creber properties
				const scrapeStrattonCreberProperties = async (propertyType, totalRecords, is_rent) => {
					const recordsPerPage = 10;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sales" ? "SALES" : "LETTINGS";
					const urlPath =
						propertyType === "sales"
							? "properties/sales/status-available/most-recent-first"
							: "properties/lettings/status-available/most-recent-first";

					console.log(
						`🏠 Scraping Stratton Creber ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Process pages sequentially to avoid issues
					for (let i = 1; i <= totalPages; i++) {
						const listing_url = `https://www.strattoncreber.co.uk/${urlPath}/page-${i}#/`;

						const page = await browser.newPage();
						await page.setUserAgent(
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
						);

						try {
							console.log(`📄 Scraping ${typeLabel.toLowerCase()} page ${i}/${totalPages}`);

							await page.goto(listing_url, { waitUntil: "networkidle2", timeout: 60000 });
							await page.waitForTimeout(3000);

							const content = await page.content();
							const $ = cheerio.load(content);
							const propertyCards = $(".hf-property-results .card");

							console.log(
								`✅ Found ${
									propertyCards.length
								} ${typeLabel.toLowerCase()} properties on page ${i}/${totalPages}`
							);

							// Collect all properties from this page first
							const properties = [];
							propertyCards.each((_index, element) => {
								try {
									const $element = $(element);
									const link = $element.find("a").attr("href")
										? "https://www.strattoncreber.co.uk" + $element.find("a").attr("href")
										: null;
									const title = $element.find(".card__text-content").text().trim();
									const bedroomsText = $element
										.find(".card-content__spec-list-number")
										.first()
										.text()
										.trim();
									const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
									const priceText = $element.find(".card__heading").text().trim();
									const price = priceText.match(/£([\d,]+)/)?.[1] || null;

									if (link && title && price) {
										properties.push({ link, title, bedrooms, price });
									}
								} catch (err) {
									console.error("Error processing card:", err.message);
								}
							});

							await page.close();

							// Process each property with coordinates
							for (const property of properties) {
								const detailPage = await browser.newPage();
								await detailPage.setUserAgent(
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
								);

								try {
									const coords = await getLatLongFromDetail(property.link, detailPage);
									await updatePriceByPropertyURL(
										property.link,
										property.price,
										property.title,
										property.bedrooms,
										agent_id,
										is_rent,
										coords.latitude,
										coords.longitude
									);
									console.log(
										`✅ Processed: ${property.title} - £${property.price} - ${coords.latitude},${coords.longitude}`
									);
								} catch (error) {
									console.error(`❌ Error processing property:`, error.message);
								} finally {
									await detailPage.close();
								}

								// Small delay between properties
								await new Promise((resolve) => setTimeout(resolve, 500));
							}
						} catch (error) {
							console.error(`Error fetching ${typeLabel.toLowerCase()} page ${i}:`, error.message);
							await page.close();
						}

						// Add delay between pages
						if (i < totalPages) {
							const delay = 2000 + Math.random() * 1000;
							console.log(`⏱️ Waiting ${Math.round(delay / 1000)}s before next page...`);
							await new Promise((resolve) => setTimeout(resolve, delay));
						}
					}
				};

				// Scrape sales first, then lettings
				await scrapeStrattonCreberProperties("sales", 157, false);
				await scrapeStrattonCreberProperties("lettings", 25, true);

				await browser.close();
				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - Stratton Creber (Sales + Lettings)`);
			}

			if (agent_id == 135) {
				// Taylors - Uses Puppeteer with stealth plugin

				const puppeteerExtra = require("puppeteer-extra");
				const StealthPlugin = require("puppeteer-extra-plugin-stealth");
				puppeteerExtra.use(StealthPlugin());

				// Create a single browser instance to reuse
				const browser = await puppeteerExtra.launch({
					headless: "new",
					args: ["--no-sandbox", "--disable-setuid-sandbox"],
				});

				// Helper function to get latitude and longitude from property detail page
				const getLatLongFromDetail = async (link, page) => {
					try {
						await page.goto(link, { waitUntil: "networkidle2", timeout: 30000 });
						const content = await page.content();
						const $ = cheerio.load(content);
						const scriptTags = $("script");
						for (let i = 0; i < scriptTags.length; i++) {
							const scriptContent = $(scriptTags[i]).html();
							if (scriptContent && scriptContent.includes("propertyObject")) {
								const latMatch = scriptContent.match(/ga4_property_latitude:\s*([0-9.-]+)/);
								const lngMatch = scriptContent.match(/ga4_property_longitude:\s*([0-9.-]+)/);
								return {
									latitude: latMatch ? parseFloat(latMatch[1]) : null,
									longitude: lngMatch ? parseFloat(lngMatch[1]) : null,
								};
							}
						}
						return { latitude: null, longitude: null };
					} catch (error) {
						console.error(`Error fetching detail page ${link}:`, error.message);
						return { latitude: null, longitude: null };
					}
				};

				// Helper function to scrape Taylors properties
				const scrapeTaylorsProperties = async (propertyType, totalRecords, is_rent) => {
					const recordsPerPage = 10;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sales" ? "SALES" : "LETTINGS";
					const urlPath =
						propertyType === "sales"
							? "properties/sales/status-available/most-recent-first"
							: "properties/lettings/status-available/most-recent-first";
					const batchSize = 2; // Process 2 pages at a time

					console.log(
						`🏠 Scraping Taylors ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Process pages in batches of 2
					for (let i = 1; i <= totalPages; i += batchSize) {
						const batch = [];
						for (let j = 0; j < batchSize && i + j <= totalPages; j++) {
							batch.push(i + j);
						}

						// Scrape pages in batch
						for (const pageNum of batch) {
							const listing_url = `https://www.taylorsestateagents.co.uk/${urlPath}/page-${pageNum}#/`;

							const page = await browser.newPage();
							await page.setUserAgent(
								"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
							);

							try {
								console.log(`📄 Scraping ${typeLabel.toLowerCase()} page ${pageNum}/${totalPages}`);

								await page.goto(listing_url, { waitUntil: "networkidle2", timeout: 60000 });
								await page.waitForTimeout(3000);

								const content = await page.content();
								const $ = cheerio.load(content);
								const propertyCards = $(".hf-property-results .card");

								console.log(
									`✅ Found ${
										propertyCards.length
									} ${typeLabel.toLowerCase()} properties on page ${pageNum}/${totalPages}`
								);

								// Collect all properties from this page first
								const properties = [];
								propertyCards.each((_index, element) => {
									try {
										const $element = $(element);
										const link = $element.find("a").attr("href")
											? "https://www.taylorsestateagents.co.uk" + $element.find("a").attr("href")
											: null;
										const title = $element.find(".card__text-content").text().trim();
										const bedroomsText = $element
											.find(".card-content__spec-list-number")
											.first()
											.text()
											.trim();
										const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
										const priceText = $element.find(".card__heading").text().trim();
										const price = priceText.match(/£([\d,]+)/)?.[1] || null;

										if (link && title && price) {
											properties.push({ link, title, bedrooms, price });
										}
									} catch (err) {
										console.error("Error processing card:", err.message);
									}
								});

								await page.close();

								// Process each property with coordinates
								for (const property of properties) {
									const detailPage = await browser.newPage();
									await detailPage.setUserAgent(
										"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
									);

									try {
										const coords = await getLatLongFromDetail(property.link, detailPage);
										await updatePriceByPropertyURL(
											property.link,
											property.price,
											property.title,
											property.bedrooms,
											agent_id,
											is_rent,
											coords.latitude,
											coords.longitude
										);
										console.log(
											`✅ Processed: ${property.title} - £${property.price} - ${coords.latitude},${coords.longitude}`
										);
									} catch (error) {
										console.error(`❌ Error processing property:`, error.message);
									} finally {
										await detailPage.close();
									}

									// Small delay between properties
									await new Promise((resolve) => setTimeout(resolve, 500));
								}
							} catch (error) {
								console.error(
									`Error fetching ${typeLabel.toLowerCase()} page ${pageNum}:`,
									error.message
								);
								await page.close();
							}
						}

						// Add delay between batches
						if (i + batchSize <= totalPages) {
							const delay = 2000 + Math.random() * 1000;
							console.log(`⏱️ Waiting ${Math.round(delay / 1000)}s before next batch...`);
							await new Promise((resolve) => setTimeout(resolve, delay));
						}
					}
				};

				// Scrape sales first, then lettings
				await scrapeTaylorsProperties("sales", 1280, false);
				await scrapeTaylorsProperties("lettings", 224, true);

				await browser.close();
				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - Taylors (Sales + Lettings)`);
			}

			if (agent_id == 107) {
				// BELVOIR - Sales and Rentals scraper with Puppeteer
				const scrapeBelvoirProperties = async (propertyType, totalRecords) => {
					const recordsPerPage = 11;
					const totalPages = Math.ceil(totalRecords / recordsPerPage);
					const typeLabel = propertyType === "sale" ? "SALES" : "RENTALS";
					const urlPath = propertyType === "sale" ? "for-sale" : "for-rent";
					const is_rent = propertyType === "rent";

					console.log(
						`🏠 Scraping BELVOIR ${typeLabel} properties (${totalRecords} total, ${totalPages} pages)`
					);

					// Launch browser once for this property type
					const browser = await puppeteer.launch({
						headless: "new",
						args: [
							"--no-sandbox",
							"--disable-setuid-sandbox",
							"--disable-dev-shm-usage",
							"--disable-accelerated-2d-canvas",
							"--disable-gpu",
						],
					});

					try {
						// Process pages in batches of 2
						const concurrencyLimit = 2;
						for (let batchStart = 1; batchStart <= totalPages; batchStart += concurrencyLimit) {
							const batchEnd = Math.min(batchStart + concurrencyLimit - 1, totalPages);
							const pagePromises = [];

							for (let pageNum = batchStart; pageNum <= batchEnd; pageNum++) {
								pagePromises.push(
									(async (currentPage, currentUrlPath, currentTypeLabel, currentIsRent) => {
										const page = await browser.newPage();
										try {
											await page.setViewport({ width: 1920, height: 1080 });
											await page.setUserAgent(
												"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
											);

											const listing_url = `https://www.belvoir.co.uk/properties/${currentUrlPath}/?per_page=11&drawMap=&address=&address_lat_lng=&price_min=&price_max=&bedrooms_min=-1&hide_under_offer=on&yield_min=&yield_max=&pg=${currentPage}`;

											await page.goto(listing_url, {
												waitUntil: "networkidle2",
												timeout: 60000,
											});

											console.log(
												`📄 Page ${currentPage}/${totalPages}: Processing ${currentTypeLabel.toLowerCase()} properties`
											);

											// Extract properties from listing page
											const properties = await page.evaluate(() => {
												const items = [];
												document.querySelectorAll(".tease-property").forEach((element) => {
													try {
														const linkEl = element.querySelector(".text-link");
														let link = linkEl?.getAttribute("href") || null;
														if (link && !link.startsWith("http")) {
															link = "https://www.belvoir.co.uk" + link;
														}

														const addr1 = element.querySelector(".addr1")?.textContent || "";
														const addr2 = element.querySelector(".addr2")?.textContent || "";
														const title = [addr1, addr2]
															.map((t) => t.replace(/\s+/g, " ").trim())
															.filter(Boolean)
															.join(", ");

														const bedroomsText =
															element
																.querySelector(".bedroom-icon")
																?.nextElementSibling?.textContent?.trim() || "";
														const bedroomsMatch = bedroomsText.match(/\d+/);
														const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

														const priceText =
															element.querySelector(".amount")?.textContent?.trim() || "";
														const priceMatch = priceText.match(/£([\d,]+)/);
														const price = priceMatch ? priceMatch[1] : null;

														if (link && title && price) {
															items.push({ link, title, bedrooms, price });
														}
													} catch (err) {
														console.error("Error processing listing:", err.message);
													}
												});
												return items;
											});

											console.log(
												`   Found ${properties.length} properties on page ${currentPage}`
											);

											// Fetch geo data from detail pages one by one
											for (const property of properties) {
												try {
													await page.goto(property.link, {
														waitUntil: "domcontentloaded",
														timeout: 30000,
													});

													const geoData = await page.evaluate(() => {
														const scripts = Array.from(
															document.querySelectorAll('script[type="application/ld+json"]')
														);
														for (const script of scripts) {
															try {
																const data = JSON.parse(script.textContent);
																if (data.geo?.latitude && data.geo?.longitude) {
																	return {
																		latitude: data.geo.latitude,
																		longitude: data.geo.longitude,
																	};
																}
															} catch (e) {}
														}
														return null;
													});

													if (geoData) {
														property.latitude = geoData.latitude;
														property.longitude = geoData.longitude;
													}

													// Save property immediately after fetching geo data
													await updatePriceByPropertyURL(
														property.link,
														property.price,
														property.title,
														property.bedrooms,
														agent_id,
														currentIsRent,
														property.latitude,
														property.longitude
													).catch((error) => {
														console.error(`❌ Error saving property:`, error.message);
													});
												} catch (detailErr) {
													console.error(
														`Error fetching geo data for ${property.link}:`,
														detailErr.message
													);
												}
											}
										} catch (pageErr) {
											console.error(
												`Error fetching BELVOIR ${currentTypeLabel.toLowerCase()} page ${currentPage}:`,
												pageErr.message
											);
										} finally {
											await page.close();
										}
									})(pageNum, urlPath, typeLabel, is_rent)
								);
							}

							// Wait for all pages in this batch to complete
							await Promise.all(pagePromises);
							console.log(`✅ Batch ${batchStart}-${batchEnd} completed`);
						}
					} finally {
						console.log(`🔒 Closing browser for ${typeLabel}`);
						await browser.close();
						console.log(`✅ Browser closed for ${typeLabel}`);
					}
				};

				// Scrape sales first, then rentals (sequential)
				console.log(`🚀 Starting sales scrape...`);
				await scrapeBelvoirProperties("sale", 1696);
				console.log(`✅ Sales scrape completed`);

				console.log(`🚀 Starting rentals scrape...`);
				await scrapeBelvoirProperties("rent", 1196);
				console.log(`✅ Rentals scrape completed`);

				console.log(`🧹 Updating remove status...`);
				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - BELVOIR (Sales + Rentals)`);
			}

			if (agent_id == 70) {
				// Fine & Country - Sales and Rentals scraper with Puppeteer
				const scrapeFineAndCountryProperties = async (propertyType, totalPages) => {
					const recordsPerPage = 10;
					const typeLabel = propertyType === "sales" ? "SALES" : "LETTINGS";
					const urlPath =
						propertyType === "sales" ? "sales/property-for-sale" : "lettings/property-to-rent";
					const is_rent = propertyType === "lettings";

					console.log(
						`🏠 Scraping Fine & Country ${typeLabel} properties (${totalPages} pages, ${recordsPerPage} per page)`
					);

					// Launch browser once for this property type
					const browser = await puppeteer.launch({
						headless: "new",
						args: [
							"--no-sandbox",
							"--disable-setuid-sandbox",
							"--disable-dev-shm-usage",
							"--disable-accelerated-2d-canvas",
							"--disable-gpu",
						],
					});

					try {
						// Process pages in batches of 2
						const concurrencyLimit = 2;
						for (let batchStart = 1; batchStart <= totalPages; batchStart += concurrencyLimit) {
							const batchEnd = Math.min(batchStart + concurrencyLimit - 1, totalPages);
							const pagePromises = [];

							for (let pageNum = batchStart; pageNum <= batchEnd; pageNum++) {
								pagePromises.push(
									(async (currentPage, currentUrlPath, currentTypeLabel, currentIsRent) => {
										const page = await browser.newPage();
										try {
											await page.setViewport({ width: 1920, height: 1080 });
											await page.setUserAgent(
												"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
											);

											const listing_url = `https://www.fineandcountry.co.uk/${currentUrlPath}/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${currentPage}`;

											await page.goto(listing_url, {
												waitUntil: "networkidle2",
												timeout: 60000,
											});

											console.log(
												`📄 Page ${currentPage}/${totalPages}: Processing ${currentTypeLabel.toLowerCase()} properties`
											);

											// Extract properties from listing page
											const properties = await page.evaluate(() => {
												const items = [];
												const cardCount = document.querySelectorAll(".card-property").length;

												document.querySelectorAll(".card-property").forEach((element) => {
													try {
														const linkEl = element.querySelector(".property-title-link");
														const link = linkEl?.getAttribute("href") || null;

														const titleEl = element.querySelector(".property-title-link span");
														const title = titleEl?.textContent?.trim() || "";

														const priceEl = element.querySelector(".property-price");
														const priceText = priceEl?.textContent?.trim() || "";
														const priceMatch = priceText.match(/£([\d,]+)/);
														const price = priceMatch ? priceMatch[1] : null;

														const bedroomsEl = element.querySelector(".card__list-rooms li p");
														const bedrooms = bedroomsEl?.textContent?.trim() || null;

														if (link && title && price) {
															items.push({ link, title, bedrooms, price });
														}
													} catch (err) {
														console.error("Error processing listing:", err.message);
													}
												});

												return {
													items,
													cardCount,
													hasNoResults: !!document.querySelector(".no-results, .no-properties"),
													bodyText: document.body.innerText.substring(0, 200),
												};
											});

											console.log(
												`   Found ${properties.items.length} properties on page ${currentPage} (${properties.cardCount} cards total)`
											);

											if (properties.items.length === 0 && properties.cardCount === 0) {
												console.log(
													`   ⚠️ No property cards found. Page might be empty or structure changed.`
												);
												if (properties.hasNoResults) {
													console.log(`   ℹ️ "No results" message detected on page.`);
												}
											}

											// Fetch geo data from detail pages and save properties one by one
											for (const property of properties.items) {
												try {
													await page.goto(property.link, {
														waitUntil: "domcontentloaded",
														timeout: 30000,
													});

													const geoData = await page.evaluate(() => {
														const mapEl = document.querySelector("#locrating-map");
														if (mapEl) {
															const lat = mapEl.getAttribute("data-lat");
															const lng = mapEl.getAttribute("data-lang");
															if (lat && lng) {
																return {
																	latitude: parseFloat(lat),
																	longitude: parseFloat(lng),
																};
															}
														}
														return null;
													});

													if (geoData) {
														property.latitude = geoData.latitude;
														property.longitude = geoData.longitude;
													}

													await updatePriceByPropertyURL(
														property.link,
														property.price,
														property.title,
														property.bedrooms,
														agent_id,
														currentIsRent,
														property.latitude,
														property.longitude
													);
												} catch (error) {
													console.error(
														`❌ Error processing property ${property.link}:`,
														error.message
													);
												}
											}
										} catch (pageErr) {
											console.error(
												`Error fetching Fine & Country ${currentTypeLabel.toLowerCase()} page ${currentPage}:`,
												pageErr.message
											);
										} finally {
											await page.close();
										}
									})(pageNum, urlPath, typeLabel, is_rent)
								);
							}

							// Wait for all pages in this batch to complete
							await Promise.all(pagePromises);
							console.log(`✅ Batch ${batchStart}-${batchEnd} completed`);
						}
					} finally {
						console.log(`🔒 Closing browser for ${typeLabel}`);
						await browser.close();
						console.log(`✅ Browser closed for ${typeLabel}`);
					}
				};

				// Scrape sales first, then lettings (sequential)
				// await scrapeFineAndCountryProperties("sales", 608);
				// console.log(`🚀 Starting lettings scrape...`);
				await scrapeFineAndCountryProperties("lettings", 21);
				console.log(`✅ Lettings scrape completed`);

				console.log(`🧹 Updating remove status...`);
				await updateRemoveStatus(agent_id);
				console.log(`✅ Completed agent ${agent_id} - Fine & Country (Sales + Lettings)`);
			}

			if (agent_id == 208) {
				let is_rent = false;
				let totalRecords = 150;
				let recordsPerPage = 12;
				let totalPages = Math.ceil(totalRecords / recordsPerPage);

				for (let i = 0; i < totalPages; i++) {
					let start = i * recordsPerPage;

					const listing_url = `https://www.michael-everett.co.uk/properties-for-sale?start=${start}`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						let locations = [];
						const regex = /locations\.push\(\[(.*?),(.*?),(.*?)\]\)/g;
						let match;
						while ((match = regex.exec(data)) !== null) {
							const lat = parseFloat(match[1]);
							const lon = parseFloat(match[2]);
							const id = match[3].trim();
							locations.push([lat, lon, id]);
						}

						$("#smallProps .eapow-overview-row").each(async (index, element) => {
							try {
								const soldBanner = $(element).find('img[src*="banner_sold.png"]').length > 0;
								if (soldBanner) {
									console.log("Skipping SOLD property...");
									return; // skip this property and move to next
								}

								const link =
									"https://www.michael-everett.co.uk" +
									$(element).find(".eapow-property-thumb-holder a").first().attr("href");
								const matchText = $(element).find(".propPrice").text().trim();

								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title = $(element).find(".eapow-overview-title h3").first().text().trim();

								const bedrooms =
									$(element).find(".listing-icons span").first().text().trim() || null;

								const [latitude, longitude, propertyId] = locations[index] || [null, null, null];

								await updatePriceByPropertyURL(
									link,
									price,
									title,
									bedrooms,
									agent_id,
									is_rent,
									latitude,
									longitude
								);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});

						await browser.close();
					} catch (err) {
						console.error(`Error processing page ${i}: ${err.message}`);
					}
				}
			}

			if (agent_id == 208) {
				let is_rent = true;
				let totalRecords = 7;
				let recordsPerPage = 12;
				let totalPages = Math.ceil(totalRecords / recordsPerPage);

				for (let i = 0; i < totalPages; i++) {
					let start = i * recordsPerPage;

					const listing_url = `https://www.michael-everett.co.uk/properties-to-let?start=${start}`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						let locations = [];
						const regex = /locations\.push\(\[(.*?),(.*?),(.*?)\]\)/g;
						let match;
						while ((match = regex.exec(data)) !== null) {
							const lat = parseFloat(match[1]);
							const lon = parseFloat(match[2]);
							const id = match[3].trim();
							locations.push([lat, lon, id]);
						}

						$("#smallProps .eapow-overview-row").each(async (index, element) => {
							try {
								const link =
									"https://www.michael-everett.co.uk" +
									$(element).find(".eapow-property-thumb-holder a").first().attr("href");
								const matchText = $(element).find(".propPrice").text().trim();

								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title = $(element).find(".eapow-overview-title h3").first().text().trim();

								const bedrooms =
									$(element).find(".listing-icons span").first().text().trim() || null;

								const [latitude, longitude, propertyId] = locations[index] || [null, null, null];

								await updatePriceByPropertyURL(
									link,
									price,
									title,
									bedrooms,
									agent_id,
									is_rent,
									latitude,
									longitude
								);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});

						await browser.close();
					} catch (err) {
						console.error(`Error processing page ${i}: ${err.message}`);
					}
				}
			}

			if (agent_id == 207) {
				let page_no = 2;
				let is_rent = false;
				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.scottcity.co.uk/buy/property-for-sale/?page=${i}`;
					console.log(listing_url);

					try {
						// Puppeteer setup and scraping
						const browser = await puppeteer.launch();
						const page = await browser.newPage();

						await page.setUserAgent(
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
								"AppleWebKit/537.36 (KHTML, like Gecko) " +
								"Chrome/123.0.0.0 Safari/537.36"
						);

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const content = await page.content();
						const $ = cheerio.load(content);

						// 1️⃣ Extract the JSON with all property data
						// Extract JSON with all property data
						const scriptContent = $('script[type="text/javascript"]')
							.map((i, el) => $(el).html())
							.get()
							.find((c) => c.includes("var properties ="));

						const vm = require("vm");
						let properties = [];
						if (scriptContent) {
							try {
								const sandbox = {};
								vm.createContext(sandbox); // create isolated context
								vm.runInContext(scriptContent, sandbox); // run the script safely
								properties = sandbox.properties; // now you have the array as JS objects
							} catch (err) {
								console.error("Failed to evaluate properties:", err.message);
							}
						}

						$(".property-list .property").each(async (index, element) => {
							try {
								const link =
									"https://www.scottcity.co.uk" +
									$(element).find("a.property-description-link").first().attr("href");
								const matchText = $(element).find(".list-price").text().trim();

								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title = $(element).find(".list-address").first().text().trim();

								//await page.waitForSelector('.FeaturedProperty__list-stats-item--bedrooms span', { visible: true });
								const bedrooms =
									$(element)
										.find("li.FeaturedProperty__list-stats-item--bedrooms span")
										.first()
										.text()
										.trim() || null;

								const dataId =
									$(element).find("a.add_bookmark.bookmark").attr("data-id")?.trim() || null;

								// Look up latitude & longitude from JSON by PropertyId
								let latitude = null;
								let longitude = null;
								if (dataId && properties.length > 0) {
									const found = properties.find((p) => p.PropertyId === dataId);
									if (found) {
										latitude = found.latitude;
										longitude = found.longitude;
									}
								}

								console.log(latitude, longitude);

								//console.log(link, price, title, bedrooms, agent_id);

								await updatePriceByPropertyURL(
									link,
									price,
									title,
									bedrooms,
									agent_id,
									is_rent,
									latitude,
									longitude
								);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});

						await browser.close();
					} catch (err) {
						console.error(`Error processing page ${i}: ${err.message}`);
					}
				}
			}

			if (agent_id == 207) {
				// You can change this to the correct agent_id
				let page_no = 1;

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.scottcity.co.uk/let/property-to-let`;
					console.log(listing_url);

					try {
						// Puppeteer setup and scraping
						const browser = await puppeteer.launch();
						const page = await browser.newPage();

						await page.setUserAgent(
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) " +
								"AppleWebKit/537.36 (KHTML, like Gecko) " +
								"Chrome/123.0.0.0 Safari/537.36"
						);

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						// Wait for property list to load
						await page.waitForSelector(".property-list .property");

						const content = await page.content();
						const $ = cheerio.load(content);

						// Extract JSON with all property data
						const scriptContent = $('script[type="text/javascript"]')
							.map((i, el) => $(el).html())
							.get()
							.find((c) => c.includes("var properties ="));
						const vm = require("vm");
						let properties = [];
						if (scriptContent) {
							try {
								const sandbox = {};
								vm.createContext(sandbox); // create isolated context
								vm.runInContext(scriptContent, sandbox); // run the script safely
								properties = sandbox.properties; // now you have the array as JS objects
							} catch (err) {
								console.error("Failed to evaluate properties:", err.message);
							}
						}

						$(".property-list .property").each(async (index, element) => {
							try {
								const link =
									"https://www.scottcity.co.uk" +
									$(element).find("a.property-description-link").first().attr("href");

								const matchText = $(element).find(".list-price").text().trim();
								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title = $(element).find(".list-address").first().text().trim();

								const bedrooms =
									$(element)
										.find("li.FeaturedProperty__list-stats-item--bedrooms span")
										.first()
										.text()
										.trim() || null;

								const dataId =
									$(element).find("a.add_bookmark.bookmark").attr("data-id")?.trim() || null;

								// Look up latitude & longitude from JSON by PropertyId
								let latitude = null;
								let longitude = null;
								if (dataId && properties.length > 0) {
									const found = properties.find((p) => p.PropertyId === dataId);
									if (found) {
										latitude = found.latitude;
										longitude = found.longitude;
									}
								}

								let is_rent = true;
								await updatePriceByPropertyURL(
									link,
									price,
									title,
									bedrooms,
									agent_id,
									is_rent,
									latitude,
									longitude
								);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});

						await browser.close();
					} catch (err) {
						console.error(`Error processing page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Patrick Gardner
			if (agent_id == 5) {
				let page_no = 20;

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.patrickgardner.com/property-search/page/${i}/?radius=1&availability[]=2&availability[]=6`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property-cols .work_box").each(async (index, element) => {
							try {
								const link = $(element).find("a").first().attr("href");
								const matchText = $(element).find(".common-sticker.sub_title h6").first().text();

								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title = $(element).find(".sub_title h5").first().text();

								const bedrooms = null; // If there's a way to extract the number of bedrooms, you can add it here.

								// Log or process the data
								// console.log(link, price, title, bedrooms);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Dexters
			if (agent_id == 3) {
				let page_no = 257;

				for (let i = 1; i <= page_no; i++) {
					const listing_url =
						"https://www.dexters.co.uk/property-sales/properties-available-for-sale-in-london/page-" +
						i;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".result .result-content").each(async (index, element) => {
							try {
								const link =
									"https://www.dexters.co.uk" + $(element).find("a").first().attr("href");

								const matchText = $(element).find(".price-qualifier").first().text();
								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title =
									$(element).find("span.address-area-post").first().text().trim() || null;

								let bedrooms = $(element).find(".list-info .Bedrooms").first().text().trim();
								const bedroomMatch = bedrooms.match(/(\d+)\s*Bed/);
								bedrooms = bedroomMatch ? bedroomMatch[1] : null;

								// Log or perform the desired action
								// console.log(link, price, title, bedrooms);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Purple Bricks
			if (agent_id == 12) {
				let page_no = 92;

				for (let i = 1; i <= page_no; i++) {
					const listing_url =
						"https://www.purplebricks.co.uk/search/property-for-sale/greater-london/london?page=" +
						i +
						"&sortBy=2&searchType=ForSale&searchRadius=2&soldOrLet=false&location=london&latitude=51.5072178&longitude=-0.1275862&betasearch=true";

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						const links = [];

						$(".search-resultsstyled__StyledSearchResultsContainer-krg5hu-1 li").each(
							async (index, element) => {
								try {
									const link =
										"https://www.purplebricks.co.uk" + $(element).find("a").first().attr("href");

									var title = $(element)
										.find(".property-cardstyled__StyledAddress-sc-15g6092-10")
										.first()
										.text();

									var bedrooms = null; // Update with proper extraction if available

									var matchText = $(element).find("a").first().attr("aria-label");

									var match_price = matchText.match(/£([\d,]+)/);
									var price = match_price ? match_price[1] : null;

									// Log or process listing
									//console.log(link, price);

									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								} catch (err) {
									console.error(
										`Error processing listing on page ${i}, index ${index}: ${err.message}`
									);
								}
							}
						);
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Acorn
			if (agent_id == 42) {
				let page_no = 98;
				for (let i = 1; i <= page_no; i++) {
					const listing_url =
						"https://www.acorngroup.co.uk/property-search/properties-available-for-sale-more-than-25-miles-of-london/page-" +
						i;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".card.card--long-main").each(async (index, element) => {
							try {
								const link =
									"https://www.acorngroup.co.uk" + $(element).find("a").first().attr("href");

								const matchText = $(element).find(".price-qualifier").first().text();
								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title = $(element).find(".card__content-inner p").first().text();

								const bedrooms = $(element).find(".icon-bedroom").siblings("span").text().trim();

								// Log the results or perform actions
								//console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Marsh & Parsons
			if (agent_id == 4) {
				let page_no = 20;
				for (let i = 1; i <= page_no; i++) {
					const listing_url =
						"https://www.marshandparsons.co.uk/properties-for-sale/london/?filters=exclude_sold%2Cexclude_under_offer&page=" +
						i;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".flex-none .my-4").each(async (index, element) => {
							try {
								const link = $(element).find("a").first().attr("href");

								var matchText = $(element).find(".text-MAP_teal").first().text();
								var match_price = matchText.match(/£([\d,]+)/);
								var price = match_price ? match_price[1] : null;

								var title = $(element).find(".text-xl span").first().text();

								var bedrooms = $(element).find(".gap-4.items-center").first().text().trim();

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Bairstow Eves
			if (agent_id == 13) {
				let page_no = 55;
				for (let i = 1; i <= page_no; i++) {
					const listing_url =
						"https://www.bairstoweves.co.uk/properties/sales/status-available/page-" + i + "#/";

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						// Puppeteer setup and page scraping
						const browser = await puppeteer.launch();
						const page = await browser.newPage();

						await page.goto(listing_url, { waitUntil: "networkidle2" }); // Navigate to the page

						const content = await page.content();
						const $ = cheerio.load(content);

						const cards = $(".hf-property-results .card");
						console.log("Total cards found:", cards.length);

						// Process each card
						cards.each(async (index, element) => {
							try {
								const link =
									"https://www.bairstoweves.co.uk" + $(element).find("a").first().attr("href");

								const matchText = $(element).find(".card__link span").first().text();
								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title = $(element)
									.find(".card__link")
									.contents()
									.filter(function () {
										return this.nodeType === 3 && this.nodeValue.trim() !== "";
									})
									.text()
									.trim()
									.replace(/,/g, "");

								const bedrooms = $(element)
									.find(".card-content__spec-list-number")
									.first()
									.text()
									.trim();

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});

						// Close the browser after processing all listings
						await browser.close();
					} catch (err) {
						console.error(`Error processing page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Brinkleys
			if (agent_id == 44) {
				let page_no = 3;

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.brinkleys.co.uk/property-search/page/${i}/?orderby&instruction_type=letting&address_keyword&min_bedrooms&minprice&maxprice&property_type&showstc=off`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						const links = [];

						$(".property-grid").each(async (index, element) => {
							try {
								const link = $(element).find("a").first().attr("href");

								const metaContainer = $(element).find(".property-grid__meta");

								const title = metaContainer.find("h4").text().trim();
								const h5Element = metaContainer.find("h5");

								const h5Text = h5Element.text();

								const bedroomMatch = h5Text.match(/(\d+)\s*Bed/);
								const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

								const priceText = metaContainer.find("h6 span").text();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// Log or process listing
								// console.log(link, title, bedrooms, price, agent_id);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Century21
			if (agent_id == 45) {
				let page_no = 22;

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.century21uk.com/all-properties-for-sale/bedrooms/0-31?exclude-sale-agreed=1&exclude-sstc=1&page=${i}`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".property-card").each(async (index, element) => {
							try {
								const link =
									"https://www.century21uk.com" +
									($(element)
										.find(".swiper-slide")
										.attr("onclick")
										?.match(/'([^']+)'/)?.[1] ?? "");

								const title = $(element).find("h4").text().trim();

								const bedroomMatch = $(element)
									.find(".mr-3 span")
									.text()
									.match(/(\d+)\s*BED/);
								const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

								const priceMatch = $(element)
									.find(".mb-4 span")
									.text()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing property on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching Century21 page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Chase Evans
			if (agent_id == 37) {
				const page_no = 18;

				const browser = await puppeteer.launch({ headless: true });

				try {
					for (let i = 1; i <= page_no; i++) {
						const listing_url = `https://www.chaseevans.co.uk/property/for-sale/in-london/page-${i}/`;

						const page = await browser.newPage();

						try {
							await page.setUserAgent(
								"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
							);

							await page.goto(listing_url, { waitUntil: "networkidle2" });

							await page.waitForSelector(".sales-section", {
								timeout: 10000,
							});

							const listings = await page.$$eval(".sales-wrap", (elements) =>
								elements.map((el) => {
									const linkPath = el.querySelector(".slide-content > a")?.getAttribute("href");
									const link = linkPath ? "https://www.chaseevans.co.uk" + linkPath : null;

									const title = el.querySelector(".slide-content h3")?.innerText.trim() || null;
									const bedrooms =
										el.querySelector(".icon-wrap .icon-bed + .count")?.innerText.trim() || null;

									const priceText = el.querySelector(".highlight-text")?.innerText || "";
									const priceMatch = priceText.match(/£([\d,]+)/);
									const price = priceMatch ? priceMatch[1] : null;

									return { link, title, bedrooms, price };
								})
							);

							console.log(`--- Page ${i} ---`);
							for (const { link, title, bedrooms, price } of listings) {
								try {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								} catch (err) {
									console.error(`Error updating property on page ${i}: ${err.message}`);
								}
							}
						} catch (err) {
							console.error(`Error processing page ${i}: ${err.message}`);
						} finally {
							await page.close();
						}
					}
				} catch (err) {
					console.error(`Browser-level error: ${err.message}`);
				} finally {
					await browser.close();
				}
			}

			// Agent - Chestertons
			if (agent_id == 14) {
				let page_no = 213;

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.chestertons.co.uk/properties/sales?page=${i}`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".pegasus-property-card").each(async (index, element) => {
							try {
								const link = "https://www.century21uk.com" + $(element).find("a").attr("href");
								const title = $(element).find(".text-base.pr-10").text().trim() || null;

								const bedroomMatch = $(element)
									.find("span.flex.flex-row.items-center.text-xs")
									.text()
									.match(/(\d+)/);
								const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

								const priceMatch = $(element)
									.find("span.font-bold")
									.text()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Choices
			if (agent_id == 109) {
				let page_no = 9;

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.choices.co.uk/property-search/page/${i}/?orderby&instruction_type=sale&address_keyword&min_bedrooms&minprice&maxprice&property_type&showstc=off`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".property-grid").each(async (index, element) => {
							try {
								const link = $(element).find("a").first().attr("href");

								const metaContainer = $(element).find(".property-grid__meta");

								const title = metaContainer
									.find("h4 span")
									.map((_, el) => $(el).text().trim())
									.get()
									.join(" ");

								const h5Element = metaContainer.find("h5");
								const h5Text = h5Element.text();

								const bedroomMatch = h5Text.match(/(\d+)\s*Bed/);
								const bedrooms = bedroomMatch ? bedroomMatch[1] : null;

								const priceText = metaContainer.find("h6 span").text();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// Log or perform action
								// console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Connells
			if (agent_id == 46) {
				let page_no = 434; //434

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.connells.co.uk/properties/sales/page-${i}`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".property__inner").each(async (index, element) => {
							try {
								const link = $(element).find(".property-card-images__link").attr("href")
									? "https://www.connells.co.uk" +
									  $(element).find(".property-card-images__link").attr("href")
									: null;

								const title = $(element).find(".property__address").text().trim() || null;

								const bedroomsText = $(element).find(".property__summary").text().trim();
								const bedrooms = bedroomsText ? bedroomsText.split(" ")[0] : null;

								const priceText = $(element)
									.find(".property__price-info-item.property__price")
									.text()
									.trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								//console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Douglas Allen
			if (agent_id == 47) {
				const page_no = 49; //49
				const browser = await puppeteer.launch({ headless: true });

				try {
					for (let i = 1; i <= page_no; i++) {
						const listing_url = `https://www.douglasallen.co.uk/property/for-sale/in-chipping-ognar-essex/radius-30-miles/page-${i}/`;

						try {
							const page = await browser.newPage();

							await page.setUserAgent(
								"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
							);

							await page.goto(listing_url, { waitUntil: "networkidle2" });
							await page.waitForSelector(".property-card", {
								timeout: 10000,
							});

							const content = await page.content();
							const $ = cheerio.load(content);

							$(".property-card").each(async (index, element) => {
								try {
									const link = $(element).find("a").attr("href")
										? "https://www.douglasallen.co.uk" + $(element).find("a").attr("href")
										: null;

									const title =
										$(element).find(".properties-info > h2").find("a").text().trim() || null;

									const bedroomsText = $(element)
										.find(".icon-bedroom")
										.siblings("span")
										.text()
										.trim();
									const bedrooms = bedroomsText ? bedroomsText : null;

									const priceText = $(element).find(".property-price").text().trim();
									const priceMatch = priceText.match(/£([\d,]+)/);
									const price = priceMatch ? priceMatch[1] : null;

									// console.log(link, title, bedrooms, price);

									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								} catch (err) {
									console.error(
										`Error processing listing on page ${i}, index ${index}: ${err.message}`
									);
								}
							});

							await page.close();
						} catch (err) {
							console.error(`Error processing page ${i}: ${err.message}`);
						}
					}
				} catch (err) {
					console.error(`General error during Douglas Allen scraping: ${err.message}`);
				} finally {
					await browser.close();
				}
			}

			//Agent - Douglas and Gordon
			if (agent_id == 48) {
				const browser = await puppeteer.launch({ headless: false });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 15; //15

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.douglasandgordon.com/buy/list/anywhere/houses-and-flats/?filter=exclude-under-offer&usersearch=true&page=${i}`;

					await page.goto(listing_url, { waitUntil: "networkidle2" });

					const html = await page.content();
					const $ = cheerio.load(html);

					$(".type-lbl").each(async (index, element) => {
						const anchor = $(element).find("a");

						const link = anchor.attr("href")
							? "https://www.douglasandgordon.com" + anchor.attr("href")
							: null;

						const title = $(element).find(".cta-link").text().trim() || null;

						const bedroomsText = $(element).find(".ico-bedroom").text().trim();
						const bedrooms = bedroomsText ? bedroomsText.split(" ")[0] : null;

						const priceText = $(element).find(".col.m-0.mr-2").text().trim();
						const priceMatch = priceText.match(/£([\d,]+)/);
						const price = priceMatch ? priceMatch[1] : null;

						// console.log(link, title, bedrooms, price, agent_id);

						await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
					});
				}

				await browser.close();
			}

			//Agent - Frank Hariss
			if (agent_id == 51) {
				let page_no = 7;
				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.frankharris.co.uk/properties/sales/status-available/page-${i}`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".property-card").each(async (index, element) => {
							try {
								const anchor = $(element).find("a");
								const link = anchor.attr("href")
									? "https://www.frankharris.co.uk" + anchor.attr("href")
									: null;

								const title =
									$(element).find(".property-card-content").find("a").attr("title") || null;

								const bedroomsText = $(element).find(".bed-baths").first().text().trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*bed/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $(element).find(".price > data").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`❌ Error parsing property card on page ${i}, index ${index}:`,
									err.message
								);
							}
						});
					} catch (err) {
						console.error(`❌ Failed to fetch page ${i}:`, err.message);
					}
				}
			}

			//Agent - Haart
			if (agent_id == 52) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 27; // Set this to 27 or more if needed

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.haart.co.uk/property-results/?IsPurchase=true&Location=London,%20Greater%20London&SearchDistance=50&Latitude=51.51437&Longitude=-0.09229&MinPrice=0&MaxPrice=100000000&MinimumBeds=0&SortBy=HighestPrice&NumberOfResults=6&Page=${i}&Stc=False&OnMkt=True&PropertyTypes=0`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".property-box").each(async (index, element) => {
							try {
								const anchor = $(element).find("a");
								const link = anchor.attr("href")
									? "https://www.haart.co.uk" + anchor.attr("href")
									: null;

								const title = $(element).find(".propAida").text().trim() || null;

								const bedroomsText = $(element).find(".propBeds").text().trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*bedroom/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $(element).find(".propPrice").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// console.log(link, price, title, bedrooms, agent_id);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`❌ Error parsing listing on page ${i}, index ${index}:`,
									err.message
								);
							}
						});
					} catch (err) {
						console.error(`❌ Failed to fetch page ${i}:`, err.message);
					}
				}
				await browser.close();
			}

			//Agent - Hamptons
			if (agent_id == 108) {
				let page_no = 1; // Update to 221 as needed

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.hamptons.co.uk/london/london/sales/within-25-miles/status-available/page-${i}#/`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".property-card").each(async (index, element) => {
							try {
								const anchor = $(element).find("a");
								const link = anchor.attr("href")
									? "https://www.hamptons.co.uk" + anchor.attr("href")
									: null;

								const title = $(element).find(".property-card__title").text().trim() || null;

								const bedroomsText = $(element)
									.find(".property-card__bedbath-item")
									.first()
									.text()
									.trim();
								const bedrooms = bedroomsText ? bedroomsText : null;

								const priceText = $(element).find(".property-card__price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`❌ Error parsing listing on page ${i}, index ${index}:`,
									err.message
								);
							}
						});
					} catch (err) {
						console.error(`❌ Failed to fetch page ${i}:`, err.message);
					}
				}
			}

			// Agent - Hunters
			if (agent_id == 38) {
				let page_no = 571; // Total number of pages to scrape

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.hunters.com/search-results/for-sale/in-england-and-wales/page-${i}/?orderby=price_desc&department=residential`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".property--card").each(async (index, element) => {
							try {
								const anchor = $(element).find(".property-title").find("a");
								const link = anchor.attr("href") ? anchor.attr("href") : null;

								const title = $(element).find(".property-title span").text().trim() || null;

								const bedroomsText = $(element).find(".property-type").text().trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*bedroom/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $(element).find(".property-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`❌ Error parsing listing on page ${i}, index ${index}:`,
									err.message
								);
							}
						});
					} catch (err) {
						console.error(`❌ Failed to fetch page ${i}:`, err.message);
					}
				}
			}

			// Agent - Sothebys
			if (agent_id == 55) {
				let page_no = 23;

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://sothebysrealty.co.uk/buy/property-for-sale/?p=${i}`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".small-slider-container")
							.siblings("a")
							.each(async (index, element) => {
								try {
									const $element = $(element);

									const link = $element.attr("href")
										? "https://sothebysrealty.co.uk" + $element.attr("href")
										: null;

									const title =
										$element
											.find("span.text-lg")
											.text()
											.trim()
											.replace(/\n/g, "")
											.replace(/\s+/g, " ") || null;

									const bedroomsText = $element
										.find(".flex.items-start.justify-center.gap-x-1 > span")
										.first()
										.text()
										.trim();
									const bedrooms = bedroomsText ? bedroomsText : null;

									const priceText = $element
										.find(".font-benton .font-normal.text-sm.items-center")
										.text()
										.trim();
									const priceMatch = priceText.match(/£ ([\d,]+)/);
									const price = priceMatch ? priceMatch[1] : null;

									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								} catch (err) {
									console.error(
										`❌ Error parsing listing on page ${i}, index ${index}:`,
										err.message
									);
								}
							});
					} catch (err) {
						console.error(`❌ Failed to fetch page ${i}:`, err.message);
					}
				}
			}

			// Agent - Statons
			if (agent_id == 115) {
				let page_no = 29;

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.statons.com/property-search/page/${i}/?department=residential-sales&address_keyword&availability%5B0%5D=2&availability%5B1%5D=6`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".work_box").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".sub_title").text().trim() || null;

								const bedroomsText = $element
									.find(".flex.items-start.justify-center.gap-x-1 > span")
									.first()
									.text()
									.trim();
								const bedrooms = bedroomsText ? bedroomsText : null;

								const priceText = $element.find(".price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`❌ Error parsing listing on page ${i}, index ${index}:`,
									err.message
								);
							}
						});
					} catch (err) {
						console.error(`❌ Failed to fetch page ${i}:`, err.message);
					}
				}
			}

			// Agent - Stirling Ackroyd
			if (agent_id == 56) {
				let page_no = 71; //71

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.stirlingackroyd.com/property-search/page/${i}/?department=residential-sales&address_keyword&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&utm_campaign=pre-appraisal-first-email-no-click-chaser+%287%29&utm_medium=email&utm_source=email_ibm&spMailingID=25447489&spUserID=MTgzMzMxODczODQwOAS2&spJobID=2630838489&spReportId=MjYzMDc5NjU0OAS2&radius=0.75&view=list`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".properties > li").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".details h6 a").text().trim() || null;

								const bedroomsText = $element.find("li:has(.fa-bed-front)").text().trim();
								const bedrooms = bedroomsText ? bedroomsText.replace(/\D/g, "") : null;

								const priceText = $element.find(".price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// Log the results or perform actions
								//console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error processing listing element: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Winkworth
			if (agent_id == 36) {
				let page_no = 186; //186

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.winkworth.co.uk/PropertiesSearch/Index?locationName=&countyName=Hertfordshire&office=&orderBy=&status=&channel=7f45d0b8-2d58-4403-a338-2f99b676254f&viewType=&Location=berkshire&priceFrom=&priceTo=&bedroomsFrom=&bedroomsTo=&propertyType=all&IncludeUnderOffer=false&IncludeSoldLet=false&page=${i}`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".search-result-property").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element
									.find(".search-result-property__content-card-link")
									.attr("href")
									? "https://www.winkworth.co.uk" + $element.find("a").attr("href")
									: null;

								const title = $element.find(".search-result-property__title").text().trim() || null;

								const bedroomsText = $element.find(".specs__text:first").text().trim();
								const bedrooms = bedroomsText ? bedroomsText.replace(/\D/g, "") : null;

								const priceText = $element.find(".search-result-property__price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// Log the results or perform actions
								//console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error processing listing element: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Yopa
			if (agent_id == 58) {
				let page_no = 498; //498

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.yopa.co.uk/houses-for-sale/?price_min=&price_max=&bedrooms=&page=${i}&filter_by=recent`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".property-search-card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".property-heading__title").text().trim() || null;

								const bedroomsText = $element.find(".property-type").text().trim();
								const bedrooms = bedroomsText ? bedroomsText.replace(/\D/g, "") : null;

								const priceText = $element.find(".property-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// Log the results or perform actions
								//console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error processing listing element: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Andrews
			if (agent_id == 69) {
				let page_no = 164; //164

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.andrewsonline.co.uk/property-for-sale/properties-for-sale-in-uk/page-${i}`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".search-item").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.andrewsonline.co.uk" + $element.find("a").attr("href")
									: null;

								const title = $element.find(".title-underline").text().trim() || null;

								const bedroomsText = $element.find("a > span").text().trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*bedroom/i);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $element.find(".price-qualifier").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// Log the results or perform actions
								//console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - White & Sons
			if (agent_id == 84) {
				let page_no = 21; //21

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.whiteandsons.co.uk/properties/sales/status-available/page-${i}#/`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".property-card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find(".card-link").attr("href")
									? "https://www.whiteandsons.co.uk" + $element.find(".card-link").attr("href")
									: null;

								const title = $element.find(".card-link span").text().trim() || null;

								const bedroomsText = $element.find(".card-link > h4").text().trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*bedroom/i);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $element.find(".property-card__price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								//console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing property at index ${index} on page ${i}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Barratt Homes
			if (agent_id == 60) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				const listing_url = `https://www.barratthomes.co.uk/search-results/?qloc=London%252C%2520UK&latLng=51.5072178%252C-0.1275862&view=LIST`;

				await page.goto(listing_url, { waitUntil: "networkidle2" });

				// Wait for listing elements to be rendered
				await page.waitForSelector(".results > li", { timeout: 10000 });

				const html = await page.content();
				const $ = cheerio.load(html);

				$(".results > li").each(async (index, element) => {
					const $element = $(element);
					const link = $element.find("a").attr("href") || null;
					const title = $element.find(".development-cta__title").text().trim() || null;

					const bedroomsText = $element.find(".development-cta__feature").text().trim();
					const bedroomsMatch = bedroomsText.match(/(\d+)(?:,\s*\d+)*\s*bedroom/i);
					const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

					const priceText = $element.find(".development-cta__feature").text().trim();
					const priceMatch = priceText.match(/£([\d,]+)\s*to\s*£([\d,]+)/i);
					const price = priceMatch ? priceMatch[2] : null;

					await updatePriceByPropertyURL(link, title, bedrooms, price, agent_id);
				});

				await browser.close();
			}

			// Agent - Bellway Homes
			if (agent_id == 61) {
				try {
					const listing_url = `https://www.bellway.co.uk/new-homes/results?placeId=ChIJdd4hrwug2EcRmSrV3Vo6llI&keyword=London%2C+UK&goodToGo=false`;

					const { data } = await axios.get(listing_url);
					const $ = cheerio.load(data);

					$(".tile").each(async (index, element) => {
						try {
							const $element = $(element);

							const link = $element.find("a").attr("href")
								? "https://www.bellway.co.uk" + $element.find("a").attr("href")
								: null;

							const title = $element.find(".heading").text().trim() || null;

							const bedroomsText = $element.find(".icon-bed2").siblings("div").text().trim();
							const bedroomsMatch = bedroomsText.match(/(\d+)\s*bedroom/i);
							const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

							const priceText = $element.find(".price > strong").text().trim();
							const priceMatch = priceText.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						} catch (innerErr) {
							console.error(`Error processing tile at index ${index}:`, innerErr);
						}
					});
				} catch (err) {
					console.error("Error fetching or parsing Bellway Homes listings:", err);
				}
			}

			// Agent - Redrow
			if (agent_id == 77) {
				let page_no = 3;

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.redrow.co.uk/search?displaytype=List&distance=40&isNational=false&latitude=51.515993&longitude=-0.1392256&page=${i}&searchtype=Development&sortby=Default&term=United%20Kingdom%20House`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".zone-size-4").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find(".development-link-wrap").attr("href")
									? "https://www.redrow.co.uk" +
									  $element.find(".development-link-wrap").attr("href")
									: null;

								const title = $element.find("h3").text().trim() || null;

								const bedroomsText = $element.find(".beds > span").text().trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*bed/i);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $element.find(".price > span").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing Redrow listings on page ${i}:`, err);
					}
				}
			}

			// Agent - Aston Chase
			if (agent_id == 59) {
				let page_no = 1; // You can change this to 2 or more as needed

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://astonchase.com/property-list/page/${i}/?dpt=sale&pstus=exclude&srtbyprice=high#038;pstus=exclude&srtbyprice=high`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".properties-content").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".address > p > a").text().trim() || null;

								const bedroomsText = $element.find(".beds > span").text().trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*bed/i);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $element.find(".price > a > span").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing Aston Chase listings on page ${i}:`, err);
					}
				}
			}

			// Agent - Fine & Country (OLD - REMOVED - Now using new Puppeteer stealth version above)
			// if (agent_id == 70) {
			// 	const browser = await puppeteer.launch({ headless: true });
			// 	const page = await browser.newPage();

			// 	await page.setUserAgent(
			// 		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
			// 	);

			// 	let page_no = 356; //356

			// 	for (let i = 1; i <= page_no; i++) {
			// 		const listing_url = `https://www.fineandcountry.co.uk/sales/property-for-sale/united-kingdom?currency=GBP&addOptions=sold&sortBy=price-high&country=GB&address=United%20Kingdom&page=${i}`;

			// 		await page.goto(listing_url, { waitUntil: "networkidle2" });

			// 		const html = await page.content();
			// 		const $ = cheerio.load(html);

			// 		$(".card-property").each(async (index, element) => {
			// 			const $element = $(element);

			// 			const linkPath = $element.find(".slide__media > a").attr("href");
			// 			const link = linkPath && linkPath !== "javascript:void(0)" ? linkPath : null;

			// 			const title = $element.find(".property-title-link > span").text().trim() || null;

			// 			const bedroomsText = $element.find(".card__list-rooms li > p").text().trim();
			// 			const bedroomsMatch = bedroomsText.match(/(\d+)/); // Capture only the number
			// 			const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

			// 			const priceText = $element.find(".property-price > span").text().trim();
			// 			const priceMatch = priceText.match(/£([\d,]+)/);
			// 			const price = priceMatch ? priceMatch[1] : null;

			// 			// console.log(link, title, bedrooms, price);

			// 			await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
			// 		});
			// 	}

			// 	await browser.close();
			// }

			// Agent - Berkshire Hathaway
			if (agent_id == 63) {
				let page_no = 5;

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.bhhslondonproperties.com/properties-for-sale?page=${i}`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property-card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find("h3").text().trim() || null;

								const bedroomsText = $element
									.find("p.text-sm.text-white.montserrat.semi.uppercase")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*Bedrooms/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $element.find(".price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(
							`Error fetching or parsing Berkshire Hathaway listings on page ${i}:`,
							err
						);
					}
				}
			}

			// Agent - Kinleigh Folkard & Hayward
			if (agent_id == 75) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 277; //277

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.kfh.co.uk/search-results/?category=RESALE&currencyid=1&first=12&nearme=false&newhomes=true&onlynewhomes=false&page=${i}&priceHighest=100000000&priceLowest=0&riverside=false&sort=HIGHEST&type=RESIDENTIAL&unavailable=false&underoffer=false`;
						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);
						$(".PropertyCard__StyledPropertyCard-sc-1kiuolp-0").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find(".property-card-image").attr("href")
									? "https://www.kfh.co.uk" + $element.find(".property-card-image").attr("href")
									: null;

								const title =
									$element.find(".PropertyCard__StyledAddressLink-sc-1kiuolp-10").text().trim() ||
									null;

								const bedroomsText = $element
									.find(".PropertyMeta__StyledMetaItem-sc-1sityta-1")
									.first()
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/(\d+)\s*bedroom/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[1] : null;

								const priceText = $element
									.find(".Typography__TypographyStyles-sc-rpmtkm-0")
									.text()
									.trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing KFH listings on page ${i}:`, err);
					}
				}
				await browser.close();
			}

			// Agent - JLL
			if (agent_id == 72) {
				let page_no = 1; // Adjust to 17 or more as needed

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://residential.jll.co.uk/search?tenureType=sale&sortBy=price&sortDirection=desc&page=${i}`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".SRPPropertyCard").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://residential.jll.co.uk" + $element.find("a").attr("href")
									: null;

								const title = $element.find(".SRPPropertyCard__title").text().trim() || null;

								const bedroomsText = $element.find(".bedrooms span").text().trim();
								const bedrooms = bedroomsText ? bedroomsText : null;

								const priceText = $element.find(".PropertyMetric__item").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing JLL listings on page ${i}:`, err);
					}
				}
			}

			// Agent - John D Wood
			if (agent_id == 39) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 17; //17

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.johndwood.co.uk/all-properties-for-sale/status-available/most-recent-first/page-${i}#/`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find(".card__link").attr("href")
									? "https://www.johndwood.co.uk" + $element.find(".card__link").attr("href")
									: null;

								const title = $element.find(".card__text-content").text().trim() || null;

								const bedroomsText = $element.find(".card__text-title").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".card__heading").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing John D Wood listings on page ${i}:`, err);
					}
				}
				await browser.close();
			}

			// Agent - Ellis & Co
			if (agent_id == 67) {
				let page_no = 44; // Adjust if needed

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.ellisandco.co.uk/search-results/for-sale/in-london/page-${i}/?orderby=price_desc&department=residential`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property--card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title =
									$element.find(".property-title a").text().replace(/\s+/g, " ").trim() || null;

								const bedroomsText = $element.find(".property-type").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".property-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing Ellis & Co listings on page ${i}:`, err);
					}
				}
			}

			// Agent - Daniel Cobb
			if (agent_id == 66) {
				let page_no = 8; // Adjust as needed

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.danielcobb.co.uk/property-sales/properties-available-for-sale-in-london/page-${i}`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.danielcobb.co.uk" + $element.find("a").attr("href")
									: null;

								const title =
									$element.find(".form-control-static a").text().replace(/\s+/g, " ").trim() ||
									null;

								const bedroomsText = $element.find(".h3 a").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".price-container strong").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing Daniel Cobb listings on page ${i}:`, err);
					}
				}
			}

			// Agent - Martyn Gerrard
			if (agent_id == 76) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 19; // Adjust if needed

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.martyngerrard.co.uk/property/for-sale/in-london/available/page-${i}/`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".property-card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.martyngerrard.co.uk" + $element.find("a").attr("href")
									: null;

								const title =
									$element.find(".address-title").text().replace(/\s+/g, " ").trim() || null;

								const bedroomsText = $element.find(".prop-title").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".price_qua_price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing Martyn Gerrard listings on page ${i}:`, err);
					}
				}

				await browser.close();
			}

			// Agent - Stow Brothers
			if (agent_id == 80) {
				let page_no = 19; // Adjust as necessary

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.stowbrothers.com/property-search/page/${i}/`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".type-property").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.stowbrothers.com" + $element.find("a").attr("href")
									: null;

								const title = $element.find(".w-2-cols").text().replace(/\s+/g, " ").trim() || null;

								const bedroomsText = $element.find(".prop-title").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing Stow Brothers listings on page ${i}:`, err);
					}
				}
			}

			// Agent - Remax
			if (agent_id == 32) {
				let page_no = 45; // Adjust as necessary

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://remax.co.uk/property-for-sale?page=${i}`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property-item").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title =
									$element.find(".c-gray2.fx-20.f-500").text().replace(/\s+/g, " ").trim() || null;

								const bedroomsText = $element.find(".c-gray2.fx-13.f-500").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".f-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(
									`Error processing Remax listing on page ${i}, index ${index}:`,
									innerErr
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching or parsing Remax listings on page ${i}:`, err);
					}
				}
			}

			// Agent - Kerr & Co
			if (agent_id == 74) {
				let page_no = 3; // Update as needed
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.kerrandco.com/properties/sales/tag-residential/status-available/page-${i}#/`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".property-card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.kerrandco.com" + $element.find("a").attr("href")
									: null;

								const title =
									$element.find(".property-card__description").text().replace(/\s+/g, " ").trim() ||
									null;

								const bedroomsText = $element.find(".property-card__summary").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".property-card__price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching Kerr & Co page ${i}:`, err);
					}
				}

				await browser.close();
			}

			// Agent - Patrick Gardner
			if (agent_id == 5) {
				let page_no = 20; // Update if needed

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.patrickgardner.com/property-search/page/${i}/?radius=1&availability%5B0%5D=2&availability%5B1%5D=6`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".work-box").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href") || null;

								const title =
									$element.find(".sub_title h5").text().replace(/\s+/g, " ").trim() || null;

								const bedroomsText = $element.find(".property-card__summary").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".sub_title h6").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error processing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching Patrick Gardner page ${i}:`, err);
					}
				}
			}

			// Agent - The Guild of Property Professionals
			if (agent_id == 35) {
				let page_no = 20; // Update as needed

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.guildproperty.co.uk/search?page=${i}&national=false&p_department=RS&p_division=&location=London&auto-lat=&auto-lng=&keywords=&minimumPrice=&minimumRent=&maximumPrice=&maximumRent=&rentFrequency=&minimumBedrooms=&maximumBedrooms=&searchRadius=50&recentlyAdded=&propertyIDs=&propertyType=&rentType=&orderBy=&networkID=&clientID=&officeID=&availability=1&propertyAge=&prestigeProperties=&includeDisplayAddress=Yes&videoettesOnly=0&360TourOnly=0&virtualTourOnly=0&country=&addressNumber=&equestrian=0&tag=&golfGroup=&coordinates=&priceAltered=&sfonly=0&openHouse=0&student=&isArea=false&limit=20`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".panel-body").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href") || null;

								const title =
									$element.find(".card-title a").text().replace(/\s+/g, " ").trim() || null;

								const bedroomsText = $element.find(".property-card__summary").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".h4.m-0").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error parsing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching Guild Property page ${i}:`, err);
					}
				}
			}

			// Agent - Frosts
			if (agent_id == 110) {
				let page_no = 20; // Set to actual page count if known

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.frosts.co.uk/properties-for-sale/all-properties/!/radius/40/page/${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".info_section").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.frosts.co.uk" + $element.find("a").attr("href")
									: null;

								const title = $element.find(".address").text().replace(/\s+/g, " ").trim() || null;

								const bedroomsText = $element.find(".blurb").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".sale_price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (innerErr) {
								console.error(`Error parsing listing on page ${i}, index ${index}:`, innerErr);
							}
						});
					} catch (err) {
						console.error(`Error fetching Frosts page ${i}:`, err);
					}
				}
			}

			// Agent - Reeds Rains
			if (agent_id == 33) {
				let page_no = 269; //269
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.reedsrains.co.uk/properties-for-sale/england/!/location/england~40/page/${i}`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);
						$(".property-thumbnail-container").each(async (index, element) => {
							try {
								const $element = $(element);

								let link = $element.find("a").attr("href") || null;
								if (link && !link.startsWith("http")) {
									link = "https://www.reedsrains.co.uk" + link;
								}

								const rawHtml = $element.find(".property-thumbnail__description").html();
								const titleMatch = rawHtml
									?.split("<br>")[1]
									?.trim()
									.match(/^(.+?),\s*([^,]+),\s*([A-Z]{1,2}\d{1,2})$/);
								const title = titleMatch ? titleMatch.slice(1).join(", ") : null;

								const bedroomsText = $element
									.find(".property-thumbnail__description")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".property-thumbnail__price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (listingErr) {
								console.error(`Error parsing listing on page ${i}, index ${index}:`, listingErr);
							}
						});
					} catch (pageErr) {
						console.error(`Error fetching Reeds Rains page ${i}:`, pageErr);
					}
				}
				await browser.close();
			}

			// Agent - Mark Coysh
			if (agent_id == 6) {
				let page_no = 5; //5

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.markcoysh.co.uk/search_results/page/${i}/?keyword&department=residential-sales`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".details").each(async (index, element) => {
							try {
								const $element = $(element);

								let link = $element.find("h3 a").attr("href") || null;
								if (link && !link.startsWith("http")) {
									link = "https://www.markcoysh.co.uk" + link;
								}

								const title = $element.find("h3 a").text().trim();

								const bedroomsText = $element
									.find(".property-thumbnail__description")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (listingErr) {
								console.error(`Error parsing listing on page ${i}, index ${index}:`, listingErr);
							}
						});
					} catch (pageErr) {
						console.error(`Error fetching Mark Coysh page ${i}:`, pageErr);
					}
				}
			}

			// Agent - Jackie Quinn
			if (agent_id == 8) {
				let page_no = 12; //12

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.jackiequinn.co.uk/search?category=1&listingtype=5&statusids=1%2C10%2C4%2C16%2C3&obc=Price&obd=Descending&page=${i}&perpage=9`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".propertyBox").each(async (index, element) => {
							try {
								const $element = $(element);

								let link = $element.find(".searchProName a").attr("href") || null;
								if (link && !link.startsWith("http")) {
									link = "https://www.jackiequinn.co.uk" + link;
								}

								const title = $element.find(".searchProName a").text().trim();

								const bedroomsText = $element
									.find(".property-thumbnail__description")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find("h3 div").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (listingErr) {
								console.error(`Error parsing listing on page ${i}, index ${index}:`, listingErr);
							}
						});
					} catch (pageErr) {
						console.error(`Error fetching Jackie Quinn page ${i}:`, pageErr);
					}
				}
			}

			// Agent - BELVOIR!
			if (agent_id == 107) {
				let page_no = 1; //152

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.belvoir.co.uk/properties/for-sale/?per_page=11&drawMap=&address=&address_lat_lng=&price_min=&price_max=&bedrooms_min=-1&hide_under_offer=on&yield_min=&yield_max=&pg=${i}`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".tease-property").each(async (index, element) => {
							try {
								const $element = $(element);

								let link = $element.find(".text-link").attr("href") || null;
								if (link && !link.startsWith("http")) {
									link = "https://www.belvoir.co.uk" + link;
								}

								const title = [$element.find(".addr1").text(), $element.find(".addr2").text()]
									.map((t) => t.replace(/\s+/g, " ").trim())
									.join(", ");

								const bedroomsText = $element
									.find(".bedroom-icon")
									.siblings(".counter")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".amount").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (listingErr) {
								console.error(`Error parsing listing on page ${i}, index ${index}:`, listingErr);
							}
						});
					} catch (pageErr) {
						console.error(`Error fetching BELVOIR! page ${i}:`, pageErr);
					}
				}
			}

			// Agent - Robert Holmes
			if (agent_id == 78) {
				let page_no = 8; //8

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://robertholmes.co.uk/search/page/${i}/?address_keyword&department=residential-sales&availability=2`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".grid-box-card").each(async (index, element) => {
							try {
								const $element = $(element);

								let link = $element.find("a").attr("href") || null;
								if (link && !link.startsWith("http")) {
									link = "https://robertholmes.co.uk" + link;
								}

								const title = $element.find(".property-archive-title h4").text().trim();

								const bedroomsText = $element.find(".icons-list").first("li span").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".property-archive-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (listingErr) {
								console.error(`Error parsing listing on page ${i}, index ${index}:`, listingErr);
							}
						});
					} catch (pageErr) {
						console.error(`Error fetching Robert Holmes page ${i}:`, pageErr);
					}
				}
			}

			// Agent - Warren
			if (agent_id == 83) {
				let page_no = 6; //6

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.warrenputney.co.uk/putney/sw15/0/25000000/0/buy/page-${i}`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".card").each(async (index, element) => {
							try {
								const $element = $(element);

								let link = $element.find("a").attr("href") || null;
								if (link && !link.startsWith("http")) {
									link = "https://www.warrenputney.co.uk" + link;
								}

								const title = $element.find(".card-title").text().trim();

								const bedroomsText = $element
									.find(".card-icons")
									.first(".card-icon span")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".card-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (listingErr) {
								console.error(`Error parsing listing on page ${i}, index ${index}:`, listingErr);
							}
						});
					} catch (pageErr) {
						console.error(`Error fetching Warren page ${i}:`, pageErr);
					}
				}
			}

			// Agent - Farar
			if (agent_id == 68) {
				let page_no = 7; // 7

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.farrar.co.uk/properties/page/${i}/?department=residential-sales&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;
								const title = $element.find(".propertyTitle a").text().trim();

								const bedroomsText = $element.find(".beds").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(
									`Error processing property on page ${i}, property index ${index}:`,
									error
								);
							}
						});
					} catch (error) {
						console.error(`Error fetching page ${i} of listings:`, error);
					}
				}
			}

			// Agent - Maskells
			if (agent_id == 86) {
				let page_no = 5; // 5

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.maskells.com/search/${i}.html?showstc=off&showsold=off&availability=For+Sale&instruction_type=Sale`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.maskells.com" + $element.find("a").attr("href")
									: null;

								const title = $element.find(".property-address").text().trim();

								const bedroomsText = $element.find(".property-type").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".property-price b").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(
									`Error processing property on page ${i}, property index ${index}:`,
									error
								);
							}
						});
					} catch (error) {
						console.error(`Error fetching page ${i} of listings:`, error);
					}
				}
			}

			// Agent - Martin & Co
			if (agent_id == 7) {
				let page_no = 257; // 257

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.martinco.com/search-results/for-sale/in-united-kingdom/page-${i}/?orderby=price_desc&department=residential`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property--card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".property-title a span").text().trim();

								const bedroomsText = $element.find(".property-type").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".property-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(
									`Error processing property on page ${i}, property index ${index}:`,
									error
								);
							}
						});
					} catch (error) {
						console.error(`Error fetching page ${i} of listings:`, error);
					}
				}
			}

			// Agent - Plaza Estates
			if (agent_id == 88) {
				let page_no = 8; // 8

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://plazaestates.co.uk/search/page/${i}/?address_keyword&radius=0&keyword&department=residential-sales&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&minimum_bathrooms&property_type&availability=2`;

						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".grid-box").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".property-archive-title h4").text().trim();

								const bedroomsText = $element.find(".icons-list").first("li span").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".property-archive-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(
									`Error processing property on page ${i}, property index ${index}:`,
									error
								);
							}
						});
					} catch (error) {
						console.error(`Error fetching page ${i} of listings:`, error);
					}
				}
			}

			// Agent - Strutt & Parker
			if (agent_id == 34) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 38; //38

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.struttandparker.com/properties/residential/buyingrenting?r%5Bloc%5D=England&r%5Bloc-lat%5D=52.52610664473&r%5Bloc-long%5D=-1.6827519056475&r%5Bloc-original%5D=England&r%5Bloc-type%5D=6&r%5Bloc-id%5D=56277&r%5Bfilter_template%5D=properties%2Fresidential%2Fbuyingrenting&r%5Bsr%5D=for-sale&r%5Bsold%5D=on&r%5Bfrhld%5D=on&r%5Blshld%5D=on&price-type=on&r%5Bprtype%5D=week&r%5Bfurnished%5D=on&r%5Bunfurnished%5D=on&r%5Bclass%5D=Residential%20sales&r%5Bsort_by%5D=property_price_min--desc&r%5Blist_page%5D=${i}&r[is_not_origin_home_page]=0`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });
						await page.waitForSelector(".grid-columns__item");

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".grid-columns__item").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".card__heading").text().trim();

								const bedroomsText = $element.find(".property-features__item--bed").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".card__price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(
									`Error processing property on page ${i}, property index ${index}:`,
									error
								);
							}
						});
					} catch (error) {
						console.error(`Error fetching page ${i} of listings:`, error);
					}
				}
				await browser.close();
			}

			// Agent - Greater London Properties
			if (agent_id == 89) {
				const listing_url = `https://www.greaterlondonproperties.co.uk/for-sale/?status=for-sale&bedrooms=any&min-price=any&max-price=any&exclude-under-offer=on`;

				try {
					const { data } = await axios.get(listing_url);
					const $ = cheerio.load(data);

					$(".publish").each(async (index, element) => {
						try {
							const $element = $(element);

							const link = $element.find("a").attr("href") ? $element.find("a").attr("href") : null;

							const title = $element.find(".h4").text().trim();

							const bedroomsText = $element
								.find(".flex--row.content-gap")
								.first("span")
								.text()
								.trim();
							const bedroomsMatch = bedroomsText.match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

							const priceText = $element.find(".sub-heading span").text().trim();
							const priceMatch = priceText.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							if (link && price && title && bedrooms) {
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							}
						} catch (error) {
							console.error(`Error processing property at index ${index}:`, error);
						}
					});
				} catch (error) {
					console.error("Error fetching the listings page:", error);
				}
			}

			// Agent - Horton and Garton
			if (agent_id == 92) {
				let page_no = 7; // 7

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.hortonandgarton.co.uk/findhome/page/${i}/?department=residential-sales&location=0`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".col-12.col-md-6").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find("a").text().trim();

								const bedroomsText = $element
									.find(".d-flex.pl-3.pr-4 h5")
									.first("span")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".col-12.mb-4 h5").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(`Error processing property at index ${index}:`, error);
							}
						});
					} catch (error) {
						console.error("Error fetching the listings page:", error);
					}
				}
			}

			// Agent - Coopers Residential
			if (agent_id == 93) {
				let page_no = 48; // 48

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.coopersresidential.co.uk/sales/property-search/?location=&radius=0&minPrice=0&maxPrice=1000000000&itemsPerPage=10&sortOrder=1&page=${i}`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".twelve.columns").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find(".darkBlue").attr("href")
									? "https://www.coopersresidential.co.uk" + $element.find("a").attr("href")
									: null;

								const title = $element.find("h3").text().trim();

								const bedroomsText = $element.find(".summary").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(`Error processing property at index ${index}:`, error);
							}
						});
					} catch (error) {
						console.error("Error fetching the listings page:", error);
					}
				}
			}

			// Agent - Aspire
			if (agent_id == 94) {
				let page_no = 6; // 6

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.aspire.co.uk/search/${i}.html?instruction_type=Sale&country=GB&address_keyword=&minprice=&bid=&maxprice=&property_type=`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.coopersresidential.co.uk" + $element.find("a").attr("href")
									: null;

								const title = $element.find("p").html().split("<br>")[1].trim();

								const bedroomsText = $element.find(".thumb-icons").text().trim().match(/\d+/)[0];
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find("strong").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(`Error processing property at index ${index}:`, error);
							}
						});
					} catch (error) {
						console.error("Error fetching the listings page:", error);
					}
				}
			}

			// Agent - Featherstone Leigh
			if (agent_id == 95) {
				let page_no = 7; // 7

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.featherstoneleigh.co.uk/properties-for-sale/?_availability=for-sale&_paged=${i}`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property_card").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".property_card__title").text().trim();

								const bedroomsText = $element
									.find(".brxe-text-basic")
									.text()
									.trim()
									.match(/\d+/)[0];
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".property_card__price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(`Error processing property at index ${index}:`, error);
							}
						});
					} catch (error) {
						console.error("Error fetching the listings page:", error);
					}
				}
			}

			// Agent - Cow & Co
			if (agent_id == 96) {
				let page_no = 5; // 5

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://cowandco-london.com/property-search/page/${i}/?orderby&instruction_type=sale&address_keyword&min_bedrooms&minprice&maxprice&property_type&showstc=off`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".property-grid").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? $element.find("a").attr("href")
									: null;

								const title = $element.find(".property-grid__meta h4").text().trim();

								const bedroomsText = $element
									.find(".property-grid__meta h5")
									.text()
									.trim()
									.match(/\d+/)[0];
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find("h6 span").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(`Error processing property at index ${index}:`, error);
							}
						});
					} catch (error) {
						console.error("Error fetching the listings page:", error);
					}
				}
			}

			// Agent - Hemmingfords
			if (agent_id == 97) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 3; //3

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://hemmingfords.co.uk/properties/for-sale/hide-completed/page/${i}`;
					await page.goto(listing_url, { waitUntil: "networkidle2" });
					await page.waitForSelector(".group.relative.z-10.flex", {
						timeout: 5000,
					});

					try {
						const html = await page.content();
						const $ = cheerio.load(html);

						$(".group.relative.z-10.flex").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://hemmingfords.co.uk" + $element.find("a").attr("href")
									: null;

								const title = $element.find("h2.mt-10").text().trim() || null;

								const bedroomsText = $element
									.find(".tracking-\\[0\\.28px\\]")
									.first()
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".mr-5.text-white").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title && bedrooms) {
									// console.log(link, price, title, bedrooms, agent_id);
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.log(error);
								console.error(`Error processing property at index ${index}:`, error);
							}
						});
					} catch (error) {
						console.error("Error fetching the listings page:", error);
					}
				}
				await browser.close();
			}

			// Agent - Alex Crown
			if (agent_id == 98) {
				const page_no = 2;
				const baseUrl = "https://www.alexcrown.co.uk";

				let browser = null;

				try {
					browser = await puppeteer.launch({
						headless: true,
						args: ["--no-sandbox", "--disable-setuid-sandbox"],
					});

					for (let i = 1; i <= page_no; i++) {
						const listing_url = `${baseUrl}/buy/property-for-sale?page=${i}`;
						console.log(`Fetching page: ${listing_url}`);

						const page = await browser.newPage();

						await page.setUserAgent(
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
						);

						await page.setDefaultNavigationTimeout(60000); // 60 seconds

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const htmlContent = await page.content();

						const $ = cheerio.load(htmlContent);

						$(".property").each(async (index, element) => {
							try {
								const $element = $(element);

								const relativeLink = $element.find(".property-description-link").attr("href");
								const link = relativeLink ? baseUrl + relativeLink : null;

								const title = $element.find(".grid-address").text().trim();

								const bedroomsText = $element
									.find(".FeaturedProperty__list-stats-item--bedrooms span")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? parseInt(bedroomsMatch[0], 10) : null;

								const priceText = $element.find(".nativecurrencyvalue").text().trim();
								const price = priceText ? parseFloat(priceText.replace(/[£,]/g, "")) : null;

								if (link && price !== null && title && bedrooms !== null) {
									await updatePriceByPropertyURL({
										link,
										title,
										bedrooms,
										price,
										agent_id,
										page: i,
									});
								} else {
								}
							} catch (error) {
								console.error(
									`Error processing property at index ${index} on page ${i}:`,
									error.message
								);
							}
						});

						await page.close();
						console.log(`Finished processing page ${i}`);
					}

					console.log("Scraping completed successfully.");
				} catch (error) {
					console.error("Error during Puppeteer scraping process:", error.message);
					if (error.name === "TimeoutError") {
						console.error(
							"Navigation timeout occurred. The page might be too slow to load or an issue with the network."
						);
					}
				} finally {
					if (browser) {
						await browser.close();
						console.log("Browser closed.");
					}
				}
			}

			// Agent - Abacus Estates
			if (agent_id == 100) {
				let page_no = 443;
				for (let i = 0; i <= page_no; i += 12) {
					const listing_url = `https://www.abacusestates.com/results?offset=${i}`;

					try {
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".results-grid-item").each(async (index, element) => {
							try {
								const $element = $(element);

								const link = $element.find("a").attr("href")
									? "https://www.abacusestates.com" + $element.find("a").attr("href")
									: null;

								const title = $element.find(".property-description h2").text().trim();

								const bedroomsText = $element.find(".brxe-text-basic").text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

								const priceText = $element.find(".results_priceask").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								if (link && price && title) {
									await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
								}
							} catch (error) {
								console.error(`Error processing property at index ${index}:`, error);
							}
						});
					} catch (error) {
						console.error("Error fetching the listings page:", error);
					}
				}
			}

			//Agent - Alan de Maid
			if (agent_id == 103) {
				try {
					let page_no = 35;
					for (let i = 1; i <= page_no; i++) {
						const listing_url = `https://www.alandemaid.co.uk/properties/sales/status-available/most-recent-first/page-${i}#/`;
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const browser = await puppeteer.launch();
						const page = await browser.newPage();

						await page.goto(listing_url, { waitUntil: "networkidle2" }); // change to your URL

						const content = await page.content();
						const $ = cheerio.load(content);

						$(".card").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://www.alandemaid.co.uk" + $element.find("a").attr("href")
									: null;
								const title = $element.find(".card__text-content").text().trim();
								const bedroomsText = $element
									.find(".card-content__spec-list-number")
									.first()
									.text()
									.trim();
								const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
								const priceText = $element.find(".card__heading").text().trim();
								const price = priceText.match(/£([\d,]+)/)?.[1] || null;
								console.log(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error("Alan de Maid listing error:", err.message);
							}
						});
						await browser.close();
					}
				} catch (err) {
					console.error("Alan de Maid fetch error:", err.message);
				}
			}

			//Agent - Aldermartin, Baines & Cuthbert
			if (agent_id == 104) {
				try {
					let page_no = 2;
					for (let i = 1; i <= page_no; i++) {
						const listing_url = `https://abcestates.co.uk/property-search/page/${i}/?department=residential-sales&address_keyword&radius=1`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".col-md-12.no-marg").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href") || null;
								const title = $element.find("h3").text().trim();
								const bedroomsText = $element.find(".beds_text").first().text().trim();
								const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
								const priceText = $element.find(".property_price").text().trim();
								const price = priceText.match(/£([\d,]+)/)?.[1] || null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error("ABC Estates listing error:", err.message);
							}
						});
					}
				} catch (err) {
					console.error("ABC Estates fetch error:", err.message);
				}
			}

			//Agent - Alex & Matteo
			if (agent_id == 105) {
				try {
					let page_no = 4;
					for (let i = 1; i <= page_no; i++) {
						const listing_url = `https://www.alex-matteo.com/properties-for-sale/page/${i}/?department=sales&max-price&bedrooms&exclude-sold=1`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".card").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.attr("href") || null;
								const title = $element.find(".card__title").text().trim();
								const bedroomsText = $element.find(".card__text").first("span").text().trim();
								const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
								const priceText = $element.find(".card__bottom__title").text().trim();
								const price = priceText.match(/£([\d,]+)/)?.[1] || null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error("Alex & Matteo listing error:", err.message);
							}
						});
					}
				} catch (err) {
					console.error("Alex & Matteo fetch error:", err.message);
				}
			}

			//Agent - Alex Neil Estate Agents
			if (agent_id == 106) {
				try {
					let page_no = 48;
					for (let i = 0; i <= page_no; i += 12) {
						const listing_url = `https://www.alexneil.com/results?offset=${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".results-grid-item").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find(".photo-contain a").attr("href")
									? "https://www.alexneil.com" + $element.find(".photo-contain a").attr("href")
									: null;
								const title = $element.find(".property-description h2").text().trim();
								const bedroomsText = $element
									.find(".property-attributes")
									.first("span")
									.text()
									.trim();
								const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
								const priceText = $element.find(".results_priceask").text().trim();
								const price = priceText.match(/£([\d,]+)/)?.[1] || null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error("Alex Neil listing error:", err.message);
							}
						});
					}
				} catch (err) {
					console.error("Alex Neil fetch error:", err.message);
				}
			}

			//Agent - CRAY & NORTON
			if (agent_id == 28) {
				try {
					let page_no = 60;
					for (let i = 0; i <= page_no; i += 12) {
						const listing_url = `https://www.crayandnorton.co.uk/results?market=1&ccode=UK&view=grid&pricetype=1&statustype=4&offset=${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".results-grid-item").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://www.crayandnorton.co.uk" + $element.find("a").attr("href")
									: null;
								const title = $element.find(".property-description h2").text().trim();
								const bedroomsText = $element
									.find(".property-attributes")
									.first("span")
									.text()
									.trim();
								const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
								const priceText = $element.find(".results_priceask").text().trim();
								const price = priceText.match(/£([\d,]+)/)?.[1] || null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error("CRAY & NORTON listing error:", err.message);
							}
						});
					}
				} catch (err) {
					console.error("CRAY & NORTON fetch error:", err.message);
				}
			}

			//Agent - LIVIN
			if (agent_id == 27) {
				try {
					let page_no = 2;
					for (let i = 1; i <= page_no; i++) {
						const listing_url = `https://livinestateagents.co.uk/property-for-sale/property/any-bed/all-location?exclude=1&page=${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".card").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find(".card-image-container").attr("href")
									? "https://livinestateagents.co.uk" +
									  $element.find(".card-image-container").attr("href")
									: null;
								const title = $element.find(".property__title").text().trim().split("in")[1];
								const bedroomsText = $element.find(".bedroom").first().text().trim();
								const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
								const priceText = $element.find(".property__price").text().trim();
								const price = priceText.match(/£([\d,]+)/)?.[1] || null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error("LIVIN listing error:", err.message);
							}
						});
					}
				} catch (err) {
					console.error("LIVIN fetch error:", err.message);
				}
			}

			//Agent - Haboodle
			if (agent_id == 24) {
				try {
					const listing_url = `https://www.haboodle.co.uk/find-a-property/?department=residential-sales&address_keyword=&radius=&minimum_bedrooms=&maximum_rent=&maximum_price=`;
					const { data } = await axios.get(listing_url);
					const $ = cheerio.load(data);

					$(".details").each(async (index, element) => {
						try {
							const $element = $(element);
							const link = $element.find("h3 a").attr("href") || null;
							const title = $element.find("h3 a").text().trim();
							const bedroomsText = $element.find(".room-bedrooms span").first().text().trim();
							const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
							const priceText = $element.find(".price").text().trim();
							const price = priceText.match(/£([\d,]+)/)?.[1] || null;
							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						} catch (err) {
							console.error("Haboodle listing error:", err.message);
						}
					});
				} catch (err) {
					console.error("Haboodle fetch error:", err.message);
				}
			}

			//Agent - Oaks Estate Agents
			if (agent_id == 23) {
				try {
					let page_no = 33;
					for (let i = 1; i <= page_no; i++) {
						const listing_url = `https://oaksestateagents.com/properties/page/${i}/?address_keyword&department=residential-sales&minimum_price&maximum_price&minimum_rent&maximum_rent&minimum_bedrooms&post_type=property`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);

						$(".type-property").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href") || null;
								const title = $element.find("h3 a").text().trim();
								const bedroomsText = $element.find(".room-count").first().text().trim();
								const bedrooms = bedroomsText.match(/\d+/)?.[0] || null;
								const priceText = $element.find(".price").text().trim();
								const price = priceText.match(/£([\d,]+)/)?.[1] || null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error("Oaks Estate listing error:", err.message);
							}
						});
					}
				} catch (err) {
					console.error("Oaks Estate fetch error:", err.message);
				}
			}

			//Agent - Allsop ( No url and price is in different formats)
			if (agent_id == 22) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 80; //80

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.allsop.co.uk/property-search?available_only=true&page=${i}&residential_property%5B%5D=All&residential_property%5B%5D=House&residential_property%5B%5D=Flat+%2F+Block&residential_property%5B%5D=Land&residential_property%5B%5D=Ground+Rent&residential_property%5B%5D=Garage+%2F+Parking&residential_property%5B%5D=Other&residential_property%5B%5D=Development`;

					await page.goto(listing_url, { waitUntil: "networkidle2" });

					const html = await page.content();
					const $ = cheerio.load(html);

					$(".__lot_container").each(async (index, element) => {
						const $element = $(element);

						const link = $element.find("a").attr("href") ? $element.find("a").attr("href") : null;

						const title = $element.find(".__location").text().trim();

						const bedroomsText = $element.find(".room-count").first().text().trim();
						const bedroomsMatch = bedroomsText.match(/\d+/);
						const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

						const priceText = $element.find(".__lot_price_grid").text().trim();
						const priceMatch = priceText.match(/£([\d,]+)/);
						const price = priceMatch ? priceMatch[1] : null;

						// console.log(link, title, bedrooms, price);

						await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
					});
				}

				await browser.close();
			}

			//Agent - Gibson Laine
			if (agent_id == 20) {
				let page_no = 2;
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.gibsonlane.co.uk/all-properties-for-sale?status=available&page=${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$(".property-listing").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find(".text-lg.text-primary").attr("href")
									? "https://www.gibsonlane.co.uk" +
									  $element.find(".text-lg.text-primary").attr("href")
									: null;
								const title = $element.find(".text-lg.text-primary").first().text().trim();
								const bedroomsText = $element.find(".inline-block.ml-2").first().text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceText = $element.find(".text-2xl.leading-loose").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (e) {
								console.error(`Error processing Gibson Laine listing on page ${i}:`, e.message);
							}
						});
					} catch (e) {
						console.error(`Error fetching Gibson Laine page ${i}:`, e.message);
					}
				}
			}

			//Agent - Snellers
			if (agent_id == 19) {
				let page_no = 1;
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.snellers.co.uk/properties/sales/status-available/page-${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$(".property-card-content").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://www.snellers.co.uk" + $element.find("a").attr("href")
									: null;
								const title = $element.find("a h1").text().trim();
								const bedroomsText = $element.find(".bed-baths li").first().text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceText = $element.find(".money").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (e) {
								console.error(`Error processing Snellers listing on page ${i}:`, e.message);
							}
						});
					} catch (e) {
						console.error(`Error fetching Snellers page ${i}:`, e.message);
					}
				}
			}

			//Agent - MOVELI
			if (agent_id == 18) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);
				try {
					const listing_url = `https://www.moveli.co.uk/properties?category=for-sale&searchKeywords=&status=For%20Sale&maxPrice=any&minBeds=any&sortOrder=price-desc`;
					await page.goto(listing_url, { waitUntil: "networkidle2" });
					await page.waitForSelector(".property-item", { timeout: 60000 });

					const html = await page.content();
					const $ = cheerio.load(html);
					$(".property-item").each(async (index, element) => {
						try {
							const $element = $(element);
							const link = $element.find("a").attr("href")
								? "https://www.moveli.co.uk" + $element.find("a").attr("href")
								: null;
							const title = $element.find(".property_label h4").text().trim();
							const bedroomsText = $element.find("div p").eq(3).text().trim();
							const bedroomsMatch = bedroomsText.match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
							const priceText = $element.find(".format_price").text().trim();
							const price = priceText ? priceText : null;
							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						} catch (e) {
							console.error("Error processing MOVELI listing:", e.message);
						}
					});
				} catch (e) {
					console.error("Error fetching MOVELI listings:", e.message);
				}
				await browser.close();
			}

			//Agent - Keller Williams
			if (agent_id == 17) {
				try {
					const listing_url = `https://www.kwuk.com/search/?department=residential-sales&residential_commercial=residential&address_keyword=&radius=&minimum_price=&maximum_price=&minimum_rent=&maximum_rent=&property_type=&minimum_bedrooms=&maximum_bedrooms=&include_new_homes=yes&include_sold_stc=no`;
					const { data } = await axios.get(listing_url);
					const $ = cheerio.load(data);
					$(".details").each(async (index, element) => {
						try {
							const $element = $(element);
							const link = $element.find("h3 a").first().attr("href") || null;
							const title = $element.find("h3 a").first().text().trim();
							const bedroomsText = $element.find(".strapline").first().text().trim();
							const bedroomsMatch = bedroomsText.match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
							const priceText = $element.find(".price").text().trim();
							const priceMatch = priceText.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;
							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						} catch (e) {
							console.error("Error processing Keller Williams listing:", e.message);
						}
					});
				} catch (e) {
					console.error("Error fetching Keller Williams listings:", e.message);
				}
			}

			//Agent - V&H Homes
			if (agent_id == 11) {
				let page_no = 1;
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://vhhomes.co.uk/search?type=buy&status=available&per-page=10&sort=price-high&status-ids=371%2C385%2C391%2C1394&page=${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$("._property-details-container").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find(".unstyled-link").attr("href") || null;
								const title = $element.find(".unstyled-link").text().trim();
								const bedroomsText = $element
									.find("._property-rooms-container")
									.first("span")
									.text()
									.trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceText = $element.find("._property-price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (e) {
								console.error(`Error processing V&H Homes listing on page ${i}:`, e.message);
							}
						});
					} catch (e) {
						console.error(`Error fetching V&H Homes page ${i}:`, e.message);
					}
				}
			}

			//Agent - The Personal Agent
			if (agent_id == 9) {
				let page_no = 4;
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://thepersonalagent.co.uk/page/${i}/?purpose-radios=SALE&s=`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$(".property-card").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.attr("href") || null;
								const title = $element.find("address").text().trim();
								const bedroomsText = $element.find(".facilities .item span").first().text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceText = $element.find(".price").text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (e) {
								console.error(
									`Error processing The Personal Agent listing on page ${i}:`,
									e.message
								);
							}
						});
					} catch (e) {
						console.error(`Error fetching The Personal Agent page ${i}:`, e.message);
					}
				}
			}

			//Agent - Cairds
			if (agent_id == 10) {
				let page_no = 11;
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.cairds.co.uk/properties/Sales/All/0-Beds/999999999/0/0/page/${i}/`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$(".col-lg-6.col-md-6").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://www.cairds.co.uk" + $element.find("a").attr("href")
									: null;
								const title = $element.find("a").text().trim();
								const bedroomsText = $element.find("strong").eq(1).text().trim();
								const bedroomsMatch = bedroomsText.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceText = $element.find("strong").first().text().trim();
								const priceMatch = priceText.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (e) {
								console.error(`Error processing Cairds listing on page ${i}:`, e.message);
							}
						});
					} catch (e) {
						console.error(`Error fetching Cairds page ${i}:`, e.message);
					}
				}
			}

			// Agent - Carter Jonas
			if (agent_id == 113) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 58; //58

				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://www.carterjonas.co.uk/property-search?division=Homes&radius=50&locationLat=51.5072174&locationLng=-0.1275862&northEastLat=51.6723442&northEastLng=0.148271039&southWestLat=51.38494&southWestLng=-0.351468325&searchTerm=London&toBuy=true&includeSoldOrSoldSTC=true&includeLetAgreedOrUnderOffer=true&freehold=true&leasehold=true&newHomes=true&sortOrder=HighestPriceFirst&page=${i}`;

					await page.goto(listing_url, { waitUntil: "networkidle2" });

					const html = await page.content();
					const $ = cheerio.load(html);

					$(".relative.z-0").each(async (index, element) => {
						const $element = $(element);

						const link = $element.find("h3 a").attr("href")
							? "https://www.carterjonas.co.uk" + $element.find("h3 a").attr("href")
							: null;

						const title = $element.find("h3 a").text().trim();

						const bedroomsText = $element.find(".text-plum li").first().text().trim();
						const bedroomsMatch = bedroomsText.match(/\d+/);
						const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;

						const priceText = $element.find("h4").first().text().trim();
						const priceMatch = priceText.match(/£([\d,]+)/);
						const price = priceMatch ? priceMatch[1] : null;

						await updatePriceByPropertyURL(link, title, bedrooms, price, agent_id);
					});
				}

				await browser.close();
			}

			// Agent - The Estate Agency
			if (agent_id == 111) {
				let page_no = 1;
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://theestate.agency/listings?viewType=gallery&sortby=dateListed-desc&saleOrRental=Sale&rental_period=week&status=available&page=${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$(".v2-flex.v2-flex-col.v2-items-start").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://theestate.agency" + $element.find("a").attr("href")
									: null;
								const title = $element.find("a h4").text().trim();
								const bedroomsMatch = $element
									.find(".v2-text-body-small")
									.text()
									.trim()
									.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceMatch = $element
									.find(".v2-text-body-bold")
									.first()
									.text()
									.trim()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error parsing The Estate Agency listing on page ${i}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching The Estate Agency page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Jackson-Stops
			if (agent_id == 114) {
				let page_no = 272;
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.jackson-stops.co.uk/properties/sales/page-${i}?page_size=12#grid`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$(".property-single__grid").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://jackson-stops.co.uk" + $element.find("a").attr("href")
									: null;
								const title = $element.find(".property-single__grid__address").text().trim();
								const bedroomsMatch = $element
									.find(".property-single__grid__rooms span")
									.eq(2)
									.text()
									.trim()
									.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceMatch = $element
									.find(".property-single__grid__price")
									.first()
									.text()
									.trim()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error parsing Jackson-Stops listing on page ${i}: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching Jackson-Stops page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Gascoigne-Pees
			if (agent_id == 116) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 49; //49

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.gpees.co.uk/properties/sales/status-available/most-recent-first/page-${i}#/`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);
						$(".card").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://gpees.co.uk" + $element.find("a").attr("href")
									: null;
								const title = $element.find(".card__text-content").text().trim();
								const bedroomsMatch = $element.find(".card__text-title").text().trim().match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceMatch = $element
									.find(".card__heading")
									.first()
									.text()
									.trim()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error parsing Gascoigne-Pees listing on page ${i}: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching Gascoigne-Pees page ${i}: ${err.message}`);
					}
				}
				await browser.close();
			}

			// Agent - Parkers
			if (agent_id == 117) {
				let page_no = 1;
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.parkersproperties.co.uk/search-results/for-sale/in-south-england/page-${i}/`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$(".property--card").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href") || null;
								const title = $element
									.find(".property-title span")
									.map((i, el) => $(el).text().trim())
									.get()
									.join(" ");
								const bedroomsMatch = $element.find(".property-type").text().trim().match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceMatch = $element
									.find(".property-price")
									.first()
									.text()
									.trim()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error parsing Parkers listing on page ${i}: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching Parkers page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - Mann & Co
			if (agent_id == 118) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 88; //88

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.manncountrywide.co.uk/properties/sales/status-available/most-recent-first/page-${i}#/`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);
						$(".card").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://theestate.agency" + $element.find("a").attr("href")
									: null;
								const title = $element.find(".card__text-content").text().trim();
								const bedroomsMatch = $element.find(".card__text-title").text().trim().match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceMatch = $element
									.find(".card__heading")
									.first()
									.text()
									.trim()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error parsing Mann & Co listing on page ${i}: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching Mann & Co page ${i}: ${err.message}`);
					}
				}
				await browser.close();
			}

			// Agent - Cubitt & West
			if (agent_id == 119) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 88; //88

				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.cubittandwest.co.uk/property/for-sale/in-billingshurst-west-sussex/radius-30-miles/page-${i}/`;

						await page.goto(listing_url, { waitUntil: "networkidle2" });

						const html = await page.content();
						const $ = cheerio.load(html);
						$(".about-properties").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").first().attr("href")
									? "https://theestate.agency" + $element.find("a").first().attr("href")
									: null;
								const title = $element.find("a").first().text().trim();
								const bedroomsMatch = $element
									.find(".icon-bedroom")
									.siblings("span")
									.text()
									.trim()
									.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceMatch = $element
									.find(".property-price")
									.first()
									.text()
									.trim()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;
								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error parsing Cubitt & West listing on page ${i}: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching Cubitt & West page ${i}: ${err.message}`);
					}
				}
				await browser.close();
			}

			// Agent - EweMove
			if (agent_id == 49) {
				let page_no = 126; //126
				for (let i = 1; i <= page_no; i++) {
					try {
						const listing_url = `https://www.ewemove.com/property/for-sale?page=${i}`;
						const { data } = await axios.get(listing_url);
						const $ = cheerio.load(data);
						$(".property-card").each(async (index, element) => {
							try {
								const $element = $(element);
								const link = $element.find("a").attr("href")
									? "https://ewemove.com" + $element.find("a").attr("href")
									: null;
								const title = $element.find(".mb-4.text-sm.font-myriad").text().trim();
								const bedroomsMatch = $element
									.find(".mt-2.text-sm span")
									.text()
									.trim()
									.match(/\d+/);
								const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
								const priceMatch = $element
									.find(".my-4.font-myriad.text-pink-100 > span")
									.text()
									.trim()
									.match(/£([\d,]+)/);
								const price = priceMatch ? priceMatch[1] : null;

								// console.log(link, price, title, bedrooms, agent_id);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(`Error parsing EweMove listing on page ${i}: ${err.message}`);
							}
						});
					} catch (err) {
						console.error(`Error fetching EweMove page ${i}: ${err.message}`);
					}
				}
			}

			// Agent - estateseast
			if (agent_id == 91) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();
				await page.goto(
					"https://www.estateseast.co.uk/search/?showstc=off&instruction_type=Sale&showsoldstc=off&showsold=off",
					{
						waitUntil: "networkidle2",
					}
				);

				const html = await page.content();
				const $ = cheerio.load(html);

				$(".property-listing").each(async (i, element) => {
					const $element = $(element);
					const link = $element.attr("href")
						? "https://estateseast.co.uk" + $element.attr("href")
						: null;
					const title = $element.find(".fw-bold.1h-sm").text().trim();
					const bedroomsMatch = $element.find(".mt-2.text-sm span").text().trim().match(/\d+/);
					const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
					const priceMatch = $element
						.find("span.fw-bold")
						.text()
						.trim()
						.match(/£([\d,]+)/);
					const price = priceMatch ? priceMatch[1] : null;

					// console.log(link, price, title, bedrooms, agent_id);
					await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
				});

				await browser.close();
			}

			// Agent - emoov
			if (agent_id == 112) {
				let loadMore = true;
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();
				await page.goto(
					"https://emoov.co.uk/find-a-property/any-location/any-price/all-types/any-bedrooms/fourty-miles",
					{
						waitUntil: "networkidle2",
					}
				);

				await page.waitForSelector(".row.mobile_ar_view.mb-3");
				let previousItemCount;

				while (loadMore) {
					try {
						await page.evaluate(() => {
							const lastItem = document.querySelector(".row.mobile_ar_view.mb-3:last-child");
							if (lastItem) {
								lastItem.scrollIntoView();
							}
						});

						await page
							.waitForSelector(".loading-container", {
								visible: true,
								timeout: 3000,
							})
							.catch(() => {
								loadMore = false;
							});

						if (!loadMore) break;

						await page.waitForSelector(".loading-container", {
							hidden: true,
							timeout: 10000,
						});

						const newItemCount = await page.evaluate(
							() => document.querySelectorAll(".row.mobile_ar_view.mb-3").length
						);

						if (newItemCount <= previousItemCount) {
							loadMore = false;
						}

						previousItemCount = newItemCount;

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".row.mobile_ar_view.mb-3").each(async (i, element) => {
							const $element = $(element);
							const link = $element.find("a").attr("href")
								? "https://emoov.co.uk" + $element.find("a").attr("href")
								: null;
							const title = $element.find(".colour_red").text().trim();
							const bedroomsMatch = $element
								.find(".light-text")
								.first("span")
								.text()
								.trim()
								.match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
							const priceMatch = $element
								.find(".property-price.price_size")
								.text()
								.trim()
								.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							// console.log(
							// 	`Link: ${link}, Price: £${price}, Title: ${title}, Bedrooms: ${bedrooms}, Agent ID: ${agent_id}`
							// );

							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						});

						await new Promise((r) => setTimeout(r, 300));
					} catch (e) {
						loadMore = false;
					}
				}

				await browser.close();
			}

			//Agent - Romans
			if (agent_id == 16) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();
				await page.goto(
					"https://www.romans.co.uk/properties-search-results?location_id=15654&search=London%2C+Greater+London&search_type=buy",
					{
						waitUntil: "networkidle2",
					}
				);

				const html = await page.content();
				const $ = cheerio.load(html);

				$(".search-results-item").each(async (i, element) => {
					const $element = $(element);
					const onclickAttr = $element.attr("onclick");
					const link = onclickAttr ? onclickAttr.match(/'([^']+)'/)[1] : null;
					const title = $element.find("b").text().trim();
					const bedroomsMatch = $element.find(".headline-description").text().trim().match(/\d+/);
					const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
					const priceMatch = $element
						.find(".search-results-item-property-price")
						.text()
						.trim()
						.match(/£([\d,]+)/);
					const price = priceMatch ? priceMatch[1] : null;

					// console.log(link, price, title, bedrooms, agent_id);
					await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
				});

				await browser.close();
			}

			//Agent - Nicolas Van Patrick
			// if (agent_id == 87) {
			// 	const browser = await puppeteer.launch({ headless: false });
			// 	const page = await browser.newPage();

			// 	await page.setUserAgent(
			// 		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
			// 	);

			// 	let page_no = 1; //4

			// 	for (let i = 1; i <= page_no; i++) {
			// 		await page.goto(`https://nicolasvanpatrick.com/sales/?option=Hide&pg=${i}`, {
			// 			waitUntil: 'networkidle2',
			// 		});

			// 		await page.waitForSelector('.cnt_frame');
			// 		const html = await page.content();
			// 		const $ = cheerio.load(html);

			// 		$('.cnt_frame').each(async (i, element) => {
			// 			const $element = $(element);
			// 			const link = $element.find('a').attr('href')
			// 				? 'https://nicolasvanpatrick.com' + $element.find('a').attr('href')
			// 				: null;
			// 			const title = $element.find('h2').text().trim();
			// 			const bedroomsMatch = $element.find('.group prty_items').eq(0).text().trim().match(/\d+/);
			// 			const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
			// 			const priceMatch = $element
			// 				.find('#price')
			// 				.text()
			// 				.trim()
			// 				.match(/£([\d,]+)/);
			// 			const price = priceMatch ? priceMatch[1] : null;

			// 			console.log(link, price, title, bedrooms, agent_id);
			// 			// await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
			// 		});
			// 	}
			// 	await browser.close();
			// }

			//Agent - Knight Frank

			if (agent_id == 2) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				await page.setUserAgent(
					"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
				);

				let page_no = 3858; //3858

				for (let i = 0; i <= page_no; i += 48) {
					await page.goto(
						`https://www.knightfrank.co.uk/properties/residential/for-sale/uk/all-types/all-beds;offset=${i};availability=available`,
						{
							waitUntil: "networkidle2",
						}
					);

					await page.waitForSelector(".properties-item");
					const html = await page.content();
					const $ = cheerio.load(html);

					$(".properties-item").each(async (i, element) => {
						const $element = $(element);
						const link = $element.find("a").attr("href")
							? "https://knightfrank.co.uk" + $element.find("a").attr("href")
							: null;
						const title = $element.find(".grid-address").text().trim();
						const bedroomsMatch = $element.find(".bed").text().trim().match(/\d+/);
						const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
						const priceMatch = $element
							.find(".ng-star-inserted")
							.text()
							.trim()
							.match(/£([\d,]+)/);
						const price = priceMatch ? priceMatch[1] : null;

						// console.log(link, price, title, bedrooms, agent_id);
						await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
					});
				}
				await browser.close();
			}

			// Agent - Marriott Vernon
			if (agent_id == 25) {
				let page_no = 4;
				const browser = await puppeteer.launch({ headless: false });
				const page = await browser.newPage();

				for (let i = 1; i <= page_no; i++) {
					await page.goto(
						`https://www.marriottvernon.com/search/${i}.html?showstc=off&instruction_type=Sale&address_keyword=&minprice=&maxprice=&property_type=`,
						{
							waitUntil: "networkidle2",
						}
					);

					await page.waitForSelector(".cards");

					try {
						const html = await page.content();
						const $ = cheerio.load(html);

						$(".cards").each(async (i, element) => {
							const $element = $(element);
							const link = $element.attr("href")
								? "https://marriottvernon.com" + $element.attr("href")
								: null;
							const title = $element.find(".cards__textbox h5").text().split("br")[0];
							const bedroomsMatch = $element.find(".cards__textbox h4").text().trim().match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
							const priceMatch = $element
								.find(".cards__textbox h5")
								.html()
								.replace(/<br\s*\/?>/gi, "\n")
								.split("\n")[1]
								.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							// console.log(
							// 	`Link: ${link}, Price: £${price}, Title: ${title}, Bedrooms: ${bedrooms}, Agent ID: ${agent_id}`
							// );

							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						});
					} catch (e) {
						console.log(e);
					}
				}

				await browser.close();
			}

			// Agent - Benham & Reeves
			if (agent_id == 43) {
				let loadMore = true;
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();
				await page.goto("https://www.benhams.com/property-for-sale/?search_type=buy&search%5B%5D", {
					waitUntil: "networkidle2",
				});

				await page.waitForSelector(".property-listing-discrpition");
				let previousItemCount;

				while (loadMore) {
					try {
						await page.evaluate(() => {
							const lastItem = document.querySelector(".property-listing-discrpition:last-child");
							if (lastItem) {
								lastItem.scrollIntoView();
							}
						});

						const buttonExists = await page.$(".loadmore");

						if (buttonExists) {
							await page.click(".loadmore");
						}

						const newItemCount = await page.evaluate(
							() => document.querySelectorAll(".property-listing-discrpition").length
						);

						if (newItemCount <= previousItemCount) {
							loadMore = false;
						}

						previousItemCount = newItemCount;

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".property-listing-discrpition").each(async (i, element) => {
							const $element = $(element);
							const link = $element.find("a").attr("href") ? $element.find("a").attr("href") : null;
							const title = $element.find("h2 a").text().trim();
							const bedroomsMatch = $element
								.find(".property-features ul")
								.first("li")
								.text()
								.trim()
								.match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
							const priceMatch = $element
								.find(".price")
								.text()
								.trim()
								.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							// console.log(
							// 	`Link: ${link}, Price: £${price}, Title: ${title}, Bedrooms: ${bedrooms}, Agent ID: ${agent_id}`
							// );

							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						});

						await new Promise((r) => setTimeout(r, 300));
					} catch (e) {
						loadMore = false;
					}
				}

				await browser.close();
			}

			// Agent - Abode
			if (agent_id == 85) {
				let loadMore = true;
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();
				await page.goto("https://www.abodeweb.co.uk/properties/sales", {
					waitUntil: "networkidle2",
				});

				await page.waitForSelector(".list-item-content");
				let previousItemCount;

				while (loadMore) {
					try {
						await page.evaluate(() => {
							const lastItem = document.querySelector(".list-item-content:last-child");
							if (lastItem) {
								lastItem.scrollIntoView();
							}
						});

						await page.waitForSelector(".load-more-button");
						await page.click(".load-more-button");

						await new Promise((r) => setTimeout(r, 300));

						const newItemCount = await page.evaluate(
							() => document.querySelectorAll(".list-item-content").length
						);

						if (newItemCount <= previousItemCount) {
							loadMore = false;
						}

						previousItemCount = newItemCount;

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".list-item-content").each(async (i, element) => {
							const $element = $(element);
							const link = $element.find("a").attr("href")
								? "https://www.abodeweb.co.uk" + $element.find("a").attr("href")
								: null;
							const title = $element.find(".property-title").text().trim();
							const bedroomsMatch = $element.find(".property-bedrooms").text().trim().match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
							const priceMatch = $element
								.find(".property-price")
								.text()
								.trim()
								.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							// console.log(`Link: ${link}, Price: £${price}, Title: ${title}, Bedrooms: ${bedrooms}, Agent ID: ${agent_id}`);

							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						});

						await new Promise((r) => setTimeout(r, 300));
					} catch (e) {
						loadMore = false;
					}
				}

				await browser.close();
			}

			// Agent - ROBINSON JACKSON
			if (agent_id == 31) {
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();
				await page.goto(
					"https://www.robinson-jackson.com/sales/properties-available-for-sale-in-london-and-kent",
					{
						waitUntil: "networkidle2",
					}
				);

				await page.waitForSelector(".property-for-sale");

				const total_items = 1039;
				let currentItems = 0;
				let previousItemCount = 0;
				let page_count = 0;

				while (currentItems < total_items) {
					try {
						// Scroll to the last item inside #scroll-loader
						await page.evaluate(() => {
							const container = document.querySelector("body");
							const footer = container?.querySelector("footer");
							if (footer) {
								const top = footer.getBoundingClientRect().top + window.scrollY;
								window.scrollTo({ top: top - 50, behavior: "smooth" });
							}
						});

						// Wait briefly to allow content to load or spinner to appear
						const spinnerAppeared = (await page.$(".ias-spinner")) !== null;

						if (spinnerAppeared) {
							try {
								await page.waitForSelector(".ias-spinner", { hidden: true, timeout: 5000 });
							} catch (err) {
								console.warn("Spinner did not disappear in time.");
							}
						} else {
							await page.waitForTimeout(1500); // fallback delay
						}

						// Count loaded .property items
						currentItems = await page.evaluate(() => {
							return document.querySelectorAll("#scroll-loader .property").length;
						});

						page_count++;
						console.log(`Page ${page_count} | Loaded Items: ${currentItems} / ${total_items}`);

						// Optional: Break if items stop loading (safety condition)
						if (currentItems <= previousItemCount) {
							console.warn("No new items loaded — exiting early.");
							break;
						}

						previousItemCount = currentItems;
					} catch (e) {
						loadMore = false;
					}
				}

				const html = await page.content();
				const $ = cheerio.load(html);
				$(".property-for-sale").each(async (i, element) => {
					const $element = $(element);
					const link = $element.find("a").attr("href")
						? "https://www.robinson-jackson.com" + $element.find("a").attr("href")
						: null;
					const title = $element.find(".property-content h4 span").text().trim();
					const bedroomsMatch = $element.find(".property-meta span").text().trim().match(/\d+/);
					const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
					const priceMatch = $element
						.find(".property-content h4")
						.text()
						.trim()
						.match(/£([\d,]+)/);
					const price = priceMatch ? priceMatch[1] : null;

					// console.log(`Link: ${link}, Price: £${price}, Title: ${title}, Bedrooms: ${bedrooms}, Agent ID: ${agent_id}`);

					await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
				});
				await browser.close();
			}

			// Agent - Your Move
			if (agent_id == 41) {
				let page_no = 347; //347
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();

				for (let i = 1; i <= page_no; i++) {
					await page.goto(`https://www.your-move.co.uk/properties-for-sale/england/!/page/${i}`, {
						waitUntil: "networkidle2",
					});

					await page.waitForSelector(".property-thumbnail-container");

					try {
						const html = await page.content();
						const $ = cheerio.load(html);

						$(".property-thumbnail-container").each(async (i, element) => {
							const $element = $(element);
							const link = $element.find(".property-thumbnail-footer__view-property").attr("href")
								? $element.find(".property-thumbnail-footer__view-property").attr("href")
								: null;
							const title = $element
								.find(".property-thumbnail__description")
								.html()
								.split("<br>")[1]
								.trim();
							const bedroomsMatch = $element
								.find(".property-thumbnail-icon--beds")
								.text()
								.trim()
								.match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
							const priceMatch = $element
								.find(".property-thumbnail__price")
								.text()
								.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							// console.log(`Link: ${link}, Price: £${price}, Title: ${title}, Bedrooms: ${bedrooms}, Agent ID: ${agent_id}`);

							await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						});
					} catch (e) {
						console.log(e);
					}
				}

				await browser.close();
			}

			//Agent - Leaders
			if (agent_id == 54) {
				const browser = await puppeteer.launch({ headless: false });
				const page = await browser.newPage();
				await page.goto(
					"https://www.leaders.co.uk/properties-search-results?location_id=15654&search=London%2C+Greater+London&search_type=buy",
					{
						waitUntil: "networkidle2",
					}
				);

				await page.waitForSelector(".search-results-item");

				const html = await page.content();
				const $ = cheerio.load(html);

				$(".search-results-item").each(async (i, element) => {
					const $element = $(element);
					const onclickAttr = $element.attr("onclick");
					const link = onclickAttr ? onclickAttr.match(/'([^']+)'/)[1] : null;
					const title = $element.find("b").text().trim();
					const bedroomsMatch = $element.find(".headline-description").text().trim().match(/\d+/);
					const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
					const priceMatch = $element
						.find(".search-results-item-property-price")
						.text()
						.trim()
						.match(/£([\d,]+)/);
					const price = priceMatch ? priceMatch[1] : null;

					// console.log(link, price, title, bedrooms, agent_id);
					await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
				});

				await browser.close();
			}

			// Agent - Streets Ahead
			if (agent_id == 81) {
				let loadMore = true;
				const browser = await puppeteer.launch({ headless: true });
				const page = await browser.newPage();
				await page.goto(
					"https://www.streetsahead.info/properties-for-sale/any-area/any-type/any-bedrooms/min-price-none/max-price-none/exclude-unavalable/desc/",
					{
						waitUntil: "networkidle2",
					}
				);

				await page.waitForSelector(".property");
				let previousItemCount;

				while (loadMore) {
					try {
						await page.evaluate(() => {
							const footer = document.querySelector("footer");
							if (footer) {
								const top = footer.getBoundingClientRect().top + window.scrollY;
								window.scrollTo({ top: top - 50, behavior: "smooth" });
							}
						});

						await new Promise((r) => setTimeout(r, 1000));

						try {
							await page.waitForSelector("#spinner", { visible: true, timeout: 5000 });
							await page.waitForSelector("#spinner", { hidden: true, timeout: 7000 });
						} catch (err) {
							console.warn("Spinner did not behave as expected.");
						}

						const newItemCount = await page.evaluate(
							() => document.querySelectorAll(".property").length
						);

						if (newItemCount <= previousItemCount) {
							loadMore = false;
						}

						previousItemCount = newItemCount;

						const html = await page.content();
						const $ = cheerio.load(html);

						$(".property").each(async (i, element) => {
							const $element = $(element);
							const link = $element.find("a").attr("href") ? $element.find("a").attr("href") : null;
							const title = $element.find(".title h3").text().trim();
							const bedroomsMatch = $element.find(".icons span").text().trim().match(/\d+/);
							const bedrooms = bedroomsMatch ? bedroomsMatch[0] : null;
							const priceMatch = $element
								.find(".price")
								.text()
								.trim()
								.match(/£([\d,]+)/);
							const price = priceMatch ? priceMatch[1] : null;

							console.log(
								`Link: ${link}, Price: £${price}, Title: ${title}, Bedrooms: ${bedrooms}, Agent ID: ${agent_id}`
							);

							// await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
						});

						await new Promise((r) => setTimeout(r, 300));
					} catch (e) {
						loadMore = false;
					}
				}

				await browser.close();
			}

			// Agent - Savils
			if (agent_id == 40) {
				let page_no = 444; //444
				for (let i = 1; i <= page_no; i++) {
					const listing_url = `https://search.savills.com/list?SearchList=Id_46920+Category_RegionCountyCountry&Tenure=GRS_T_B&SortOrder=SO_PCDD&Currency=GBP&ResidentialSizeUnit=SquareFeet&LandAreaUnit=Acre&SaleableAreaUnit=SquareMeter&Category=GRS_CAT_RES&Shapes=W10&CurrentPage=${i}`;

					try {
						const { data } = await axios.get(listing_url, {
							headers: {
								"User-Agent":
									"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
							},
						});

						const $ = cheerio.load(data);

						$(".sv-results-listing__item").each(async (index, element) => {
							try {
								const link =
									"https://search.savills.com" + $(element).find("a").first().attr("href");

								const matchText = $(element).find(".sv-details__price").first().text();
								const match_price = matchText.match(/£([\d,]+)/);
								const price = match_price ? match_price[1] : null;

								const title = $(element).find(".sv-details__address2").first().text();

								const bedrooms = $(element)
									.find(".sv--bedrooms")
									.first(".sv-property-attribute__value")
									.text()
									.trim();

								// Log the results or perform actions
								// console.log(link, title, bedrooms, price);

								await updatePriceByPropertyURL(link, price, title, bedrooms, agent_id);
							} catch (err) {
								console.error(
									`Error processing listing on page ${i}, index ${index}: ${err.message}`
								);
							}
						});
					} catch (err) {
						console.error(`Error fetching data for page ${i}: ${err.message}`);
					}
				}
			}

			// update remove status
			await updateRemoveStatus(agent_id);

			agentLog(agent_id, "Done all " + agent_id);
			// mark not running
			const finalState = ensureAgentState(agent_id);
			finalState.running = false;
			finalState.stopRequested = false;
		}
	} catch (error) {
		console.error("Server error:", error);
		res.status(500).json({ error: error.message });
	}
});

app.listen(PORT, () => {
	console.log(`Server running on http://localhost:${PORT}`);
});
