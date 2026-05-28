// Node.js equivalent for fetch_properties.php
// Usage: node backend/scrape-xml.js

const axios = require("axios");
const cheerio = require("cheerio");
const { promisePool } = require("./db.js");

// Timezone configuration equivalent to date_default_timezone_set('Asia/Kolkata')
function getKolkataTimestamp() {
	const d = new Date();
	const formatter = new Intl.DateTimeFormat("en-US", {
		timeZone: "Asia/Kolkata",
		year: "numeric",
		month: "2-digit",
		day: "2-digit",
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
		hour12: false,
	});
	const parts = formatter.formatToParts(d);
	const year = parts.find((p) => p.type === "year").value;
	const month = parts.find((p) => p.type === "month").value;
	const day = parts.find((p) => p.type === "day").value;
	const hour = parts.find((p) => p.type === "hour").value;
	const minute = parts.find((p) => p.type === "minute").value;
	const second = parts.find((p) => p.type === "second").value;
	return `${year}-${month}-${day} ${hour}:${minute}:${second}`;
}

async function scrapeXml() {
	let agentsRows;
	try {
		const query = "SELECT agent_id, rent_xml FROM agent WHERE rent_xml IS NOT NULL AND rent_xml <> ''";
		const [rows] = await promisePool.query(query);
		agentsRows = rows;
	} catch (err) {
		console.error("Agent Query Error: " + err.message);
		process.exit(1);
	}

	if (agentsRows.length > 0) {
		for (const row of agentsRows) {
			const agent_id_raw = String(row.agent_id);
			const agent_id = agent_id_raw; // In mysql2 prepared statements/parameters, raw values are clean and safe.
			const xml_url = (row.rent_xml || "").trim();

			console.log("----------------------------------------");
			console.log(`Processing agent_id: ${agent_id_raw}`);

			if (!/^https?:\/\/.+/i.test(xml_url)) {
				console.log(`Invalid URL format for agent_id: ${agent_id_raw} -> ${xml_url}`);
				continue;
			}

			let response;
			try {
				response = await axios.get(xml_url, {
					headers: {
						Accept: "application/xml, text/xml, */*",
						"User-Agent":
							"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
					},
					timeout: 45000,
				});
			} catch (fetchErr) {
				console.log(`Failed to load XML for agent_id: ${agent_id_raw} - Error: ${fetchErr.message}`);
				continue;
			}

			if (!response.data) {
				console.log(`Failed to load XML for agent_id: ${agent_id_raw} - Empty body`);
				continue;
			}

			let $;
			try {
				$ = cheerio.load(response.data, { xmlMode: true });
			} catch (parseErr) {
				console.log(`Failed to load XML for agent_id: ${agent_id_raw} - Parse error: ${parseErr.message}`);
				continue;
			}

			let inserted = 0;
			let updated = 0;
			let deletedSale = 0;
			let deletedRent = 0;
			let skipped = 0;

			// Track XML property IDs separately by destination table.
			const xmlPropertyIdsByTable = {
				property_for_sale: {},
				property_for_rent: {},
			};

			// Check if <properties> node structure exists
			const propertiesList = $("Clients Client properties Property");
			const hasPropertyNode = propertiesList.length > 0 || $("Property").length > 0;

			if (!hasPropertyNode) {
				console.log(`No properties found in XML for agent_id: ${agent_id_raw}`);
				console.log(`Completed agent_id: ${agent_id_raw}`);
				console.log(`Inserted total: ${inserted}`);
				console.log(`Updated total: ${updated}`);
				console.log(`Deleted from property_for_sale: ${deletedSale}`);
				console.log(`Deleted from property_for_rent: ${deletedRent}`);
				console.log(`Skipped/Error properties: ${skipped}`);
				continue; // Important: do not delete anything when XML has no properties node.
			}

			// Iterate over all Property elements
			const propertiesToProcess = propertiesList.length > 0 ? propertiesList : $("Property");
			for (let i = 0; i < propertiesToProcess.length; i++) {
				const propertyNode = $(propertiesToProcess[i]);
				const nowString = getKolkataTimestamp();

				// property_id from XML <propertyid>
				const property_id_raw = propertyNode.find("propertyid").text().trim();

				if (property_id_raw === "") {
					console.log("Skipped: property_id empty");
					skipped++;
					continue;
				}

				const property_category = propertyNode.find("category").text().trim();

				// Address data
				const street = propertyNode.find("Address > street").text().trim();
				const location = propertyNode.find("Address > location").text().trim();
				const region = propertyNode.find("Address > region").text().trim();

				// property_name fallback logic
				let property_name_raw = [street, location, region].filter(Boolean).join(" ").trim();

				if (property_name_raw === "") {
					const titleVal = propertyNode.find("title").text().trim();
					const headlineVal = propertyNode.find("headline").text().trim();
					const marketingHeadingVal = propertyNode.find("marketingHeading").text().trim();
					const descHeadlineVal = propertyNode.find("Description > headline").text().trim();
					const descDescriptionVal = propertyNode.find("Description > description").text().trim();

					if (titleVal !== "") {
						property_name_raw = titleVal;
					} else if (headlineVal !== "") {
						property_name_raw = headlineVal;
					} else if (marketingHeadingVal !== "") {
						property_name_raw = marketingHeadingVal;
					} else if (descHeadlineVal !== "") {
						property_name_raw = descHeadlineVal;
					} else if (descDescriptionVal !== "") {
						property_name_raw = descDescriptionVal.substring(0, 150);
					} else {
						property_name_raw = "Property ID " + property_id_raw;
					}
				}

				const property_name = property_name_raw;

				// Price parsing
				const priceVal = propertyNode.find("Price > price").text().trim();
				const raw_price = priceVal ? priceVal.replace(/[^\d.]/g, "") : null;
				const price =
					raw_price !== null && raw_price !== ""
						? Math.round(parseFloat(raw_price))
								.toString()
								.replace(/\B(?=(\d{3})+(?!\d))/g, ",")
						: null;

				// Bedrooms parsing
				const bedroomsVal = propertyNode.find("Description > bedrooms").text().trim();
				const bedrooms = bedroomsVal !== "" ? parseInt(bedroomsVal, 10) : null;

				// Latitude parsing
				const latitudeVal = propertyNode.find("Address > latitude").text().trim();
				const latitude = latitudeVal !== "" ? parseFloat(latitudeVal) : null;

				// Longitude parsing
				const longitudeVal = propertyNode.find("Address > longitude").text().trim();
				const longitude = longitudeVal !== "" ? parseFloat(longitudeVal) : null;

				// Data source URL parsing
				const dataSourceVal = propertyNode.find("link > dataSource").text().trim();
				const data_source = dataSourceVal !== "" ? dataSourceVal : null;

				const logo = "property_for_sale/logo.png";
				const remove_status = 0;

				// Decide table based on category
				const property_table =
					property_category === "For Sale" || property_category === "Residential For Sale"
						? "property_for_sale"
						: "property_for_rent";

				// Track XML IDs for end-of-agent cleanup
				xmlPropertyIdsByTable[property_table][property_id_raw] = true;

				try {
					// Check if this property already exists for same agent_id + property_id
					const check_sql = `SELECT id FROM \`${property_table}\` WHERE agent_id = ? AND property_id = ? LIMIT 1`;
					const [check_result] = await promisePool.query(check_sql, [agent_id, property_id_raw]);

					if (check_result.length > 0) {
						// Update existing row
						const update_sql = `UPDATE \`${property_table}\` SET
							property_name = ?,
							price = ?,
							bedrooms = ?,
							latitude = ?,
							longitude = ?,
							property_url = ?,
							logo = ?,
							remove_status = ?,
							updated_at = ?
							WHERE agent_id = ? AND property_id = ?`;

						await promisePool.query(update_sql, [
							property_name,
							price,
							bedrooms,
							latitude,
							longitude,
							data_source,
							logo,
							remove_status,
							nowString,
							agent_id,
							property_id_raw,
						]);
						updated++;
					} else {
						// Insert new row
						const insert_sql = `INSERT INTO \`${property_table}\` (
							property_id,
							agent_id,
							property_name,
							price,
							bedrooms,
							latitude,
							longitude,
							property_url,
							logo,
							remove_status,
							created_at,
							updated_at
						) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

						await promisePool.query(insert_sql, [
							property_id_raw,
							agent_id,
							property_name,
							price,
							bedrooms,
							latitude,
							longitude,
							data_source,
							logo,
							remove_status,
							nowString,
							nowString,
						]);
						inserted++;
					}
				} catch (dbErr) {
					console.error(`Database Error in ${property_table}: ${dbErr.message}`);
					skipped++;
				}
			}

			// Delete stale rows not present in today's XML (per table, after processing).
			for (const table of ["property_for_sale", "property_for_rent"]) {
				const ids = Object.keys(xmlPropertyIdsByTable[table]);

				try {
					let delete_result;
					if (ids.length > 0) {
						const placeholders = ids.map(() => "?").join(",");
						const delete_sql = `DELETE FROM \`${table}\`
										   WHERE agent_id = ?
										   AND property_id NOT IN (${placeholders})`;
						const [result] = await promisePool.query(delete_sql, [agent_id, ...ids]);
						delete_result = result;
					} else {
						// XML had properties, but none mapped to this table -> remove stale rows from this table only.
						const delete_sql = `DELETE FROM \`${table}\` WHERE agent_id = ?`;
						const [result] = await promisePool.query(delete_sql, [agent_id]);
						delete_result = result;
					}

					if (table === "property_for_sale") {
						deletedSale = delete_result.affectedRows || 0;
					} else {
						deletedRent = delete_result.affectedRows || 0;
					}
				} catch (deleteErr) {
					console.error(`Delete Error in ${table}: ${deleteErr.message}`);
				}
			}

			console.log(`Completed agent_id: ${agent_id_raw}`);
			console.log(`Inserted total: ${inserted}`);
			console.log(`Updated total: ${updated}`);
			console.log(`Deleted from property_for_sale: ${deletedSale}`);
			console.log(`Deleted from property_for_rent: ${deletedRent}`);
			console.log(`Skipped/Error properties: ${skipped}`);
		}
	} else {
		console.log("No agents found with valid rent_xml.");
	}

	await promisePool.end();
}

if (require.main === module) {
	scrapeXml()
		.then(() => {
			console.log("All done!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("Fatal error:", err.message);
			process.exit(1);
		});
}

module.exports = { scrapeXml };
