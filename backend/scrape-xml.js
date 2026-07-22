// Node.js equivalent for fetch_properties.php (Refactored & Optimized)
// Usage: node backend/scrape-xml.js

const axios = require("axios");
const cheerio = require("cheerio");
const { promisePool } = require("./db.js");

// Cache global Intl.DateTimeFormat to avoid heavy re-instantiation inside loops
const kolkataFormatter = new Intl.DateTimeFormat("en-US", {
	timeZone: "Asia/Kolkata",
	year: "numeric",
	month: "2-digit",
	day: "2-digit",
	hour: "2-digit",
	minute: "2-digit",
	second: "2-digit",
	hour12: false,
});

function getKolkataTimestamp() {
	const parts = kolkataFormatter.formatToParts(new Date());
	const map = {};
	for (let i = 0; i < parts.length; i++) {
		map[parts[i].type] = parts[i].value;
	}
	return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`;
}

/**
 * Concurrency helper for async tasks
 */
async function mapConcurrent(items, concurrency, fn) {
	const results = [];
	let index = 0;
	const workers = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
		while (index < items.length) {
			const i = index++;
			results[i] = await fn(items[i], i);
		}
	});
	await Promise.all(workers);
	return results;
}

/**
 * Batch insert records into property_for_sale or property_for_rent
 */
async function bulkInsertProperties(table, rows) {
	if (!rows || rows.length === 0) return 0;
	const insertSql = `INSERT INTO \`${table}\` (
		property_id,
		agent_id,
		property_name,
		price,
		currency,
		bedrooms,
		latitude,
		longitude,
		property_url,
		logo,
		remove_status,
		created_at,
		updated_at
	) VALUES ?`;

	const chunkSize = 200;
	for (let i = 0; i < rows.length; i += chunkSize) {
		const chunk = rows.slice(i, i + chunkSize);
		await promisePool.query(insertSql, [chunk]);
	}
	return rows.length;
}

/**
 * Bulk update records concurrently
 */
async function bulkUpdateProperties(table, updateItems) {
	if (!updateItems || updateItems.length === 0) return 0;
	const updateSql = `UPDATE \`${table}\` SET
		property_name = ?,
		price = ?,
		currency = ?,
		bedrooms = ?,
		latitude = ?,
		longitude = ?,
		property_url = ?,
		logo = ?,
		remove_status = ?,
		updated_at = ?
		WHERE agent_id = ? AND property_id = ?`;

	await mapConcurrent(updateItems, 10, async (item) => {
		await promisePool.query(updateSql, [
			item.property_name,
			item.price,
			item.currency,
			item.bedrooms,
			item.latitude,
			item.longitude,
			item.data_source,
			item.logo,
			item.remove_status,
			item.nowString,
			item.agent_id,
			item.property_id_raw,
		]);
	});
	return updateItems.length;
}

/**
 * Processes a single agent row
 */
async function processAgent(row) {
	const agent_id_raw = String(row.agent_id);
	const agent_id = agent_id_raw;
	const xml_url = (row.rent_xml || "").trim();

	console.log("----------------------------------------");
	console.log(`Processing agent_id: ${agent_id_raw}`);

	if (!/^https?:\/\/.+/i.test(xml_url)) {
		console.log(`Invalid URL format for agent_id: ${agent_id_raw} -> ${xml_url}`);
		return;
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
		return;
	}

	if (!response.data) {
		console.log(`Failed to load XML for agent_id: ${agent_id_raw} - Empty body`);
		return;
	}

	let $;
	try {
		$ = cheerio.load(response.data, { xmlMode: true });
	} catch (parseErr) {
		console.log(`Failed to load XML for agent_id: ${agent_id_raw} - Parse error: ${parseErr.message}`);
		return;
	}

	// Check if <properties> node structure exists
	const propertiesList = $("Clients Client properties Property");
	const hasPropertyNode = propertiesList.length > 0 || $("Property").length > 0;

	if (!hasPropertyNode) {
		console.log(`No properties found in XML for agent_id: ${agent_id_raw}`);
		console.log(`Completed agent_id: ${agent_id_raw}`);
		console.log(`Inserted total: 0`);
		console.log(`Updated total: 0`);
		console.log(`Deleted from property_for_sale: 0`);
		console.log(`Deleted from property_for_rent: 0`);
		console.log(`Skipped/Error properties: 0`);
		return;
	}

	// Fetch existing property IDs for this agent from DB upfront to avoid N+1 queries
	let existingSaleIds = new Set();
	let existingRentIds = new Set();
	try {
		const [saleRows] = await promisePool.query(
			"SELECT property_id FROM property_for_sale WHERE agent_id = ?",
			[agent_id],
		);
		const [rentRows] = await promisePool.query(
			"SELECT property_id FROM property_for_rent WHERE agent_id = ?",
			[agent_id],
		);
		saleRows.forEach((r) => existingSaleIds.add(String(r.property_id)));
		rentRows.forEach((r) => existingRentIds.add(String(r.property_id)));
	} catch (dbQueryErr) {
		console.error(`Failed pre-fetching existing IDs for agent_id ${agent_id_raw}: ${dbQueryErr.message}`);
	}

	let inserted = 0;
	let updated = 0;
	let deletedSale = 0;
	let deletedRent = 0;
	let skipped = 0;

	const xmlPropertyIdsByTable = {
		property_for_sale: {},
		property_for_rent: {},
	};

	const toInsertByTable = {
		property_for_sale: [],
		property_for_rent: [],
	};

	const toUpdateByTable = {
		property_for_sale: [],
		property_for_rent: [],
	};

	const propertiesToProcess = propertiesList.length > 0 ? propertiesList : $("Property");
	const nowString = getKolkataTimestamp();

	for (let i = 0; i < propertiesToProcess.length; i++) {
		const propertyNode = $(propertiesToProcess[i]);

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

		// Currency parsing (default to €; handles THA/THB, EUR, USD, GBP)
		const rawCurrency = (
			propertyNode.find("Price > currency").text().trim() ||
			propertyNode.find("currency").text().trim()
		).toUpperCase();

		let currency = "€";
		if (rawCurrency) {
			if (rawCurrency === "THA" || rawCurrency === "THB" || rawCurrency === "฿") {
				currency = "฿";
			} else if (rawCurrency === "EUR" || rawCurrency === "€") {
				currency = "€";
			} else if (rawCurrency === "USD" || rawCurrency === "AUD" || rawCurrency === "$") {
				currency = "$";
			} else if (rawCurrency === "GBP" || rawCurrency === "£") {
				currency = "£";
			} else {
				currency = rawCurrency;
			}
		}

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

		const existingSet = property_table === "property_for_sale" ? existingSaleIds : existingRentIds;

		const itemData = {
			property_id_raw,
			agent_id,
			property_name,
			price,
			currency,
			bedrooms,
			latitude,
			longitude,
			data_source,
			logo,
			remove_status,
			nowString,
		};

		if (existingSet.has(property_id_raw)) {
			toUpdateByTable[property_table].push(itemData);
		} else {
			// For bulk insert: match column order [property_id, agent_id, property_name, price, currency, bedrooms, latitude, longitude, property_url, logo, remove_status, created_at, updated_at]
			toInsertByTable[property_table].push([
				property_id_raw,
				agent_id,
				property_name,
				price,
				currency,
				bedrooms,
				latitude,
				longitude,
				data_source,
				logo,
				remove_status,
				nowString,
				nowString,
			]);
		}
	}

	// Perform batch inserts and concurrent updates per table
	for (const table of ["property_for_sale", "property_for_rent"]) {
		try {
			const numInserted = await bulkInsertProperties(table, toInsertByTable[table]);
			inserted += numInserted;

			const numUpdated = await bulkUpdateProperties(table, toUpdateByTable[table]);
			updated += numUpdated;
		} catch (batchErr) {
			console.error(`Batch DB Error in ${table} for agent_id ${agent_id_raw}: ${batchErr.message}`);
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
		// Process agents with a concurrency limit of 3 to speed up network fetching without overloading DB
		const AGENT_CONCURRENCY = 3;
		await mapConcurrent(agentsRows, AGENT_CONCURRENCY, processAgent);
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

