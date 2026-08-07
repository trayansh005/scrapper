// Node.js equivalent for fetch_properties.php (Refactored & Optimized)
// Usage: node backend/scrape-xml.js [agent_id]

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
	const seenCoords282 = new Set();

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
		const country = propertyNode.find("Address > country").text().trim();
		const addressFallback = [street, location, region].filter(Boolean).join(" ").trim();

		// property_name priority logic (<title> first to prevent generic address duplicates)
		const titleVal = propertyNode.find("title").text().trim();
		const headlineVal = propertyNode.find("headline").text().trim();
		const marketingHeadingVal = propertyNode.find("marketingHeading").text().trim();
		const descHeadlineVal = propertyNode.find("Description > headline").text().trim();
		const descDescriptionVal = propertyNode.find("Description > description").text().trim();

		let property_name = "";
		if (titleVal !== "") {
			property_name = titleVal;
		} else if (headlineVal !== "") {
			property_name = headlineVal;
		} else if (marketingHeadingVal !== "") {
			property_name = marketingHeadingVal;
		} else if (descHeadlineVal !== "") {
			property_name = descHeadlineVal;
		} else if (addressFallback !== "") {
			property_name = addressFallback;
		} else if (descDescriptionVal !== "") {
			property_name = descDescriptionVal.substring(0, 150);
		} else {
			property_name = "Property ID " + property_id_raw;
		}

		// Price parsing
		const priceVal = propertyNode.find("Price > price").text().trim();
		const raw_price = priceVal ? priceVal.replace(/[^\d.]/g, "") : null;
		const price =
			raw_price !== null && raw_price !== ""
				? Math.round(parseFloat(raw_price))
						.toString()
						.replace(/\B(?=(\d{3})+(?!\d))/g, ",")
				: null;

		// Currency parsing (default to £; handles THA/THB, EUR, USD, GBP)
		const rawCurrency = (
			propertyNode.find("Price > currency").text().trim() ||
			propertyNode.find("currency").text().trim()
		).toUpperCase();

		let currency = "£";
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
		const latitudeVal = (
			propertyNode.find("latitude").text() ||
			propertyNode.find("Address > latitude").text() ||
			propertyNode.find("lat").text() ||
			propertyNode.find("Latitude").text()
		).trim();
		let latitude = latitudeVal !== "" && !isNaN(parseFloat(latitudeVal)) ? parseFloat(latitudeVal) : null;

		// Longitude parsing
		const longitudeVal = (
			propertyNode.find("longitude").text() ||
			propertyNode.find("Address > longitude").text() ||
			propertyNode.find("lng").text() ||
			propertyNode.find("long").text() ||
			propertyNode.find("Longitude").text()
		).trim();
		let longitude = longitudeVal !== "" && !isNaN(parseFloat(longitudeVal)) ? parseFloat(longitudeVal) : null;

		// Location lookup fallback for missing coordinates
		if (longitude === null || latitude === null) {
			const cityCoordsMap = {
				"limassol, cyprus": { lat: 34.6786, lng: 33.0413 },
				"alanya, turkey": { lat: 36.5437, lng: 31.9998 },
				"united arab emirates": { lat: 25.2048, lng: 55.2708 },
				"dubai, united arab emirates": { lat: 25.2048, lng: 55.2708 },
				"paphos, cyprus": { lat: 34.7754, lng: 32.4245 },
				"larnaca, cyprus": { lat: 34.9167, lng: 33.6292 },
				"larnaka, cyprus": { lat: 34.9167, lng: 33.6292 },
				"nicosia, cyprus": { lat: 35.1856, lng: 33.3823 },
				"novi vinodolski, croatia": { lat: 45.1281, lng: 14.7889 },
				"povljana, croatia": { lat: 44.3467, lng: 15.1156 },
				"crikvenica, croatia": { lat: 45.1736, lng: 14.6922 },
				"tulum, mexico": { lat: 20.2114, lng: -87.4654 },
				"kotor, montenegro": { lat: 42.4244, lng: 18.7712 },
				"budva, montenegro": { lat: 42.2864, lng: 18.8400 }
			};
			const key = `${location}, ${country}`.toLowerCase().trim();
			const fallback = cityCoordsMap[key] || cityCoordsMap[location.toLowerCase().trim()] || cityCoordsMap[country.toLowerCase().trim()];
			if (fallback) {
				if (longitude === null) longitude = fallback.lng;
				if (latitude === null) latitude = fallback.lat;
			}
		}

		// Deduplicate by latitude & longitude for agent 282
		if (agent_id_raw === "282" && latitude !== null && longitude !== null) {
			const coordKey = `${latitude},${longitude}`;
			if (seenCoords282.has(coordKey)) {
				console.log(`Skipped duplicate lat/long for agent 282: ${coordKey} (property_id: ${property_id_raw})`);
				skipped++;
				continue;
			}
			seenCoords282.add(coordKey);
		}

		// Data source URL parsing
		const dataSourceVal = propertyNode.find("link > dataSource").text().trim();
		const linkVal = propertyNode.find("link").text().trim();
		const data_source = dataSourceVal !== "" ? dataSourceVal : (linkVal !== "" ? linkVal : null);

		const logo = "property_for_sale/logo.png";
		const remove_status = 0;

		// Decide table based on category (check if category contains "sale" or does not contain "rent/let")
		const isSaleCategory = /sale/i.test(property_category) || !/rent|letting|to let/i.test(property_category);
		const property_table = isSaleCategory ? "property_for_sale" : "property_for_rent";

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

async function scrapeXml(targetAgentId = null) {
	let agentsRows;
	try {
		let query = "SELECT agent_id, rent_xml FROM agent WHERE rent_xml IS NOT NULL AND rent_xml <> ''";
		const queryParams = [];
		if (targetAgentId) {
			query += " AND agent_id = ?";
			queryParams.push(targetAgentId);
		}
		const [rows] = await promisePool.query(query, queryParams);
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
		console.log(`No agents found with valid rent_xml${targetAgentId ? ` for agent_id: ${targetAgentId}` : ""}.`);
	}

	await promisePool.end();
}

if (require.main === module) {
	const targetAgentId = process.argv[2] ? process.argv[2].trim() : null;
	scrapeXml(targetAgentId)
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

