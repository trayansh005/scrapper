// Scraper Agent for Agent ID 285 (International Property Alerts)
// Handles XML feed parsing, category routing (Sale vs Rent), unique property titles, and geocoding fallbacks for missing lat/long.

'use strict';

const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env'), override: true });

const axios = require('axios');
const cheerio = require('cheerio');
const fs = require('fs');
const { promisePool } = require('./db.js');

const AGENT_ID = 285;

// In-memory location coordinates lookup to quickly populate missing lat/lng
const cityCoordsMap = new Map([
	["limassol, cyprus", { lat: 34.6786, lng: 33.0413 }],
	["alanya, turkey", { lat: 36.5437, lng: 31.9998 }],
	["united arab emirates", { lat: 25.2048, lng: 55.2708 }],
	["dubai, united arab emirates", { lat: 25.2048, lng: 55.2708 }],
	["paphos, cyprus", { lat: 34.7754, lng: 32.4245 }],
	["larnaca, cyprus", { lat: 34.9167, lng: 33.6292 }],
	["larnaka, cyprus", { lat: 34.9167, lng: 33.6292 }],
	["nicosia, cyprus", { lat: 35.1856, lng: 33.3823 }],
	["thalang district, thailand", { lat: 8.0315, lng: 98.3331 }],
	["sunny beach, bulgaria", { lat: 42.6937, lng: 27.7082 }],
	["mueang phuket district, thailand", { lat: 7.8804, lng: 98.3923 }],
	["phuket, thailand", { lat: 7.8804, lng: 98.3923 }],
	["sveti vlas, bulgaria", { lat: 42.7138, lng: 27.7601 }],
	["hurghada, egypt", { lat: 27.2579, lng: 33.8116 }],
	["yermasoyia, cyprus", { lat: 34.7214, lng: 33.0858 }],
	["bangkok, thailand", { lat: 13.7563, lng: 100.5018 }],
	["canggu, indonesia", { lat: -8.6478, lng: 115.1385 }],
	["strovolos, cyprus", { lat: 35.1481, lng: 33.3537 }],
	["novi vinodolski, croatia", { lat: 45.1281, lng: 14.7889 }],
	["povljana, croatia", { lat: 44.3467, lng: 15.1156 }],
	["crikvenica, croatia", { lat: 45.1736, lng: 14.6922 }],
	["tulum, mexico", { lat: 20.2114, lng: -87.4654 }],
	["kotor, montenegro", { lat: 42.4244, lng: 18.7712 }],
	["budva, montenegro", { lat: 42.2864, lng: 18.8400 }],
	["herceg novi, montenegro", { lat: 42.4531, lng: 18.5375 }],
	["kolasin, montenegro", { lat: 42.8222, lng: 19.5233 }],
	["labin, croatia", { lat: 45.0931, lng: 14.1200 }],
	["savudrija, croatia", { lat: 45.4975, lng: 13.5019 }],
	["zadar, croatia", { lat: 44.1194, lng: 15.2314 }],
	["milna, croatia", { lat: 43.3267, lng: 16.4447 }],
	["cyprus", { lat: 35.1264, lng: 33.4299 }],
	["turkey", { lat: 38.9637, lng: 35.2433 }],
	["croatia", { lat: 45.1, lng: 15.2 }],
	["montenegro", { lat: 42.7, lng: 19.3 }],
	["thailand", { lat: 15.87, lng: 100.99 }]
]);

function getKolkataTimestamp() {
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
	const parts = kolkataFormatter.formatToParts(new Date());
	const map = {};
	for (let i = 0; i < parts.length; i++) {
		map[parts[i].type] = parts[i].value;
	}
	return `${map.year}-${map.month}-${map.day} ${map.hour}:${map.minute}:${map.second}`;
}

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

	await mapConcurrent(updateItems, 15, async (item) => {
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

function resolveCoordinates(p, location, country) {
	let latitudeVal = (
		p.find("latitude").text() ||
		p.find("Address > latitude").text() ||
		p.find("lat").text() ||
		p.find("Latitude").text()
	).trim();
	let latitude = latitudeVal !== "" && !isNaN(parseFloat(latitudeVal)) ? parseFloat(latitudeVal) : null;

	let longitudeVal = (
		p.find("longitude").text() ||
		p.find("Address > longitude").text() ||
		p.find("lng").text() ||
		p.find("long").text() ||
		p.find("Longitude").text()
	).trim();
	let longitude = longitudeVal !== "" && !isNaN(parseFloat(longitudeVal)) ? parseFloat(longitudeVal) : null;

	// Geocode fallback for missing longitude or latitude
	if (longitude === null || latitude === null) {
		const key1 = `${location}, ${country}`.toLowerCase().trim();
		const key2 = `${location}`.toLowerCase().trim();
		const key3 = `${country}`.toLowerCase().trim();

		const fallback = cityCoordsMap.get(key1) || cityCoordsMap.get(key2) || cityCoordsMap.get(key3);
		if (fallback) {
			if (longitude === null) longitude = fallback.lng;
			if (latitude === null) latitude = fallback.lat;
		}
	}

	return { latitude, longitude };
}

async function runAgent285() {
	console.log(`========================================`);
	console.log(`Starting Scraper for Agent ID: ${AGENT_ID}`);
	console.log(`========================================`);

	// Fetch XML URL from Database for Agent 285
	let xmlUrl = "";
	try {
		const [rows] = await promisePool.query("SELECT rent_xml, sale_xml FROM agent WHERE agent_id = ?", [AGENT_ID]);
		if (rows.length > 0) {
			xmlUrl = (rows[0].rent_xml || rows[0].sale_xml || "").trim();
		}
	} catch (e) {
		console.error("DB Agent fetch error:", e.message);
	}

	let xmlData = "";
	const localXmlPath = path.join(__dirname, "7212_Default.xml");

	if (xmlUrl && /^https?:\/\//i.test(xmlUrl)) {
		console.log(`Fetching XML feed from: ${xmlUrl}`);
		try {
			const res = await axios.get(xmlUrl, {
				headers: {
					"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
					Accept: "application/xml, text/xml, */*",
				},
				timeout: 60000,
			});
			xmlData = res.data;
		} catch (err) {
			console.log(`HTTP fetch failed (${err.message}). Checking local fallback file...`);
		}
	}

	if (!xmlData && fs.existsSync(localXmlPath)) {
		console.log(`Reading local XML file: ${localXmlPath}`);
		xmlData = fs.readFileSync(localXmlPath, "utf8");
	}

	if (!xmlData) {
		console.error(`Error: Could not load XML data for agent ${AGENT_ID}`);
		process.exit(1);
	}

	const $ = cheerio.load(xmlData, { xmlMode: true });
	const propertiesList = $("Clients Client properties Property");
	const propertiesToProcess = propertiesList.length > 0 ? propertiesList : $("Property");

	console.log(`Found ${propertiesToProcess.length} properties in XML feed.`);

	// Pre-fetch existing IDs for Agent 285
	const existingSaleIds = new Set();
	const existingRentIds = new Set();
	try {
		const [saleRows] = await promisePool.query(
			"SELECT property_id FROM property_for_sale WHERE agent_id = ?",
			[AGENT_ID]
		);
		const [rentRows] = await promisePool.query(
			"SELECT property_id FROM property_for_rent WHERE agent_id = ?",
			[AGENT_ID]
		);
		saleRows.forEach((r) => existingSaleIds.add(String(r.property_id)));
		rentRows.forEach((r) => existingRentIds.add(String(r.property_id)));
	} catch (e) {
		console.error("DB pre-fetch error:", e.message);
	}

	let inserted = 0;
	let updated = 0;
	let skipped = 0;
	const nowString = getKolkataTimestamp();

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

	for (let i = 0; i < propertiesToProcess.length; i++) {
		const propertyNode = $(propertiesToProcess[i]);
		const property_id_raw = propertyNode.find("propertyid").text().trim();

		if (!property_id_raw) {
			skipped++;
			continue;
		}

		const category = propertyNode.find("category").text().trim();

		// CATEGORY ROUTING FIX:
		// "Residential For Sale" & "Commercial For Sale" both belong to property_for_sale
		const isSale = /sale/i.test(category) || !/rent|letting|to let/i.test(category);
		const property_table = isSale ? "property_for_sale" : "property_for_rent";

		// Address fields
		const street = propertyNode.find("Address > street").text().trim();
		const location = propertyNode.find("Address > location").text().trim();
		const region = propertyNode.find("Address > region").text().trim();
		const country = propertyNode.find("Address > country").text().trim();
		const addressFallback = [street, location, region].filter(Boolean).join(" ").trim();

		// TITLE / PROPERTY NAME SELECTION FIX:
		// Prioritize <title> to prevent identical address fallback names like "Alanya Antalya"
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

		// PRICE PARSING
		const priceVal = propertyNode.find("Price > price").text().trim();
		const raw_price = priceVal ? priceVal.replace(/[^\d.]/g, "") : null;
		const price =
			raw_price !== null && raw_price !== ""
				? Math.round(parseFloat(raw_price))
						.toString()
						.replace(/\B(?=(\d{3})+(?!\d))/g, ",")
				: null;

		// CURRENCY PARSING
		const rawCurrency = (
			propertyNode.find("Price > currency").text().trim() ||
			propertyNode.find("currency").text().trim()
		).toUpperCase();

		let currency = "£";
		if (rawCurrency) {
			if (rawCurrency === "THA" || rawCurrency === "THB" || rawCurrency === "฿") currency = "฿";
			else if (rawCurrency === "EUR" || rawCurrency === "€") currency = "€";
			else if (rawCurrency === "USD" || rawCurrency === "AUD" || rawCurrency === "$") currency = "$";
			else if (rawCurrency === "GBP" || rawCurrency === "£") currency = "£";
			else currency = rawCurrency;
		}

		// BEDROOMS
		const bedroomsVal = propertyNode.find("Description > bedrooms").text().trim();
		const bedrooms = bedroomsVal !== "" ? parseInt(bedroomsVal, 10) : null;

		// LATITUDE & LONGITUDE FIX
		const { latitude, longitude } = resolveCoordinates(propertyNode, location, country);

		// URL & META
		const dataSourceVal = propertyNode.find("link > dataSource").text().trim();
		const linkVal = propertyNode.find("link").text().trim();
		const data_source = dataSourceVal || linkVal || null;

		const logo = "property_for_sale/logo.png";
		const remove_status = 0;

		xmlPropertyIdsByTable[property_table][property_id_raw] = true;
		const existingSet = property_table === "property_for_sale" ? existingSaleIds : existingRentIds;

		const itemData = {
			property_id_raw,
			agent_id: String(AGENT_ID),
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
			toInsertByTable[property_table].push([
				property_id_raw,
				String(AGENT_ID),
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

	// Execute Inserts & Updates
	for (const table of ["property_for_sale", "property_for_rent"]) {
		try {
			const numInserted = await bulkInsertProperties(table, toInsertByTable[table]);
			inserted += numInserted;

			const numUpdated = await bulkUpdateProperties(table, toUpdateByTable[table]);
			updated += numUpdated;
		} catch (err) {
			console.error(`Batch DB error in ${table}: ${err.message}`);
		}
	}

	// Clean up stale rows & misplaced rent rows for Agent 285
	for (const table of ["property_for_sale", "property_for_rent"]) {
		const ids = Object.keys(xmlPropertyIdsByTable[table]);
		try {
			if (ids.length > 0) {
				const placeholders = ids.map(() => "?").join(",");
				const deleteSql = `DELETE FROM \`${table}\` WHERE agent_id = ? AND property_id NOT IN (${placeholders})`;
				const [res] = await promisePool.query(deleteSql, [String(AGENT_ID), ...ids]);
				console.log(`Deleted ${res.affectedRows || 0} stale rows from ${table}`);
			} else {
				const deleteSql = `DELETE FROM \`${table}\` WHERE agent_id = ?`;
				const [res] = await promisePool.query(deleteSql, [String(AGENT_ID)]);
				console.log(`Cleared all ${res.affectedRows || 0} rows from ${table}`);
			}
		} catch (err) {
			console.error(`Cleanup error in ${table}: ${err.message}`);
		}
	}

	console.log(`========================================`);
	console.log(`Agent ID ${AGENT_ID} Summary:`);
	console.log(`Total Inserted: ${inserted}`);
	console.log(`Total Updated: ${updated}`);
	console.log(`Total Skipped: ${skipped}`);
	console.log(`========================================`);

	await promisePool.end();
}

if (require.main === module) {
	runAgent285()
		.then(() => {
			console.log("Scraper agent 285 complete!");
			process.exit(0);
		})
		.catch((err) => {
			console.error("Fatal error running agent 285:", err);
			process.exit(1);
		});
}

module.exports = { runAgent285 };
