// Database helper functions for property updates
const { promisePool, updatePriceByPropertyURL } = require("../db.js");
const { formatPriceUk } = require("./property-helpers.js");
const DB_VERBOSE_LOGS = process.env.DB_VERBOSE_LOGS === "1";

/**
 * Optimized update function - only updates price for existing properties
 * @param {string} link - Property URL
 * @param {number} price - Property price
 * @param {string} title - Property title
 * @param {string} bedrooms - Number of bedrooms
 * @param {number} agent_id - Agent ID
 * @param {boolean} is_rent - Whether it's a rental property
 * @returns {Object} - Object with isExisting, updated, and optional error
 */
async function updatePriceByPropertyURLOptimized(
	link,
	price,
	title,
	bedrooms,
	agent_id,
	is_rent = false,
	currency = "£",
) {
	let attempts = 0;
	const maxRetries = 3;
	while (attempts < maxRetries) {
		try {
			if (link) {
				let tableName = "property_for_sale";
				if (is_rent) {
					tableName = "property_for_rent";
				}

				const linkTrimmed = link.trim();
				const formattedPrice = price === "POA" ? "POA" : (formatPriceUk(price) || "POA");
				const truncatedTitle = title ? title.substring(0, 150) : "";

				// Check if property exists for THIS agent and get current data
				const [propertiesUrlRows] = await promisePool.query(
					`SELECT price, latitude, longitude, bedrooms FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
					[linkTrimmed, agent_id],
				);

				if (propertiesUrlRows.length > 0) {
					const row = propertiesUrlRows[0];
					const currentPrice = row.price;
					const hasCoords = row.latitude !== null && row.longitude !== null;
					const hasBedrooms = row.bedrooms !== null && row.bedrooms !== 0;

					// UPDATE existing property - always update updated_at, but only log if price changed
					const [result] = await promisePool.query(
						`UPDATE ${tableName}
                        SET property_name = ?, price = ?, currency = ?, remove_status = 0, updated_at = NOW()
                        WHERE property_url = ? AND agent_id = ?`,
						[truncatedTitle, formattedPrice, currency, linkTrimmed, agent_id],
					);

					if (currentPrice !== formattedPrice && DB_VERBOSE_LOGS) {
						console.log(
							`✅ Updated price: ${linkTrimmed.substring(0, 50)}... | Old: ${currency}${currentPrice} -> New: ${currency}${formattedPrice}`,
						);
					}
					return { 
						isExisting: true, 
						updated: currentPrice !== formattedPrice,
						missingData: !hasCoords || !hasBedrooms
					};
				} else {
					// For new properties, we'll need coordinates
					return { isExisting: false, updated: false, missingData: true };
				}
			}
			return { isExisting: false, updated: false };
		} catch (error) {
			attempts++;
			if (error.code === 'ECONNREFUSED' && attempts < maxRetries) {
				console.warn(`⚠️ DB Connection refused, retrying (${attempts}/${maxRetries})...`);
				await new Promise(r => setTimeout(r, 2000 * attempts));
				continue;
			}
			console.error(`❌ Error checking property ${link}:`, error.message || error);
			// Don't throw - return error state instead to prevent crawler from failing
			return { isExisting: false, updated: false, error: error.message || String(error) };
		}
	}
}

/**
 * Process property with coordinates from detail page
 * @param {string} url - Property URL
 * @param {number} price - Property price
 * @param {string} title - Property title
 * @param {string} bedrooms - Number of bedrooms
 * @param {number} agentId - Agent ID
 * @param {boolean} isRent - Whether it's a rental
 * @param {string} html - HTML content to extract coordinates from
 * @param {number} manualLat - Optional manual latitude
 * @param {number} manualLon - Optional manual longitude
 * @param {string} currency - Currency symbol (default '£')
 */
async function processPropertyWithCoordinates(
	url,
	price,
	title,
	bedrooms,
	agentId,
	isRent,
	html,
	manualLat = null,
	manualLon = null,
	currency = "£",
) {
	const { extractCoordinatesFromHTML, extractBedroomsFromHTML } = require("./property-helpers.js");

	try {
		let latitude = manualLat;
		let longitude = manualLon;
		let finalBedrooms = bedrooms;

		// If no manual coords, extract from HTML
		if (latitude === null || longitude === null) {
			if (process.env.DEBUG_COORDS === "1") {
				console.log(
					`[DB] ℹ️ Extracting coordinates from HTML (manual lat=${manualLat}, lng=${manualLon})`,
				);
			}
			const coords = await extractCoordinatesFromHTML(html);
			latitude = coords.latitude;
			longitude = coords.longitude;
			if (process.env.DEBUG_COORDS === "1") {
				console.log(`[DB] ℹ️ Extraction result: lat=${latitude}, lng=${longitude}`);
			}
		}

		// If no bedrooms, try to extract from HTML
		if (finalBedrooms === null || finalBedrooms === undefined || finalBedrooms === "") {
			const textForBedrooms = typeof html === 'string' ? html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ') : '';
			finalBedrooms = extractBedroomsFromHTML(textForBedrooms);
		}

		const formattedPrice = price === "POA" ? "POA" : (formatPriceUk(price) || "POA");
		// Truncate title to 150 characters to prevent database column size errors
		const truncatedTitle = title ? title.substring(0, 150) : "";

		let attempts = 0;
		const maxRetries = 3;
		while (attempts < maxRetries) {
			try {
				await updatePriceByPropertyURL(
					url,
					formattedPrice,
					truncatedTitle,
					finalBedrooms,
					agentId,
					isRent,
					latitude,
					longitude,
					currency,
				);
				break;
			} catch (error) {
				attempts++;
				if (error.code === 'ECONNREFUSED' && attempts < maxRetries) {
					console.warn(`⚠️ DB Connection refused during insert, retrying (${attempts}/${maxRetries})...`);
					await new Promise(r => setTimeout(r, 2000 * attempts));
					continue;
				}
				throw error;
			}
		}

		if (DB_VERBOSE_LOGS) {
			console.log(
				`✅ New property: ${title} (${currency}${formattedPrice}) - Coords: ${latitude}, ${longitude}${
					finalBedrooms ? `, Beds: ${finalBedrooms}` : ""
				}`,
			);
		}

		return { latitude, longitude };
	} catch (error) {
		console.error(`❌ Failed ${url}:`, error.message);
		return { latitude: null, longitude: null };
	}
}

module.exports = {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
};
