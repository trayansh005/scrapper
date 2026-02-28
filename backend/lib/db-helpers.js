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
) {
	try {
		if (link) {
			let tableName = "property_for_sale";
			if (is_rent) {
				tableName = "property_for_rent";
			}

			const linkTrimmed = link.trim();
			const formattedPrice = formatPriceUk(price);

			// Check if property exists for THIS agent and get current price
			const [propertiesUrlRows] = await promisePool.query(
				`SELECT price FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
				[linkTrimmed, agent_id],
			);

			if (propertiesUrlRows.length > 0) {
				const currentPrice = propertiesUrlRows[0].price;

				// UPDATE existing property - always update updated_at, but only log if price changed
				const [result] = await promisePool.query(
					`UPDATE ${tableName}
                    SET price = ?, remove_status = 0, updated_at = NOW()
                    WHERE property_url = ? AND agent_id = ?`,
					[formattedPrice, linkTrimmed, agent_id],
				);

				if (currentPrice !== formattedPrice && DB_VERBOSE_LOGS) {
					console.log(
						`✅ Updated price: ${linkTrimmed.substring(0, 50)}... | Old: £${currentPrice} -> New: £${formattedPrice}`,
					);
				}
				return { isExisting: true, updated: currentPrice !== formattedPrice };
			} else {
				// For new properties, we'll need coordinates
				return { isExisting: false, updated: false };
			}
		}
		return { isExisting: false, updated: false };
	} catch (error) {
		console.error(`❌ Error checking property ${link}:`, error.message || error);
		console.error(`Full error:`, error);
		// Don't throw - return error state instead to prevent crawler from failing
		return { isExisting: false, updated: false, error: error.message || String(error) };
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
) {
	const { extractCoordinatesFromHTML, extractBedroomsFromHTML } = require("./property-helpers.js");

	try {
		let latitude = manualLat;
		let longitude = manualLon;
		let finalBedrooms = bedrooms;

		// If no manual coords, extract from HTML
		if (latitude === null || longitude === null) {
			const coords = await extractCoordinatesFromHTML(html);
			latitude = coords.latitude;
			longitude = coords.longitude;
		}

		// If no bedrooms, try to extract from HTML
		if (finalBedrooms === null || finalBedrooms === undefined || finalBedrooms === "") {
			finalBedrooms = extractBedroomsFromHTML(html);
		}

		const formattedPrice = formatPriceUk(price);

		await updatePriceByPropertyURL(
			url,
			formattedPrice,
			title,
			finalBedrooms,
			agentId,
			isRent,
			latitude,
			longitude,
		);

		if (DB_VERBOSE_LOGS) {
			console.log(
				`✅ New property: ${title} (£${formattedPrice}) - Coords: ${latitude}, ${longitude}${
					finalBedrooms ? `, Beds: ${finalBedrooms}` : ""
				}`,
			);
		}
	} catch (error) {
		console.error(`❌ Failed ${url}:`, error.message);
		// Don't throw - just log the error
	}
}

module.exports = {
	formatPriceUk,
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
};
