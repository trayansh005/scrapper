// Database helper functions for property updates
const { promisePool, updatePriceByPropertyURL } = require("../db.js");

function formatPriceUk(value) {
	if (value === null || value === undefined) return null;
	const digits = value.toString().replace(/[^0-9]/g, "");
	if (!digits) return null;

	return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

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

			// Check if property exists for THIS agent
			const [propertiesUrlRows] = await promisePool.query(
				`SELECT COUNT(*) as count FROM ${tableName} WHERE property_url = ? AND agent_id = ?`,
				[linkTrimmed, agent_id],
			);

			if (propertiesUrlRows[0].count > 0) {
				const formattedPrice = formatPriceUk(price);
				// UPDATE existing property - only price
				const [result] = await promisePool.query(
					`UPDATE ${tableName}
                    SET price = ?, updated_at = NOW()
                    WHERE property_url = ? AND agent_id = ?`,
					[formattedPrice, linkTrimmed, agent_id],
				);

				if (result.affectedRows > 0) {
					console.log(
						`✅ Updated price: ${linkTrimmed.substring(0, 50)}... | Price: £${formattedPrice}`,
					);
				}
				return { isExisting: true, updated: result.affectedRows > 0 };
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
	const { extractCoordinatesFromHTML } = require("./property-helpers.js");

	try {
		let latitude = manualLat;
		let longitude = manualLon;

		// If no manual coords, extract from HTML
		if (latitude === null || longitude === null) {
			const coords = await extractCoordinatesFromHTML(html);
			latitude = coords.latitude;
			longitude = coords.longitude;
		}

		await updatePriceByPropertyURL(
			url,
			formatPriceUk(price),
			title,
			bedrooms,
			agentId,
			isRent,
			latitude,
			longitude,
		);

		console.log(
			`✅ New property: ${title} (£${formatPriceUk(price)}) - Coords: ${latitude}, ${longitude}`,
		);
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
