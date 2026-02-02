// Database helper functions for property updates
const { promisePool, updatePriceByPropertyURL } = require("../db.js");

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
				// UPDATE existing property - only price
				const [result] = await promisePool.query(
					`UPDATE ${tableName}
                    SET price = ?, updated_at = NOW()
                    WHERE property_url = ? AND agent_id = ?`,
					[price, linkTrimmed, agent_id],
				);

				if (result.affectedRows > 0) {
					console.log(`✅ Updated price: ${linkTrimmed.substring(0, 50)}... | Price: £${price}`);
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
 */
async function processPropertyWithCoordinates(url, price, title, bedrooms, agentId, isRent, html) {
	const { extractCoordinatesFromHTML } = require("./property-helpers.js");

	try {
		const coords = await extractCoordinatesFromHTML(html);

		await updatePriceByPropertyURL(
			url,
			price,
			title,
			bedrooms,
			agentId,
			isRent,
			coords.latitude,
			coords.longitude,
		);

		console.log(
			`✅ New property: ${title} (£${price}) - Coords: ${coords.latitude}, ${coords.longitude}`,
		);
	} catch (error) {
		console.error(`❌ Failed ${url}:`, error.message);
		// Don't throw - just log the error
	}
}

module.exports = {
	updatePriceByPropertyURLOptimized,
	processPropertyWithCoordinates,
};
