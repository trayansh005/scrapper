const { promisePool } = require("./db.js");

async function updateRentLinks() {
	try {
		// Update property_for_rent table for agent 54
		// Replace 'properties-to-rent' with 'properties-for-rent' in property_url
		const [result] = await promisePool.query(
			`UPDATE property_for_rent SET property_url = REPLACE(property_url, 'properties-to-rent', 'properties-for-rent') WHERE agent_id = 54`
		);
		console.log(`Updated ${result.affectedRows} rows in property_for_rent for agent 54`);
	} catch (error) {
		console.error("Error updating links:", error.message);
	} finally {
		process.exit(0);
	}
}

updateRentLinks();
