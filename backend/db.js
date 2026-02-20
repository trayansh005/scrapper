const mysql = require("mysql2"); // Use mysql2 for better performance

// Create a connection pool
const pool = mysql.createPool({
	host: process.env.DB_HOST || "localhost",
	user: "root",
	password: process.env.PASSWORD || "",
	database: "scrape",
	waitForConnections: true,
	connectionLimit: 10, // Limits active connections to 10
	queueLimit: 0,
});

console.log("Database pool created and ready for connections");

// Export a promise-based pool for async/await usage
const promisePool = pool.promise();

const { formatPriceUk } = require("./lib/property-helpers.js");

// Update or insert property by URL (check then update, else create)
async function updatePriceByPropertyURL(
	link,
	price,
	title,
	bedrooms,
	agent_id,
	is_rent = false,
	latitude = null,
	longitude = null,
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
				// UPDATE existing property for THIS agent
				const [result] = await promisePool.query(
					`UPDATE ${tableName}
                    SET price = ?, latitude = ?, longitude = ?, updated_at = NOW()
                    WHERE property_url = ? AND agent_id = ?`,
					[price, latitude, longitude, linkTrimmed, agent_id],
				);

				if (result.affectedRows > 0) {
					console.log(
						`✅ Updated: ${linkTrimmed.substring(
							0,
							50,
						)}... | Price: £${formatPriceUk(price)} | Coords: ${latitude}, ${longitude}`,
					);
				} else {
					console.log(`⚠️ No update: ${linkTrimmed.substring(0, 50)}...`);
				}
			} else {
				// INSERT new property
				const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

				const logo = "property_for_sale/logo.png"; // static logo
				const currentTime = new Date();

				await promisePool.query(insertQuery, [
					title,
					agent_id,
					price,
					bedrooms,
					linkTrimmed,
					logo,
					latitude,
					longitude,
					currentTime,
					currentTime,
				]);

				console.log(
					`✅ Created: ${linkTrimmed.substring(
						0,
						50,
					)}... | Price: £${formatPriceUk(price)} | Coords: ${latitude}, ${longitude}`,
				);
			}
		}
	} catch (error) {
		console.error(`❌ Error updating property: ${error.message}`);
		throw error;
	}
}

// Update remove status for old or future-dated properties
async function updateRemoveStatus(agent_id) {
	try {
		const remove_status = 1;
		const params = [remove_status, agent_id];

		// Flag records that are stale or have clearly bad future timestamps
		const [saleResult] = await promisePool.query(
			`UPDATE property_for_sale
             SET remove_status = ?
             WHERE agent_id = ?
             AND (updated_at < NOW() - INTERVAL 1 DAY OR updated_at > NOW() + INTERVAL 1 DAY)`,
			params,
		);

		const [rentResult] = await promisePool.query(
			`UPDATE property_for_rent
             SET remove_status = ?
             WHERE agent_id = ?
             AND (updated_at < NOW() - INTERVAL 1 DAY OR updated_at > NOW() + INTERVAL 1 DAY)`,
			params,
		);

		const removedCount = (saleResult?.affectedRows || 0) + (rentResult?.affectedRows || 0);
		console.log(
			`🧹 Removed old or future-dated properties for agent ${agent_id} (sale: ${
				saleResult?.affectedRows || 0
			}, rent: ${rentResult?.affectedRows || 0}, total: ${removedCount})`,
		);
	} catch (error) {
		console.error("Error updating remove status:", error.message);
	}
}

module.exports = { promisePool, updatePriceByPropertyURL, updateRemoveStatus };
// Deprecated: backward compatibility if code uses pool.promise() directly
module.exports.promise = () => promisePool;
