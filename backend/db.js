const mysql = require("mysql2"); // Use mysql2 for better performance
require("dotenv").config();

// Create a connection pool
const pool = mysql.createPool({
	host: process.env.DB_HOST,
	port: Number(process.env.PORT) || 3306,
	user: process.env.USER,
	password: process.env.PASSWORD,
	database: process.env.DATABASE,
	waitForConnections: true,
	connectionLimit: 20,
	queueLimit: 0,
	enableKeepAlive: true,
	keepAliveInitialDelay: 10000,
});

console.log("Database pool created and ready for connections");

// Export a promise-based pool for async/await usage
const promisePool = pool.promise();

const { formatPriceUk } = require("./lib/property-helpers.js");
const DB_VERBOSE_LOGS = process.env.DB_VERBOSE_LOGS === "1";

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
                    SET price = ?, latitude = ?, longitude = ?, remove_status = 0, updated_at = NOW()
                    WHERE property_url = ? AND agent_id = ?`,
					[price, latitude, longitude, linkTrimmed, agent_id],
				);

				if (result.affectedRows > 0) {
					if (DB_VERBOSE_LOGS) {
						console.log(
							`✅ Updated: ${linkTrimmed.substring(
								0,
								50,
							)}... | Price: £${formatPriceUk(price)} | Coords: ${latitude}, ${longitude}`,
						);
					}
				} else if (DB_VERBOSE_LOGS) {
					console.log(`⚠️ No update: ${linkTrimmed.substring(0, 50)}...`);
				}
			} else {
				// INSERT new property
				const insertQuery = `INSERT INTO ${tableName} (property_name, agent_id, price, bedrooms, property_url, logo, latitude, longitude, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`;

				const logo = "property_for_sale/logo.png"; // static logo
				const currentTime = new Date();
				// Truncate property name to 150 characters to avoid database column size errors
				const truncatedTitle = title ? title.substring(0, 150) : "";

				await promisePool.query(insertQuery, [
					truncatedTitle,
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

				if (DB_VERBOSE_LOGS) {
					console.log(
						`✅ Created: ${linkTrimmed.substring(
							0,
							50,
						)}... | Price: £${formatPriceUk(price)} | Coords: ${latitude}, ${longitude}`,
					);
				}
			}
		}
	} catch (error) {
		console.error(`❌ Error updating property: ${error.message}`);
		throw error;
	}
}

// Update remove status for old properties
async function updateRemoveStatus(agent_id, scrapeStartTime = null) {
	try {
		const remove_status = 1;

		let timeCondition =
			"updated_at < NOW() - INTERVAL 1 DAY OR updated_at > NOW() + INTERVAL 1 DAY";

		// If we have a specific scrape start time, use it as the safety window
		// This is much more accurate as it flags anything NOT updated during THIS run
		if (scrapeStartTime) {
			const formattedStartTime =
				scrapeStartTime instanceof Date
					? scrapeStartTime.toISOString().slice(0, 19).replace("T", " ")
					: scrapeStartTime;
			timeCondition = `updated_at < '${formattedStartTime}'`;
		}

		const [saleResult] = await promisePool.query(
			`UPDATE property_for_sale
             SET remove_status = ?
             WHERE agent_id = ?
             AND (${timeCondition})`,
			[remove_status, agent_id],
		);

		const [rentResult] = await promisePool.query(
			`UPDATE property_for_rent
             SET remove_status = ?
             WHERE agent_id = ?
             AND (${timeCondition})`,
			[remove_status, agent_id],
		);

		const removedCount = (saleResult?.affectedRows || 0) + (rentResult?.affectedRows || 0);
		console.log(
			`🧹 Removed old properties for agent ${agent_id} (sale: ${
				saleResult?.affectedRows || 0
			}, rent: ${rentResult?.affectedRows || 0}, total: ${removedCount}) using window: ${
				scrapeStartTime ? scrapeStartTime : "1 DAY"
			}`,
		);
	} catch (error) {
		console.error("Error updating remove status:", error.message);
	}
}

module.exports = {
	promisePool,
	updatePriceByPropertyURL,
	updateRemoveStatus,
};
// Deprecated: backward compatibility if code uses pool.promise() directly
module.exports.promise = () => promisePool;
