const mysql = require("mysql2"); // Use mysql2 for better performance

// Create a connection pool
const pool = mysql.createPool({
	host: "locaalhost",
	user: "admin",
	password: "SecurePassword", // Change to your actual password
	password: "", // Change to your actual password
	database: "scrapper",
	waitForConnections: true,
	connectionLimit: 10, // Limits active connections to 10
	queueLimit: 0,
});

console.log("Database pool created and ready for connections");

// Export a promise-based pool for async/await usage
module.exports = pool.promise();
