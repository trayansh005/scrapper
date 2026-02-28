const { promisePool } = require("./backend/db.js");

(async () => {
	try {
		console.log("Testing DB connection...");
		const [rows] = await promisePool.query("SELECT 1 as test");
		console.log("DB connection successful:", rows);
		process.exit(0);
	} catch (err) {
		console.error("DB connection failed:", err.message);
		process.exit(1);
	}
})();
