const express = require("express");
const cors = require("cors");
const { promisePool } = require("./db.js");

const app = express();

// Enable CORS (allow all origins) and handle preflight requests
app.use(cors({ origin: true, credentials: false }));
app.options("*", cors({ origin: true, credentials: false }));

app.use(express.json());

const PORT = process.env.PORT || 4080;

// Health check endpoint for Coolify
app.get("/health", (req, res) => {
	res.status(200).json({
		status: "healthy",
		timestamp: new Date().toISOString(),
		service: "property-scraper-api",
	});
});

// Get all properties for sale
app.get("/api/properties/sale", async (req, res) => {
	try {
		const [rows] = await promisePool.query(
			"SELECT * FROM property_for_sale WHERE remove_status = 0 ORDER BY updated_at DESC LIMIT 100",
		);
		res.json({ success: true, data: rows });
	} catch (error) {
		console.error("Error fetching sale properties:", error);
		res.status(500).json({ success: false, error: error.message });
	}
});

// Get all properties for rent
app.get("/api/properties/rent", async (req, res) => {
	try {
		const [rows] = await promisePool.query(
			"SELECT * FROM property_for_rent WHERE remove_status = 0 ORDER BY updated_at DESC LIMIT 100",
		);
		res.json({ success: true, data: rows });
	} catch (error) {
		console.error("Error fetching rent properties:", error);
		res.status(500).json({ success: false, error: error.message });
	}
});

// Get properties by agent ID
app.get("/api/properties/agent/:agent_id", async (req, res) => {
	try {
		const agent_id = parseInt(req.params.agent_id);
		if (isNaN(agent_id)) {
			return res.status(400).json({ success: false, error: "Invalid agent ID" });
		}

		const [saleRows] = await promisePool.query(
			"SELECT *, 'sale' as type FROM property_for_sale WHERE agent_id = ? AND remove_status = 0 ORDER BY updated_at DESC",
			[agent_id],
		);

		const [rentRows] = await promisePool.query(
			"SELECT *, 'rent' as type FROM property_for_rent WHERE agent_id = ? AND remove_status = 0 ORDER BY updated_at DESC",
			[agent_id],
		);

		const properties = [...saleRows, ...rentRows];
		res.json({ success: true, data: properties, count: properties.length });
	} catch (error) {
		console.error("Error fetching agent properties:", error);
		res.status(500).json({ success: false, error: error.message });
	}
});

// Get property statistics
app.get("/api/stats", async (req, res) => {
	try {
		const [saleCount] = await promisePool.query(
			"SELECT COUNT(*) as count FROM property_for_sale WHERE remove_status = 0",
		);

		const [rentCount] = await promisePool.query(
			"SELECT COUNT(*) as count FROM property_for_rent WHERE remove_status = 0",
		);

		const [agentStats] = await promisePool.query(`
            SELECT 
                agent_id,
                COUNT(*) as property_count,
                'sale' as type
            FROM property_for_sale 
            WHERE remove_status = 0 
            GROUP BY agent_id
            UNION ALL
            SELECT 
                agent_id,
                COUNT(*) as property_count,
                'rent' as type
            FROM property_for_rent 
            WHERE remove_status = 0 
            GROUP BY agent_id
        `);

		res.json({
			success: true,
			data: {
				totalSale: saleCount[0].count,
				totalRent: rentCount[0].count,
				total: saleCount[0].count + rentCount[0].count,
				agentStats,
			},
		});
	} catch (error) {
		console.error("Error fetching statistics:", error);
		res.status(500).json({ success: false, error: error.message });
	}
});

// Search properties
app.get("/api/properties/search", async (req, res) => {
	try {
		const { q, type, agent_id, limit = 50 } = req.query;

		let query = "";
		let params = [];

		if (type === "sale") {
			query = "SELECT *, 'sale' as type FROM property_for_sale WHERE remove_status = 0";
		} else if (type === "rent") {
			query = "SELECT *, 'rent' as type FROM property_for_rent WHERE remove_status = 0";
		} else {
			query = `
                SELECT *, 'sale' as type FROM property_for_sale WHERE remove_status = 0
                UNION ALL
                SELECT *, 'rent' as type FROM property_for_rent WHERE remove_status = 0
            `;
		}

		if (q) {
			if (type === "sale" || type === "rent") {
				query += " AND (property_name LIKE ? OR property_url LIKE ?)";
				params.push(`%${q}%`, `%${q}%`);
			} else {
				query = `
                    SELECT *, 'sale' as type FROM property_for_sale 
                    WHERE remove_status = 0 AND (property_name LIKE ? OR property_url LIKE ?)
                    UNION ALL
                    SELECT *, 'rent' as type FROM property_for_rent 
                    WHERE remove_status = 0 AND (property_name LIKE ? OR property_url LIKE ?)
                `;
				params.push(`%${q}%`, `%${q}%`, `%${q}%`, `%${q}%`);
			}
		}

		if (agent_id) {
			if (type === "sale" || type === "rent") {
				query += " AND agent_id = ?";
				params.push(agent_id);
			} else {
				query = `
                    SELECT *, 'sale' as type FROM property_for_sale 
                    WHERE remove_status = 0 AND agent_id = ?
                    ${q ? "AND (property_name LIKE ? OR property_url LIKE ?)" : ""}
                    UNION ALL
                    SELECT *, 'rent' as type FROM property_for_rent 
                    WHERE remove_status = 0 AND agent_id = ?
                    ${q ? "AND (property_name LIKE ? OR property_url LIKE ?)" : ""}
                `;
				params = [agent_id];
				if (q) {
					params.push(`%${q}%`, `%${q}%`, agent_id, `%${q}%`, `%${q}%`);
				} else {
					params.push(agent_id);
				}
			}
		}

		query += ` ORDER BY updated_at DESC LIMIT ?`;
		params.push(parseInt(limit));

		const [rows] = await promisePool.query(query, params);
		res.json({ success: true, data: rows, count: rows.length });
	} catch (error) {
		console.error("Error searching properties:", error);
		res.status(500).json({ success: false, error: error.message });
	}
});

// Trigger scraper (placeholder - actual scraping runs separately)
app.post("/api/scraper/trigger/:agent_id", (req, res) => {
	const agent_id = parseInt(req.params.agent_id);
	if (isNaN(agent_id)) {
		return res.status(400).json({ success: false, error: "Invalid agent ID" });
	}

	// This is just a placeholder endpoint
	// Actual scraping should be triggered via cron jobs or separate processes
	res.json({
		success: true,
		message: `Scraper trigger received for agent ${agent_id}`,
		note: "Scraping runs as separate processes - check your deployment logs",
	});
});

// Error handling middleware
app.use((err, req, res, next) => {
	console.error("Unhandled error:", err);
	res.status(500).json({
		success: false,
		error: "Internal server error",
		message: err.message,
	});
});

// 404 handler
app.use((req, res) => {
	res.status(404).json({
		success: false,
		error: "Endpoint not found",
		path: req.path,
	});
});

// Test database connection on startup
async function startServer() {
	try {
		// Test the database connection
		const [result] = await promisePool.query("SELECT 1");
		console.log("✅ Database connection successful");
	} catch (error) {
		console.error("❌ Failed to connect to database:", error.message);
		console.error("⚠️  Server starting without database connection!");
		console.error("Make sure MySQL is running on localhost:3306");
	}

	app.listen(PORT, "0.0.0.0", () => {
		console.log(`🚀 Property Scraper API running on port ${PORT}`);
		console.log(`📊 Health check: http://localhost:${PORT}/health`);
		console.log(`🏠 API endpoints: http://localhost:${PORT}/api/`);
	});
}

startServer();

module.exports = app;
