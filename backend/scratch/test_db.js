const mysql = require('mysql2/promise');
require('dotenv').config({ path: './backend/.env' });

(async () => {
    try {
        console.log(`Connecting to ${process.env.DB_HOST}:${process.env.PORT}...`);
        const connection = await mysql.createConnection({
            host: process.env.DB_HOST,
            port: process.env.PORT,
            user: process.env.USER,
            password: process.env.PASSWORD,
            database: process.env.DATABASE
        });
        console.log("Connected successfully!");
        await connection.end();
    } catch (err) {
        console.error("Connection failed:", err);
    }
})();
