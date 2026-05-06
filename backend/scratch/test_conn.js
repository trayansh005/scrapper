const mysql = require('mysql2/promise');
require('dotenv').config({ path: 'backend/.env' });

(async () => {
  const config = {
    host: process.env.DB_HOST,
    port: Number(process.env.PORT) || 3307,
    user: process.env.DB_USER || process.env.USER,
    password: process.env.PASSWORD,
    database: process.env.DATABASE,
    connectionLimit: 1
  };

  console.log(`Connecting to ${config.host}:${config.port}...`);
  
  for (let i = 1; i <= 5; i++) {
    try {
      const connection = await mysql.createConnection(config);
      console.log(`[${i}] Connection successful!`);
      await connection.end();
    } catch (err) {
      console.error(`[${i}] Connection failed: ${err.message}`);
    }
    await new Promise(r => setTimeout(r, 2000));
  }
})();
