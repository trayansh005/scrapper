
const { updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

async function test() {
    const AGENT_ID = 70;
    const logger = createAgentLogger(AGENT_ID);
    
    console.log("Starting Concurrent DB test for Agent 70...");
    
    const tasks = [];
    for (let i = 0; i < 20; i++) {
        tasks.push(updatePriceByPropertyURLOptimized(
            `https://www.test-link.com/prop${i}`,
            "1,000,000",
            `Test Property ${i}`,
            "3",
            AGENT_ID,
            false
        ));
    }
    
    try {
        const results = await Promise.all(tasks);
        console.log(`Processed ${results.length} tasks.`);
        
        const errors = results.filter(r => r.error);
        if (errors.length > 0) {
            console.error(`${errors.length} tasks returned errors:`);
            errors.forEach((e, idx) => console.error(`Error ${idx}:`, e.error));
        }
    } catch (err) {
        console.error("FATAL ERROR in test execution:", err);
        console.error("Error type:", err.constructor.name);
        if (err.errors) {
            console.error("AggregateError details:", err.errors);
        }
    } finally {
        process.exit(0);
    }
}

test();
