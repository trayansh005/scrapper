
const { updatePriceByPropertyURLOptimized } = require("./lib/db-helpers.js");
const { createAgentLogger } = require("./lib/logger-helpers.js");

async function test() {
    const AGENT_ID = 70;
    const logger = createAgentLogger(AGENT_ID);
    
    console.log("Starting DB test for Agent 70...");
    
    try {
        const result = await updatePriceByPropertyURLOptimized(
            "https://www.test-link.com/prop123",
            "1,000,000",
            "Test Property",
            "3",
            AGENT_ID,
            false
        );
        
        console.log("Result:", JSON.stringify(result, null, 2));
        
        if (result.error) {
            console.error("Caught error in result:", result.error);
        }
    } catch (err) {
        console.error("FATAL ERROR in test execution:", err);
    } finally {
        process.exit(0);
    }
}

test();
