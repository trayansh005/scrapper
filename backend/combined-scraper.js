const { spawn } = require("child_process");
const path = require("path");
const fs = require("fs");

/**
 * Generic function to run a standalone agent script
 * @param {string|number} agentId - The ID of the agent
 * @param {string|number} startPage - Optional starting page number
 */
async function runAgentScript(agentId, startPage = null) {
	return new Promise((resolve) => {
		const filename = `scraper-agent-${agentId}.js`;
		const scriptPath = path.join(__dirname, filename);

		// Check if script exists
		if (!fs.existsSync(scriptPath)) {
			console.error(`\n❌ Error: Script for Agent ${agentId} not found (${filename})`);
			return resolve();
		}

		console.log(`\n================================================================`);
		console.log(`🚀 RUNNING AGENT ${agentId}`);
		if (startPage) console.log(`📄 Starting from Page: ${startPage}`);
		console.log(`================================================================\n`);

		const args = startPage ? [startPage.toString()] : [];
		const child = spawn("node", [scriptPath, ...args], {
			stdio: "inherit",
			cwd: __dirname,
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`\n✅ Agent ${agentId} completed successfully.`);
			} else {
				console.log(`\n⚠️ Agent ${agentId} exited with code ${code}.`);
			}
			resolve();
		});

		child.on("error", (err) => {
			console.error(`\n❌ Failed to start Agent ${agentId}:`, err.message);
			resolve();
		});
	});
}

// Main execution logic
(async () => {
	const args = process.argv.slice(2);

	if (args.length === 0) {
		console.log(`
📖 STANDALONE AGENT RUNNER
--------------------------
Usage:
  node combined-scraper.js <agentId> <startPage>    # Run 1 agent from a specific page
  node combined-scraper.js <id1> <id2> <id3>...      # Run multiple agents sequentially

Examples:
  node combined-scraper.js 35 10                    # Run Agent 35 starting from page 10
	node combined-scraper.js 108                      # Run Agent 108
	node combined-scraper.js 228                      # Run Agent 228
  node combined-scraper.js 225                      # Run Agent 225
	node combined-scraper.js 224                      # Run Agent 224
	node combined-scraper.js 248                      # Run Agent 248
	node combined-scraper.js 249                      # Run Agent 249
	node combined-scraper.js 245                      # Run Agent 245
  node combined-scraper.js 24 25 32 34              # Run Agents 24, 25, 32, and 34 in order
		`);
		process.exit(0);
	}

	// Case 1: Single Agent with Start Page (Exactly 2 numeric arguments)
	// BUT only if the second argument isn't ALSO a script file
	if (args.length === 2 && !isNaN(args[0]) && !isNaN(args[1])) {
		const secondArgIsScript = fs.existsSync(path.join(__dirname, `scraper-agent-${args[1]}.js`));

		if (secondArgIsScript) {
			console.log(`📝 Sequential queue: Agents ${args.join(", ")}`);
			for (const id of args) {
				await runAgentScript(id);
			}
		} else {
			await runAgentScript(args[0], args[1]);
		}
	}
	// Case 2: Multi-agent sequential run (1, 3, or more arguments)
	else {
		console.log(`📝 Sequential queue: Agents ${args.join(", ")}`);
		for (const id of args) {
			const agentId = parseInt(id);
			if (!isNaN(agentId)) {
				await runAgentScript(agentId);
			} else {
				console.log(`\n⏩ Skipping invalid ID: ${id}`);
			}
		}
	}

	console.log("\n🏁 All tasks in queue finished.");
	process.exit(0);
})();
