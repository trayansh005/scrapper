// Scraper utility functions
const { spawn } = require("child_process");
const path = require("path");

/**
 * Memory monitoring utility
 * @param {string} label - Label for the memory log
 */
function logMemoryUsage(label) {
	const used = process.memoryUsage();
	console.log(
		`[${label}] Memory: ${Math.round(used.heapUsed / 1024 / 1024)}MB / ${Math.round(
			used.heapTotal / 1024 / 1024,
		)}MB`,
	);
}

/**
 * Run agent 13 using the separate script
 * @param {number} startPage - Starting page number (defaults to 1)
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent13Scraper(startPage = 1) {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 13 (Bairstow Eves) from separate script...`);
		if (startPage > 1) {
			console.log(`📄 Starting from page ${startPage}`);
		}

		const scriptPath = path.join(__dirname, "..", "scraper-agent-13.js");
		const args = startPage > 1 ? [startPage.toString()] : [];
		const child = spawn("node", [scriptPath, ...args], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 13 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 13 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 13 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 14 using the separate script
 * @param {number} startPage - Starting page number (defaults to 1)
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent14Scraper(startPage = 1) {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 14 (Chestertons) from separate script...`);
		if (startPage > 1) {
			console.log(`📄 Starting from page ${startPage}`);
		}

		const scriptPath = path.join(__dirname, "..", "scraper-agent-14.js");
		const args = startPage > 1 ? [startPage.toString()] : [];
		const child = spawn("node", [scriptPath, ...args], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 14 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 14 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 14 script: ${err.message}`));
		});
	});
}

module.exports = {
	logMemoryUsage,
	runAgent13Scraper,
	runAgent14Scraper,
};
