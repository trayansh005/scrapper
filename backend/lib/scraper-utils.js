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

/**
 * Run agent 15 using the separate script
 * @param {number} startPage - Starting page number (defaults to 1)
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent15Scraper(startPage = 1) {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 15 (Sequence Home) from separate script...`);
		if (startPage > 1) {
			console.log(`📄 Starting from page ${startPage}`);
		}

		const scriptPath = path.join(__dirname, "..", "scraper-agent-15.js");
		const args = startPage > 1 ? [startPage.toString()] : [];
		const child = spawn("node", [scriptPath, ...args], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 15 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 15 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 15 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 16 using the separate script
 * @param {number} startPage - Starting page number (defaults to 1)
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent16Scraper(startPage = 1) {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 16 (Romans) from separate script...`);
		if (startPage > 1) {
			console.log(`📄 Starting from page ${startPage}`);
		}

		const scriptPath = path.join(__dirname, "..", "scraper-agent-16.js");
		const args = startPage > 1 ? [startPage.toString()] : [];
		const child = spawn("node", [scriptPath, ...args], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 16 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 16 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 16 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 18 using the separate script
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent18Scraper() {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 18 (Moveli) from separate script...`);

		const scriptPath = path.join(__dirname, "..", "scraper-agent-18.js");
		const child = spawn("node", [scriptPath], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 18 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 18 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 18 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 19 using the separate script
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent19Scraper() {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 19 (Snellers) from separate script...`);

		const scriptPath = path.join(__dirname, "..", "scraper-agent-19.js");
		const child = spawn("node", [scriptPath], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 19 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 19 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 19 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 22 using the separate script
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent22Scraper() {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 22 (Allsop) from separate script...`);

		const scriptPath = path.join(__dirname, "..", "scraper-agent-22.js");
		const child = spawn("node", [scriptPath], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 22 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 22 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 22 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 24 using the separate script
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent24Scraper() {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 24 (Haboodle) from separate script...`);

		const scriptPath = path.join(__dirname, "..", "scraper-agent-24.js");
		const child = spawn("node", [scriptPath], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 24 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 24 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 24 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 25 using the separate script
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent25Scraper() {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 25 (Marriott Vernon) from separate script...`);

		const scriptPath = path.join(__dirname, "..", "scraper-agent-25.js");
		const child = spawn("node", [scriptPath], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 25 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 25 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 25 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 32 using the separate script
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent32Scraper() {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 32 (Remax) from separate script...`);

		const scriptPath = path.join(__dirname, "..", "scraper-agent-32.js");
		const child = spawn("node", [scriptPath], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 32 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 32 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 32 script: ${err.message}`));
		});
	});
}

/**
 * Run agent 34 using the separate script
 * @returns {Promise} - Promise that resolves when scraper completes
 */
async function runAgent34Scraper() {
	return new Promise((resolve, reject) => {
		console.log(`\n🚀 Running Agent 34 (Strutt & Parker) from separate script...`);

		const scriptPath = path.join(__dirname, "..", "scraper-agent-34.js");
		const child = spawn("node", [scriptPath], {
			stdio: "inherit",
			cwd: path.join(__dirname, ".."),
		});

		child.on("close", (code) => {
			if (code === 0) {
				console.log(`✅ Agent 34 completed successfully`);
				resolve();
			} else {
				reject(new Error(`Agent 34 script exited with code ${code}`));
			}
		});

		child.on("error", (err) => {
			reject(new Error(`Failed to start Agent 34 script: ${err.message}`));
		});
	});
}

module.exports = {
	logMemoryUsage,
	runAgent13Scraper,
	runAgent14Scraper,
	runAgent15Scraper,
	runAgent16Scraper,
	runAgent18Scraper,
	runAgent19Scraper,
	runAgent22Scraper,
	runAgent24Scraper,
	runAgent25Scraper,
	runAgent32Scraper,
	runAgent34Scraper,
};
