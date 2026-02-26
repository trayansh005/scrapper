// Utility script to apply Agent 4 baseline refactor to multiple agent files
// Run with: node backend/refactor-agents.js

const fs = require("fs");
const path = require("path");

// List of agent IDs that need the baseline update (from screenshot)
const AGENT_IDS = [127, 133,  134, 135, 207, 209, 210, 211, 21];

function applyBaseline(filePath) {
	let code = fs.readFileSync(filePath, "utf8");
	let modified = false;

	// 1. ensure markAllPropertiesRemovedForAgent is imported from db.js
	if (!/markAllPropertiesRemovedForAgent/.test(code)) {
		// try to append to existing destructure from db.js (handles single or double quotes)
		const importRegex = /const\s+\{([^}]+)\}\s*=\s*require\(['\"]\.\/db\.js['\"]\);/;
		if (importRegex.test(code)) {
			code = code.replace(importRegex, (m, inner) => {
				const parts = inner
					.split(",")
					.map((s) => s.trim())
					.filter(Boolean);
				if (!parts.includes("markAllPropertiesRemovedForAgent"))
					parts.push("markAllPropertiesRemovedForAgent");
				return `const { ${parts.join(", ")} } = require("./db.js");`;
			});
			modified = true;
		} else {
			// no existing destructure, add a separate import line
			const requireLineMatch = code.match(/(const .*require\(['\"]\.\/db\.js['\"]\);)/);
			if (requireLineMatch) {
				const insertPos = code.indexOf(requireLineMatch[0]) + requireLineMatch[0].length;
				const snippet = `\nconst { markAllPropertiesRemovedForAgent } = require("./db.js");`;
				code = code.slice(0, insertPos) + snippet + code.slice(insertPos);
				modified = true;
			}
		}
	}

	// 2. insert markAllPropertiesRemovedForAgent call after the first startup log message (logger.step or console.log)
	// look for either logger.step or console.log startup message, case-insensitive
	const startRegex =
		/(logger\.step\((?:"|'|`)[^\)]+scraper[^\)]+\);)|(console\.log\((?:"|'|`)[^\)]+scraper[^\)]+\);)/i;
	const match = code.match(startRegex);
	if (match) {
		const insertPos = code.indexOf(match[0]) + match[0].length;
		const snippet = "\n\tawait markAllPropertiesRemovedForAgent(AGENT_ID);\n";
		if (!code.slice(insertPos, insertPos + 200).includes("markAllPropertiesRemovedForAgent")) {
			code = code.slice(0, insertPos) + snippet + code.slice(insertPos);
			modified = true;
		}
	}

	if (modified) {
		fs.writeFileSync(filePath, code, "utf8");
		console.log(`Updated ${path.basename(filePath)}`);
	} else {
		console.log(`No changes needed for ${path.basename(filePath)}`);
	}
}

for (const id of AGENT_IDS) {
	const filePath = path.resolve(__dirname, `scraper-agent-${id}.js`);
	if (fs.existsSync(filePath)) {
		applyBaseline(filePath);
	} else {
		console.warn(`Agent file not found: ${filePath}`);
	}
}
