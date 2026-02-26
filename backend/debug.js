const fs = require("fs");
const code = fs.readFileSync("backend/scraper-agent-107.js", "utf8");
const startRegex =
	/(logger\.step\((?:"|'|`)[^\)]+scraper[^\)]+\);)|(console\.log\((?:"|'|`)[^\)]+scraper[^\)]+\);)/i;
const m = code.match(startRegex);
console.log("match", m ? m[0] : null);
if (m) {
	const pos = code.indexOf(m[0]) + m[0].length;
	console.log("insertPos", pos);
	console.log("slice", code.slice(pos, pos + 200));
}
