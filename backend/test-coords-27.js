const fs = require("fs");
const cheerio = require("cheerio");

try {
	const html = fs.readFileSync("backend/detail-27.html", "utf8");
	const $ = cheerio.load(html);

	// Look for anything that looks like lat/lng in data attributes
	$("*").each((i, el) => {
		const attribs = el.attribs;
		for (const attr in attribs) {
			if (
				attr.includes("data-") &&
				(attribs[attr].includes("lat") ||
					attribs[attr].includes("lng") ||
					attribs[attr].includes("51."))
			) {
				console.log(`Found potential attr on <${el.tagName}>: ${attr}="${attribs[attr]}"`);
			}
		}
	});

	// Also look for scripts
	$("script").each((i, el) => {
		const content = $(el).contents().text();
		if (content.includes("lat") || content.includes("latitude")) {
			const match =
				content.match(/lat["']?\s*:\s*(-?\d+\.\d+)/) ||
				content.match(/latitude["']?\s*:\s*(-?\d+\.\d+)/);
			if (match) {
				console.log("Found coordinates in script:", match[0]);
			}
		}
	});
} catch (err) {
	console.error(err);
}
