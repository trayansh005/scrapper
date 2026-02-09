const fs = require("fs");
const cheerio = require("cheerio");

try {
	const html = fs.readFileSync("backend/listing-27.html", "utf8");
	const $ = cheerio.load(html);

	// Dump all links
	const links = [];
	$("a").each((i, el) => {
		const href = $(el).attr("href");
		if (href) links.push(href);
	});

	console.log("Total links:", links.length);
	console.log("Sample links:", links.slice(0, 50));

	// Look for price or something unique
	const prices = [];
	$('.price, span:contains("£"), div:contains("£")').each((i, el) => {
		prices.push($(el).text().trim());
	});
	console.log("Possible prices:", prices.slice(0, 10));
} catch (err) {
	console.error(err);
}
