const fs = require("fs");
const cheerio = require("cheerio");
const content = fs.readFileSync("tmp_47.html", "utf8");
const $ = cheerio.load(content);
const properties = [];
$(".property-card").each((i, el) => {
	const link = $(el).find("a").attr("href")
		? "https://www.douglasallen.co.uk" + $(el).find("a").attr("href").split("?")[0]
		: null;
	const title = $(el).find(".properties-info h2").text().trim() || null;
	const priceText = $(el).find(".property-price").text().trim();
	const bedText = $(el).find(".icon-bedroom").parent().text().trim();
	const statusText = $(el).find(".card-line").text().trim();
	properties.push({ link, title, priceText, bedText, statusText });
});
console.log(JSON.stringify(properties.slice(0, 3), null, 2));
console.log("Total found:", properties.length);
