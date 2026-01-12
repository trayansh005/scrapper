const { chromium } = require("playwright");

(async () => {
	const browser = await chromium.launch({ headless: true });
	const page = await browser.newPage();
	const url =
		process.argv[2] ||
		"https://www.chartersestateagents.co.uk/property-for-sale/4-bedroom-house-for-sale-in-brunswick-gardens-31-chilbolton-avenue-winchester-hampshire-so22-695be00bf3511c83bb74e595/";
	console.log("Go to detail:", url);
	await page.goto(url, { waitUntil: "networkidle", timeout: 120000 });
	await page.waitForTimeout(1500);

	const info = await page.evaluate(() => {
		const getText = (sel) => document.querySelector(sel)?.innerText?.trim() || "";

		// Try common price spots
		const candidates = [
			"[class*='price']",
			"[class*='Price']",
			"[class*='header'] h2",
			"h2",
			"h3",
			"[class*='banner']",
		];
		let priceText = "";
		for (const sel of candidates) {
			const els = Array.from(document.querySelectorAll(sel));
			for (const el of els) {
				const t = (el.innerText || el.textContent || "").trim();
				if (/£\s*[\d,]+/.test(t) && !/Deposit/i.test(t)) {
					priceText = t;
					break;
				}
			}
			if (priceText) break;
		}

		// JSON-LD price/address
		let jsonPrice = "",
			jsonAddress = "";
		const scripts = Array.from(document.querySelectorAll("script[type='application/ld+json']"));
		for (const s of scripts) {
			try {
				const j = JSON.parse(s.textContent);
				const graph = Array.isArray(j["@graph"]) ? j["@graph"] : [j];
				for (const it of graph) {
					if (it["@type"] === "Offer" && (it.price || it.priceSpecification?.price)) {
						jsonPrice = (it.price || it.priceSpecification?.price) + "";
					}
					if ((it["@type"] === "Place" || it["@type"] === "Residence") && it.name) {
						jsonAddress = it.name;
					}
				}
			} catch {}
		}

		// Address from heading
		const heading = getText("h1") || getText("h2");

		// Bedrooms heuristic
		const body = document.body.innerText;
		const bedMatch = body.match(/(\d+)\s*bed(room)?/i);
		const bedrooms = bedMatch ? parseInt(bedMatch[1], 10) : null;

		return { priceText, jsonPrice, jsonAddress, heading, bedrooms };
	});

	console.log("Detail info:", info);

	// Second pass: check iframes for lat/lng
	const browser2 = await chromium.launch({ headless: true });
	const page2 = await browser2.newPage();
	await page2.goto(url, { waitUntil: "networkidle", timeout: 120000 });
	await page2.waitForTimeout(1000);
	const coords = await page2.evaluate(() => {
		const res = { lat: null, lng: null, src: "" };
		const iframes = Array.from(document.querySelectorAll("iframe"));
		for (const f of iframes) {
			if (!f.src) continue;
			const m1 = f.src.match(/[?&]lat=([-0-9.]+).*?[&]lng=([-0-9.]+)/);
			if (m1) {
				res.lat = parseFloat(m1[1]);
				res.lng = parseFloat(m1[2]);
				res.src = f.src;
				break;
			}
			const m2 = f.src.match(/[?&]q=([-0-9.]+),([-0-9.]+)/);
			if (m2) {
				res.lat = parseFloat(m2[1]);
				res.lng = parseFloat(m2[2]);
				res.src = f.src;
				break;
			}
		}
		return res;
	});
	console.log("Coords:", coords);

	await browser2.close();
	await browser.close();
})();
