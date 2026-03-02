(async () => {
	const url = "https://www.belvoir.co.uk/properties/for-sale/in-united-kingdom/";
	try {
		const r = await fetch(url, { headers: { "user-agent": "Mozilla/5.0" } });
		const html = await r.text();

		console.log("=".repeat(60));
		console.log("BELVOIR PAGE ANALYSIS");
		console.log("=".repeat(60));
		console.log("URL:", url);
		console.log("Status:", r.status);
		console.log("HTML length:", html.length);
		console.log("");

		// Check for propertyData
		const hasPropertyData = /var\s+propertyData/.test(html);
		console.log("Has propertyData script:", hasPropertyData);

		// Check various card selectors
		const selectors = [
			".property--card",
			'[class*="property"]',
			'[class*="card"]',
			"li",
			"article",
			'[role="article"]',
		];

		console.log("\nSelector test results:");
		for (const sel of selectors) {
			const escaped = sel.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
			const count = (html.match(new RegExp(escaped, "g")) || []).length;
			console.log(`  ${sel.padEnd(25)} : ${count} matches`);
		}

		// Extract snippet around first list/card elements
		const mainIdx = html.indexOf("<main");
		const snippet = html.substring(mainIdx, mainIdx + 2000);

		console.log("\nHTML snippet (first 1500 chars of <main>):");
		console.log("---");
		console.log(snippet.substring(0, 1500));
		console.log("---");
	} catch (e) {
		console.error("Error:", e.message);
	}
})();
