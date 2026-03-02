(async () => {
	try {
		const url = "https://www.belvoir.co.uk/properties/for-sale/in-united-kingdom/";
		const response = await fetch(url, {
			headers: { "user-agent": "Mozilla/5.0" },
		});
		const html = await response.text();

		console.log("=".repeat(70));
		console.log("BELVOIR HTML STRUCTURE ANALYSIS");
		console.log("=".repeat(70));

		// Look for script data
		const scriptMatches = html.match(/<script[^>]*>([\s\S]*?)<\/script>/g) || [];
		console.log("\nTotal script tags found:", scriptMatches.length);

		// Look for json/data scripts
		const jsonScripts =
			html.match(/<script[^>]*type=["']([^"']*json[^"']*)["'][^>]*>([\s\S]*?)<\/script>/gi) || [];
		console.log("JSON data scripts:", jsonScripts.length);

		// Look for inline data patterns
		const hasWindowData = /window\.\w+\s*=\s*\{/.test(html);
		const hasVueData = /window\.__INITIAL_STATE__|__data__|props|properties/.test(html);
		const hasApplicationJson = /type="application\/json"/.test(html);

		console.log("Has window.* = {...}:", hasWindowData);
		console.log("Has Vue/React data patterns:", hasVueData);
		console.log('Has type="application/json" scripts:', hasApplicationJson);

		// Extract all script tags with content length
		const allScripts = [];
		let match;
		const scriptRegex = /<script[^>]*>([\s\S]*?)<\/script>/g;
		while ((match = scriptRegex.exec(html)) !== null) {
			const content = match[1];
			allScripts.push({
				length: content.length,
				preview: content.substring(0, 150),
				hasProperty: /properties|items|data/.test(content),
				type: html.substring(match.index, match.index + 100),
			});
		}

		console.log("\nScript content analysis:");
		allScripts.slice(0, 10).forEach((s, i) => {
			console.log(`  Script ${i + 1}: ${s.length} bytes, hasProperty: ${s.hasProperty}`);
			if (s.hasProperty) console.log(`    Preview: ${s.preview.substring(0, 100)}`);
		});

		// Look for specific elementor/component patterns
		const hasComponentData = /data-component|data-bind|data-settings/.test(html);
		const hasReactRoot = /id="root"|id="app"|data-react/.test(html);

		console.log("\nPage framework indicators:");
		console.log("Has Elementor data attributes:", hasComponentData);
		console.log("Has React/Vue root:", hasReactRoot);

		// Extract a larger snippet from main to see structure
		const mainStart = html.indexOf("<main");
		const mainEnd = html.indexOf("</main>") + 7;
		if (mainStart > -1) {
			const mainContent = html.substring(mainStart, Math.min(mainEnd, mainStart + 5000));
			console.log("\n<main> content (first 2000 chars):");
			console.log("---");
			console.log(mainContent.substring(0, 2000));
			console.log("---");
		}
	} catch (error) {
		console.error("Error:", error.message);
	}
})();
