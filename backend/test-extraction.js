// Test extraction logic
const testCases = [
	"2 Bed Flat\n£3,200 pcm\nLondon SE11",
	"Studio\n£1,195 pcm\nLondon SE11",
	"3-Bed Flat • £2,500 pcm",
	"Room in flat\n£800 pcm\nShared",
	"1 bed apartment\n£2,000\nCentral London",
];

function testExtraction(fullText) {
	let title = "";

	// Look for patterns like "2-bed flat", "studio flat", "3-bedroom", etc.
	const propMatch = fullText.match(/(\d+[\s-]?(?:bed|bedroom|studio)|studio)[^£\n]*/i);
	if (propMatch) {
		title = propMatch[0].trim().replace(/\s*\n.*/, "").substring(0, 100);
		console.log(`  ✓ Found via property pattern: "${title}"`);
		return;
	}

	// If still no title, try to extract from fullText - first meaningful line
	if (!title || title.length < 3) {
		const lines = fullText
			.split("\n")
			.map((line) => line.trim())
			.filter(
				(line) =>
					line.length > 3 &&
					!line.match(/^£|pcm|pm|pa$|let agreed|withdrawn|to rent|for rent/i) &&
					!line.match(/^[a-z]\.$|^[0-9]+$/),
			);
		if (lines.length > 0) {
			title = lines[0].substring(0, 100);
			console.log(`  ✓ Found via first line: "${title}"`);
			return;
		}
	}

	console.log(`  ✗ No match found, defaulting to "Property"`);
}

console.log("Testing extraction logic:\n");
testCases.forEach((testCase, i) => {
	console.log(`Test ${i + 1}:`);
	console.log(`  Input: ${testCase.replace(/\n/g, " | ")}`);
	testExtraction(testCase);
	console.log();
});
