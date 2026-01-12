// Quick test for formatPrice logic
function formatPrice(price) {
	if (!price && price !== 0) return "N/A";
	return "£" + Number(price).toLocaleString("en-GB");
}

console.log(formatPrice(123456)); // Expected: £123,456
console.log(formatPrice("123456")); // Expected: £123,456
console.log(formatPrice(1234567.89)); // Expected: £1,234,567.89
console.log(formatPrice(null)); // Expected: N/A
console.log(formatPrice(0)); // Expected: £0

// If format needed for input like '£123,456' -> strip and format
function formatPriceFromText(text) {
	if (!text) return "N/A";
	const match = String(text).match(/\d+[\d,]*(?:\.\d+)?/);
	if (!match) return "N/A";
	const numeric = Number(match[0].replace(/,/g, ""));
	return formatPrice(numeric);
}

console.log(formatPriceFromText("£123,456")); // Expected: £123,456
console.log(formatPriceFromText("123456")); // Expected: £123,456

console.log("Format price tests completed.");
