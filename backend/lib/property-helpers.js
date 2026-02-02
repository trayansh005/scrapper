// Property helper functions for scrapers

// Keywords to identify sold properties
const SOLD_KEYWORDS = [
	"sold subject to contract",
	"sold stc",
	"sold",
	"under offer",
	"let agreed",
	"let",
	"withdrawn",
	"off market",
];

/**
 * Check if property is sold based on text content
 * @param {string} text - Text to check for sold keywords
 * @returns {boolean} - True if property appears to be sold
 */
function isSoldProperty(text) {
	const lowerText = text.toLowerCase();
	return SOLD_KEYWORDS.some((keyword) => lowerText.includes(keyword));
}

/**
 * Extract coordinates from HTML content
 * @param {string} html - HTML content to parse
 * @returns {Object} - Object with latitude and longitude
 */
async function extractCoordinatesFromHTML(html) {
	let latitude = null;
	let longitude = null;

	try {
		// Try Google Maps directions link pattern first (most common for Chestertons)
		const googleMapsDirMatch = html.match(/google\.com\/maps\/dir\/\/([\d.-]+),([\d.-]+)/);
		const mapsMatch = html.match(/ll=([\d.-]+),([\d.-]+)/);
		const scriptMatch = html.match(/lat:\s*"?([\d.-]+)"?,\s*lng:\s*"?([\d.-]+)"?/);
		const jsonMatch = html.match(
			/"latitude"\s*:\s*"?([\d.-]+)"?,\s*"longitude"\s*:\s*"?([\d.-]+)"?/,
		);
		const latLngMatch = html.match(/"lat"\s*:\s*"?([\d.-]+)"?,\s*"lng"\s*:\s*"?([\d.-]+)"?/);
		const dataAttrMatch = html.match(
			/data-(?:lat|latitude)="([\d.-]+)"[\s\S]*?data-(?:lng|longitude)="([\d.-]+)"/,
		);
		const atMatch = html.match(/@([0-9.-]+),([0-9.-]+),\d+z/);
		const latCommentMatch = html.match(/<!--property-latitude:["']([0-9.-]+)["']-->/);
		const lngCommentMatch = html.match(/<!--property-longitude:["']([0-9.-]+)["']-->/);

		if (googleMapsDirMatch) {
			latitude = parseFloat(googleMapsDirMatch[1]);
			longitude = parseFloat(googleMapsDirMatch[2]);
		} else if (mapsMatch) {
			latitude = parseFloat(mapsMatch[1]);
			longitude = parseFloat(mapsMatch[2]);
		} else if (scriptMatch) {
			latitude = parseFloat(scriptMatch[1]);
			longitude = parseFloat(scriptMatch[2]);
		} else if (jsonMatch) {
			latitude = parseFloat(jsonMatch[1]);
			longitude = parseFloat(jsonMatch[2]);
		} else if (latLngMatch) {
			latitude = parseFloat(latLngMatch[1]);
			longitude = parseFloat(latLngMatch[2]);
		} else if (dataAttrMatch) {
			latitude = parseFloat(dataAttrMatch[1]);
			longitude = parseFloat(dataAttrMatch[2]);
		} else if (atMatch) {
			latitude = parseFloat(atMatch[1]);
			longitude = parseFloat(atMatch[2]);
		} else if (latCommentMatch && lngCommentMatch) {
			latitude = parseFloat(latCommentMatch[1]);
			longitude = parseFloat(lngCommentMatch[1]);
		}
	} catch (error) {
		console.error("Error extracting coordinates:", error.message);
	}

	return { latitude, longitude };
}

module.exports = {
	SOLD_KEYWORDS,
	isSoldProperty,
	extractCoordinatesFromHTML,
};
