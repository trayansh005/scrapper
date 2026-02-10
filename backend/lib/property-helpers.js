// Property helper functions for scrapers

// Keywords to identify sold properties
const SOLD_KEYWORDS = [
	"sold subject to contract",
	"sold stc",
	"sold",
	"under offer",
	"let agreed",
	"sale agreed",
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
 * Parse a price string into a number
 * @param {string} priceText - The price string to parse
 * @returns {number|null} - The parsed price as a number, or null if invalid
 */
function parsePrice(priceText) {
	if (!priceText) return null;

	// Remove secondary prices in brackets
	let cleanText = priceText.split("(")[0];

	// Extract everything that looks like a price (currency symbol followed by digits/commas)
	const match = cleanText.match(/[£$€]?\s*[\d,]+(\.\d+)?/);
	if (!match) return null;

	const numericPart = match[0].replace(/[£$€\s,]/g, "");
	const price = parseFloat(numericPart);

	return isNaN(price) ? null : price;
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
		// Try Homeflow properties data (Hamptons): Homeflow.set('properties', ... {"properties":[{"lat":..,"lng":..}]})
		const homeflowMatch = html.match(
			/Homeflow\.set\(['"]properties['"][\s\S]*?\\?"lat\\?"\s*:\s*([0-9.-]+)[\s\S]*?\\?"lng\\?"\s*:\s*([0-9.-]+)/,
		);
		if (homeflowMatch) {
			latitude = parseFloat(homeflowMatch[1]);
			longitude = parseFloat(homeflowMatch[2]);
			return { latitude, longitude };
		}

		// Try Snellers pattern first: data-lat and data-lng attributes
		const snellersMatch = html.match(/data-lat="([0-9.-]+)"[\s\S]*?data-lng="([0-9.-]+)"/);
		if (snellersMatch) {
			latitude = parseFloat(snellersMatch[1]);
			longitude = parseFloat(snellersMatch[2]);
			return { latitude, longitude };
		}

		// Try Moveli pattern: const location = { lat: 51.5728027, lng: -0.1638948}
		const moveliMatch = html.match(
			/const location = \{\s*lat:\s*([0-9.-]+),\s*lng:\s*([0-9.-]+)\s*\}/,
		);
		if (moveliMatch) {
			latitude = parseFloat(moveliMatch[1]);
			longitude = parseFloat(moveliMatch[2]);
			return { latitude, longitude };
		}

		// Try Google Maps directions link pattern first (most common for Chestertons)
		const googleMapsDirMatch = html.match(/google\.com\/maps\/dir\/\/([\d.-]+),([\d.-]+)/);
		const googleMapsQMatch = html.match(/[?&]q=([\d.-]+),([\d.-]+)/);
		const googleMapsCenterMatch = html.match(/[?&]center=([\d.-]+),([\d.-]+)/);
		const mapsMatch = html.match(/ll=([\d.-]+),([\d.-]+)/);
		const scriptMatch = html.match(/lat:\s*"?([\d.-]+)"?,\s*lng:\s*"?([\d.-]+)"?/);
		// Match both escaped and unescaped quotes for latitude/longitude in JSON
		const jsonMatch = html.match(
			/\\?"latitude\\?"\s*:\s*"?([\d.-]+)"?,\s*\\?"longitude\\?"\s*:\s*"?([\d.-]+)"?/,
		);
		const latLngMatch = html.match(
			/\\?"lat\\?"\s*:\s*"?([\d.-]+)"?,\s*\\?"lng\\?"\s*:\s*"?([\d.-]+)"?/,
		);
		const dataAttrMatch = html.match(
			/data-(?:lat|latitude)="([\d.-]+)"[\s\S]*?data-(?:lng|longitude)="([\d.-]+)"/,
		);
		const dataLocationMatch = html.match(/data-location="([\d.-]+),([\d.-]+)"/);
		const atMatch = html.match(/@([0-9.-]+),([0-9.-]+),\d+z/);
		const latCommentMatch = html.match(/<!--property-latitude:["']([0-9.-]+)["']-->/);
		const lngCommentMatch = html.match(/<!--property-longitude:["']([0-9.-]+)["']-->/);
		// Additional pattern for Sequence Home style comments
		const latCommentMatch2 = html.match(/property-latitude:"([0-9.-]+)"/);
		const lngCommentMatch2 = html.match(/property-longitude:"([0-9.-]+)"/);

		// Try Nestseekers geo attribute as a fallback in regex
		const geoAttrMatch = html.match(/geo=['"](\{[\s\S]*?\})['"]/);
		if (geoAttrMatch) {
			try {
				const geoJson = geoAttrMatch[1].replace(/&quot;/g, '"');
				const geoData = JSON.parse(geoJson);
				latitude = parseFloat(geoData.lat || geoData.latitude);
				longitude = parseFloat(geoData.lon || geoData.longitude || geoData.lng);
				if (!isNaN(latitude) && !isNaN(longitude)) {
					return { latitude, longitude };
				}
			} catch (e) {}
		}

		if (googleMapsDirMatch) {
			latitude = parseFloat(googleMapsDirMatch[1]);
			longitude = parseFloat(googleMapsDirMatch[2]);
		} else if (googleMapsQMatch) {
			latitude = parseFloat(googleMapsQMatch[1]);
			longitude = parseFloat(googleMapsQMatch[2]);
		} else if (googleMapsCenterMatch) {
			latitude = parseFloat(googleMapsCenterMatch[1]);
			longitude = parseFloat(googleMapsCenterMatch[2]);
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
		} else if (dataLocationMatch) {
			latitude = parseFloat(dataLocationMatch[1]);
			longitude = parseFloat(dataLocationMatch[2]);
		} else if (atMatch) {
			latitude = parseFloat(atMatch[1]);
			longitude = parseFloat(atMatch[2]);
		} else if (latCommentMatch && lngCommentMatch) {
			latitude = parseFloat(latCommentMatch[1]);
			longitude = parseFloat(lngCommentMatch[1]);
		} else if (latCommentMatch2 && lngCommentMatch2) {
			latitude = parseFloat(latCommentMatch2[1]);
			longitude = parseFloat(lngCommentMatch2[1]);
		}
	} catch (error) {
		console.error("Error extracting coordinates:", error.message);
	}

	return { latitude, longitude };
}

/**
 * Formats a price number into a UK formatted string with commas
 * @param {number|string} value - The price to format
 * @returns {string|null} - The formatted price string, or null if invalid
 */
function formatPriceUk(value) {
	if (value === null || value === undefined) return null;
	const digits = value.toString().replace(/[^0-9]/g, "");
	if (!digits) return null;

	return digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

module.exports = {
	SOLD_KEYWORDS,
	isSoldProperty,
	parsePrice,
	formatPriceUk,
	extractCoordinatesFromHTML,
};
