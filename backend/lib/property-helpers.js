// Property helper functions for scrapers

// Keywords to identify sold properties
const SOLD_KEYWORDS = [
	"sold subject to contract",
	"sold stc",
	"sold",
	"under offer",
	"let agreed",
	"let stc",
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

	if (!html || typeof html !== "string") {
		return { latitude, longitude };
	}

	try {
		// Try eapowmapoptions pattern (Map Estate Agents): var eapowmapoptions = { lat: "50.2063...", lon: "-5.4937...", ...}
		const eapowMatch = html.match(
			/eapowmapoptions\s*=\s*\{[^}]*?lat:\s*['"]([0-9.-]+)['"][^}]*?lon:\s*['"]([0-9.-]+)['"]/,
		);
		if (eapowMatch) {
			latitude = parseFloat(eapowMatch[1]);
			longitude = parseFloat(eapowMatch[2]);
			return { latitude, longitude };
		}

		// Try Ashtons / Locratingplugin pattern: loadLocratingPlugin({..., lat: '51.888', lng: '-0.3312', ...})
		const asbtonsMatch = html.match(
			/loadLocratingPlugin\(\{[^}]*?lat:\s*['"]([0-9.-]+)['"][^}]*?lng:\s*['"]([0-9.-]+)['"]/,
		);
		if (asbtonsMatch) {
			latitude = parseFloat(asbtonsMatch[1]);
			longitude = parseFloat(asbtonsMatch[2]);
			return { latitude, longitude };
		}

		// Try Rodgers Estates / Google Maps embed in ShowMap function: ShowMap(...q=51.6122665405273440%2C-0.5506179332733154")
		const rodgersMatch = html.match(/[&?]q=([0-9.-]+)%2C([0-9.-]+)/);
		if (rodgersMatch) {
			latitude = parseFloat(rodgersMatch[1]);
			longitude = parseFloat(rodgersMatch[2]);
			return { latitude, longitude };
		}

		// Try Homeflow property data (Hamptons): Homeflow.set('property', ... {"lat":..,"lng":..})
		// Flexible enough for both 'property' and 'properties'
		const homeflowMatch = html.match(
			/Homeflow\.set\(['"]propert(?:y|ies)['"][\s\S]*?\\?"?lat\\?"?\s*:\s*([0-9.-]+)[\s\S]*?\\?"?lng\\?"?\s*:\s*([0-9.-]+)/,
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
		const expertAgentLatMatch = html.match(/id="hdnLatitude"\s+value="([\d.-]+)"/);
		const expertAgentLonMatch = html.match(/id="hdnLongitude"\s+value="([\d.-]+)"/);
		const latCommentMatch = html.match(/<!--property-latitude:["']([0-9.-]+)["']-->/);
		const lngCommentMatch = html.match(/<!--property-longitude:["']([0-9.-]+)["']-->/);
		// Additional pattern for Sequence Home style comments
		const latCommentMatch2 = html.match(/property-latitude:"([0-9.-]+)"/);
		const lngCommentMatch2 = html.match(/property-longitude:"([0-9.-]+)"/);

		// Try Locrating / Douglas Allen iframe pattern: lat=51.67161&lng=0.11565
		const locratingMatch = html.match(/lat=([0-9.-]+)[\s\S]*?lng=([0-9.-]+)/);

		// Try Able Estates / Acquaint CRM iframe location pattern: location=51.509033,0.130544
		const locationParamMatch = html.match(/location=([0-9.-]+),([0-9.-]+)/);

		// Try Google Maps Street View pattern (Haart): cbll=51.452255,-0.068368
		const googleMapsStreetViewMatch = html.match(/cbll=([0-9.-]+),([0-9.-]+)/);

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
			} catch (e) { }
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
		} else if (expertAgentLatMatch && expertAgentLonMatch) {
			latitude = parseFloat(expertAgentLatMatch[1]);
			longitude = parseFloat(expertAgentLonMatch[1]);
		} else if (atMatch) {
			latitude = parseFloat(atMatch[1]);
			longitude = parseFloat(atMatch[2]);
		} else if (latCommentMatch && lngCommentMatch) {
			latitude = parseFloat(latCommentMatch[1]);
			longitude = parseFloat(lngCommentMatch[1]);
		} else if (latCommentMatch2 && lngCommentMatch2) {
			latitude = parseFloat(latCommentMatch2[1]);
			longitude = parseFloat(lngCommentMatch2[1]);
		} else if (locratingMatch) {
			latitude = parseFloat(locratingMatch[1]);
			longitude = parseFloat(locratingMatch[2]);
		} else if (locationParamMatch) {
			latitude = parseFloat(locationParamMatch[1]);
			longitude = parseFloat(locationParamMatch[2]);
		} else if (googleMapsStreetViewMatch) {
			latitude = parseFloat(googleMapsStreetViewMatch[1]);
			longitude = parseFloat(googleMapsStreetViewMatch[2]);
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
	if (value === null || value === undefined || value === "") return null;

	// If it's already a number, format it directly
	if (typeof value === "number") {
		// Round to nearest integer if needed (database usually stores as int)
		return Math.round(value)
			.toString()
			.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
	}

	const text = value.toString();
	const match = text.match(/£?\s*[\d,]+(\.\d+)?/);
	if (!match) return null;

	// Remove currency, spaces and commas to get raw digits
	const cleaned = match[0].replace(/[£\s,]/g, "");

	// Parse as float to handle decimals, then format as integer string with commas
	const num = parseFloat(cleaned);
	if (isNaN(num)) return null;

	return Math.round(num)
		.toString()
		.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/**
 * Formats a price for display with currency symbol and rental suffix
 * @param {number|string} price - The price value
 * @param {boolean} isRental - Whether it's a rental property
 * @returns {string} - Formatted display string
 */
function formatPriceDisplay(price, isRental) {
	const formatted = formatPriceUk(price);
	if (!formatted) return isRental ? "£0 pcm" : "£0";
	return `£${formatted}${isRental ? " pcm" : ""}`;
}

/**
 * Extract bedroom count from text or HTML
 * @param {string} text - Text to extract from
 * @returns {number|null} - Number of bedrooms or null
 */
function extractBedroomsFromHTML(text) {
	if (!text) return null;

	const bedroomWords = {
		one: 1,
		two: 2,
		three: 3,
		four: 4,
		five: 5,
		six: 6,
		seven: 7,
		eight: 8,
		nine: 9,
		ten: 10,
	};

	// 1. Try numeric match: "3 bedroom", "3 bed"
	const numMatch = text.match(/(\d+)\s*(?:bedrooms?|beds?|bdrms?)/i);
	if (numMatch) return parseInt(numMatch[1], 10);

	// 2. Try word match: "six bedroom", "Six Bed"
	const wordPattern = new RegExp(
		`\\b(${Object.keys(bedroomWords).join("|")})\\s*(?:bedrooms?|beds?)`,
		"i",
	);
	const wordMatch = text.match(wordPattern);
	if (wordMatch) return bedroomWords[wordMatch[1].toLowerCase()];

	// 2b. Studio fallback when no numeric/word beds exist
	if (/\bstudio\b/i.test(text)) return 0;

	// 3. Fallback for "3 & 3 bathrooms" type strings
	const combinedMatch = text.match(/(\d+)\s*bedrooms?\s*(?:&|\+)\s*\d+\s*bathrooms?/i);
	if (combinedMatch) return parseInt(combinedMatch[1], 10);

	return null;
}

module.exports = {
	SOLD_KEYWORDS,
	isSoldProperty,
	parsePrice,
	formatPriceUk,
	formatPriceDisplay,
	extractCoordinatesFromHTML,
	extractBedroomsFromHTML,
};
