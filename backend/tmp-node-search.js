const fs = require('fs');

const data = fs.readFileSync('debug_property.html', 'utf8');

// Looking for anything that looks like "lat": 51.5..." or "latitude" : "51.5..."
// We'll search for anything containing "lat" followed by something numeric
const latRegex = /lat[^a-zA-Z]{0,10}([0-9.-]{5,20})/gi;
const lngRegex = /lng[^a-zA-Z]{0,10}([0-9.-]{5,20})/gi;
const lonRegex = /lon[^a-zA-Z]{0,10}([0-9.-]{5,20})/gi;

const lats = [];
let m;
while (m = latRegex.exec(data)) {
    lats.push(m[0]);
}

const lngs = [];
while (m = lngRegex.exec(data)) {
    lngs.push(m[0]);
}

const lons = [];
while (m = lonRegex.exec(data)) {
    lons.push(m[0]);
}

console.log("LAT matches:", lats);
console.log("LNG matches:", lngs);
console.log("LON matches:", lons);

// Search for any iframe src
const iframes = data.match(/<iframe[^>]+src="([^"]+)"/gi);
console.log("\nIframes found:", iframes ? iframes.length : 0);
if (iframes) {
    iframes.forEach(it => {
        if (it.includes('lat') || it.includes('lng') || it.includes('map')) {
            console.log("Potential map iframe:", it);
        }
    });
}
