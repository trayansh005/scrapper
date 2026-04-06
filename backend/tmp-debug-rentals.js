const { extractCoordinatesFromHTML } = require('./lib/property-helpers.js');
const https = require('https');

const urls = [
    'https://robsonsweb.com/property/high-road-ickenham/',
    'https://robsonsweb.com/property/high-street-rickmansworth/',
    'https://robsonsweb.com/property/the-coach-house-rickmansworth/',
    'https://robsonsweb.com/property/eastbury-avenue-northwood/',
    'https://robsonsweb.com/property/glasfryn-court-harrow-on-the-hill/'
];

async function checkUrl(url) {
    return new Promise((resolve) => {
        https.get(url, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', async () => {
                const coords = await extractCoordinatesFromHTML(data);
                console.log(`URL: ${url}`);
                console.log(`  Coords: ${coords.latitude}, ${coords.longitude}`);
                
                if (!coords.latitude) {
                    console.log(`  DEBUG: Searching for common map patterns...`);
                    // Look for Google Maps link
                    const mapLink = data.match(/google\.com\/maps[^"'>\s]+/);
                    if (mapLink) console.log(`    Map Link: ${mapLink[0]}`);
                    
                    // Look for JSON with lat/lng
                    const jsonLat = data.match(/"lat"\s*:\s*"?([0-9.-]+)"?/i);
                    const jsonLng = data.match(/"lng"\s*:\s*"?([0-9.-]+)"?/i);
                    if (jsonLat && jsonLng) console.log(`    Possible JSON: lat=${jsonLat[1]}, lng=${jsonLng[1]}`);

                    const phMapData = data.match(/ph_map_data[^=]*=\s*([^;]+);/i);
                    if (phMapData) console.log(`    ph_map_data found!`);
                }
                resolve();
            });
        }).on('error', (e) => {
            console.error(`Error fetching ${url}: ${e.message}`);
            resolve();
        });
    });
}

(async () => {
    for (const url of urls) {
        await checkUrl(url);
    }
})();
