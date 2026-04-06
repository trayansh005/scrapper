const { extractCoordinatesFromHTML } = require('./lib/property-helpers.js');
const https = require('https');
const fs = require('fs');

const url = 'https://robsonsweb.com/property/high-road-ickenham/';

https.get(url, (res) => {
    let data = '';
    res.on('data', chunk => data += chunk);
    res.on('end', async () => {
        fs.writeFileSync('debug_property.html', data);
        console.log("HTML length:", data.length);
        
        // Search for any numbers that look like lat/lng in the area of Robsons (London/Middlesex)
        // Lat should be around 51.x, Lng around -0.x
        const latRegex = /51\.\d{4,}/g;
        const lngRegex = /-0\.\d{4,}/g;
        
        const lats = data.match(latRegex);
        const lngs = data.match(lngRegex);
        
        console.log("Found possible latitudes:", lats);
        console.log("Found possible longitudes:", lngs);
        
        // Check for common property hive map data
        const phData = data.match(/ph_map_data/i);
        console.log("ph_map_data found?", !!phData);
        
        // Search for "map" or "location" strings
        const mapContext = data.match(/.{0,50}map.{0,50}/gi);
        // console.log("Map context snippets:", mapContext ? mapContext.slice(0, 5) : "None");
    });
});
