const cheerio = require('cheerio');
const axios = require('axios');

async function testDetail() {
    const url = 'https://www.parkersproperties.co.uk/properties-for-sale/6-bedrooms-house-in-cold-ash-thatcham-berkshire-rg18/pap250287/';
    const response = await axios.get(url, {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
        }
    });
    const $ = cheerio.load(response.data);
    
    const schemaJson = $("script.tpj-schema-graph").html();
    console.log("Schema JSON found:", !!schemaJson);
    
    if (schemaJson) {
        const data = JSON.parse(schemaJson);
        const graph = data["@graph"];
        const listing = Array.isArray(graph) ? graph.find(item => item["@type"] === "RealEstateListing") : graph;
        
        if (listing && listing.contentLocation && listing.contentLocation.geo) {
            console.log("Latitude:", listing.contentLocation.geo.latitude);
            console.log("Longitude:", listing.contentLocation.geo.longitude);
        } else {
            console.log("Geo data not found in schema");
            console.log("Listing keys:", listing ? Object.keys(listing) : "null");
        }
    }
}

testDetail().catch(console.error);
