const fs = require('fs');
const path = require('path');

// 1. Read the saved script containing locations
const content = fs.readFileSync('C:\\Users\\tanus\\AppData\\Local\\Temp\\script_1.js', 'utf8');

// Find the var locations = [...] snippet
const match = content.match(/var\s+locations\s*=\s*([\s\S]*?);\s*\n/);
if (!match) {
    console.error('FAIL: locations variable not found in cached script_1.js');
    process.exit(1);
}

let locations;
try {
    eval('locations = ' + match[1]);
} catch (e) {
    console.error('FAIL: Error parsing locations array:', e.message);
    process.exit(1);
}

console.log(`Loaded ${locations.length} properties from map search data.`);

// 2. Define test cases that we manually verified via property page content:
// - ID 3135872: Address: Harper Lane, Radlett, Hertfordshire, WD7 (9 bedrooms)
// - ID 4432417: Address: Alie St, Aldgate, London, E1 (31 bedrooms)
// - ID 4897879: Address: Blanket Hall & Coach House, Beggar Hill (6 bedrooms)
const verifiedTestCases = {
    3135872: { title: "Harper Lane, Radlett, Hertfordshire, WD7", expectedBedrooms: 9 },
    4432417: { title: "Alie St, Aldgate, London, E1", expectedBedrooms: 31 },
    4897879: { title: "Blanket Hall & Coach House, Beggar Hill", expectedBedrooms: 6 }
};

let passedCases = 0;
let totalCases = Object.keys(verifiedTestCases).length;

for (const loc of locations) {
    // 3. Apply the extraction logic from scraper-agent-35.js:
    const relativeUrl = loc[9];
    const idMatch = relativeUrl.match(/\/(\d+)$/);
    if (!idMatch) continue;
    const propertyId = parseInt(idMatch[1]);

    if (verifiedTestCases[propertyId]) {
        const expected = verifiedTestCases[propertyId].expectedBedrooms;

        // This is the index under test (index 6 after the fix)
        const extractedBedrooms = loc[6];

        console.log(`Property ID: ${propertyId}`);
        console.log(`  Address: ${loc[4]}`);
        console.log(`  Expected Bedrooms: ${expected}`);
        console.log(`  Extracted (data[6]): ${extractedBedrooms}`);

        if (extractedBedrooms === expected) {
            console.log(`  => PASS ✅`);
            passedCases++;
        } else {
            console.log(`  => FAIL ❌ (Old index 5 was ${loc[5]})`);
        }
    }
}

if (passedCases === totalCases) {
    console.log(`\n🎉 SUCCESS! All ${passedCases}/${totalCases} verified properties had their bedrooms correctly extracted.`);
    process.exit(0);
} else {
    console.error(`\n❌ ERROR: Only ${passedCases}/${totalCases} properties matched the expected values.`);
    process.exit(1);
}
