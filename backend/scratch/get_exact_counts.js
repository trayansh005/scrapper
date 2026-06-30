const https = require('https');
const cheerio = require('cheerio');

const urls = [
    'https://www.guildproperty.co.uk/property-for-sale/hertfordshire/wd7-7hu/3135872',
    'https://www.guildproperty.co.uk/property-for-sale/london-aldgate/e1-8de/4432417',
    'https://www.guildproperty.co.uk/property-for-sale/ingatestone-ingatestone/cm4-0pd/4897879'
];

function fetchUrl(url) {
    return new Promise((resolve) => {
        https.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
        }, (res) => {
            let data = '';
            res.on('data', (chunk) => { data += chunk; });
            res.on('end', () => {
                const $ = cheerio.load(data);

                // Print all details selectors found in class details / icons / lists
                const bed = $('i.fa-bed').parent().text().trim() || $('.fa-bed').parent().text().trim();
                const bath = $('i.fa-bath').parent().text().trim() || $('.fa-bath').parent().text().trim();
                let reception = '';
                $('img').each((i, img) => {
                    const src = $(img).attr('src');
                    if (src && src.includes('reception')) {
                        reception = $(img).parent().text().trim();
                    }
                });

                const title = $('title').text().trim();

                console.log(`URL: ${url}`);
                console.log(`Title: ${title}`);
                console.log(`Parsed Bed feature: "${bed}"`);
                console.log(`Parsed Bath feature: "${bath}"`);
                console.log(`Parsed Reception feature: "${reception}"`);
                resolve();
            });
        }).on('error', (err) => {
            console.error('Error fetching', url, err.message);
            resolve();
        });
    });
}

(async () => {
    for (const url of urls) {
        await fetchUrl(url);
        console.log('------------------------------------');
    }
})();
