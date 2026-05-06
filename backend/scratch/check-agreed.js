const axios = require('axios');
const fs = require('fs');

async function check() {
    try {
        const response = await axios.get('https://www.simonbrien.com/search/815078');
        const html = response.data;
        const matches = html.match(/<input[^>]+type="checkbox"[^>]+>/gi);
        console.log('Checkboxes found:', matches ? matches.length : 0);
        if (matches) {
            matches.forEach(m => {
                if (m.toLowerCase().includes('agreed')) {
                    console.log('Agreed checkbox:', m);
                }
            });
        }
        
        // Also search for "agreed" in the whole text to see how it's handled
        const index = html.toLowerCase().indexOf('agreed');
        if (index !== -1) {
            console.log('Context around "agreed":', html.substring(index - 100, index + 100));
        }
    } catch (e) {
        console.error(e.message);
    }
}

check();
