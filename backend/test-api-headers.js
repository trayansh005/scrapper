const axios = require("axios");

async function testApi() {
    const channel = "sales";
    const apiUrl = `https://www.sequencehome.co.uk/search.ljson?channel=${channel}&fragment=`;

    try {
        const response = await axios.get(apiUrl, {
            headers: {
                'Accept': '*/*',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
                'Referer': 'https://www.sequencehome.co.uk/properties/sales/',
                'X-Requested-With': 'XMLHttpRequest',
                'Cookie': '_ctesius2_session=TW5QRnRLbk53SU9KNGtNellmUWJ5bk5tUjFZdGcxQzAxdnF3TS8xVFNVQlkzNHYxdi9iZHNkeGI4Nmx1eWg5OWdib2dHZWtEQXR3aXhoU1ErMEZnc2IveThib1hNZE5Uandub1VEYjZheHcxbFVVaGQ2eDlKUEpWb0prbVdTQXFyY1FSYU8zRm1qOGlVWEQ3NHJuZ2lnPT0tLVFCdE9aS2kyb2pVazdXcmk5dG5JQlE9PQ%3D%3D--10862576303a98f247a9bea33f1febc7b6650b28;'
            }
        });
        console.log("SUCCESS: Status", response.status);
        console.log("Properties count:", response.data.properties?.length);
    } catch (error) {
        console.log("FAILED: Status", error.response?.status || error.message);
        if (error.response) {
            console.log("Headers:", error.response.headers);
        }
    }
}

testApi();
