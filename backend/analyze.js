const fs = require('fs');
const html = fs.readFileSync('carter_page.html', 'utf8');
const cheerio = require('cheerio');
const $ = cheerio.load(html);

const pagination = $('.pagination, [class*="pagination"], [class*="page"], .page-number');
console.log('Pagination text:', pagination.text().replace(/\s+/g, ' ').trim());

$('script').each((i, el) => {
    const text = $(el).html() || '';
    if(text.includes('__NEXT_DATA__') || text.includes('window.state') || text.includes('totalPages')) {
        console.log('Found script with data length:', text.length);
        
        let match = text.match(/"totalPages"\s*:\s*(\d+)/i);
        if(match) console.log('Total pages match:', match[1]);
        
        match = text.match(/"count"\s*:\s*(\d+)/i);
        if(match) console.log('Count match:', match[1]);
        
        match = text.match(/"total"\s*:\s*(\d+)/i);
        if(match) console.log('Total match:', match[1]);
    }
});

const lastPageLinks = $('a').filter((i, el) => {
    const href = $(el).attr('href');
    if(href && href.includes('page=') && $(el).text().trim().match(/^\d+$/)) {
        console.log('Page link text:', $(el).text().trim(), 'href:', href);
        return true;
    }
    return false;
});
console.log('Last page links count:', lastPageLinks.length);
