// Fetch list of part files using directory listing
async function listPartFiles(folderUrl) {
    const response = await fetch(folderUrl);
    if (!response.ok) throw new Error(`Cannot list folder: ${folderUrl}`);
    const html = await response.text();

    // Parse hrefs for part-* files
    const parser = new DOMParser();
    const doc = parser.parseFromString(html, 'text/html');
    const links = Array.from(doc.querySelectorAll('a'))
        .map(a => a.getAttribute('href'))
        .filter(href => href.startsWith('part-'));
    return links;
}

// Load CSV from a single file
async function loadCSVFile(url) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`HTTP error! status: ${response.status} for ${url}`);
    const text = await response.text();
    return text.trim().split('\n').map(line => line.split(','));
}

// Load all CSV parts in a folder and merge
async function loadCSVFolder(folderUrl) {
    const partFiles = await listPartFiles(folderUrl);
    let allData = [];
    for (const file of partFiles) {
        const rows = await loadCSVFile(folderUrl + file);
        allData = allData.concat(rows);
    }
    return allData;
}

// Render a table given the table id and CSV data
function renderTable(tableId, data) {
    const tbody = document.querySelector(`#${tableId} tbody`);
    tbody.innerHTML = '';
    data.forEach(row => {
        const tr = document.createElement('tr');
        row.forEach(cell => {
            const td = document.createElement('td');
            td.textContent = cell;
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
}

// Load and render CSVs
async function init() {
    try {
        const topRestaurants = await loadCSVFolder('/data/top_3_restaurants/');
        renderTable('top-restaurants', topRestaurants);

        const avgProcessing = await loadCSVFolder('/data/avg_processing_time/');
        renderTable('avg-processing', avgProcessing);
    } catch (err) {
        console.error('Erreur lors du chargement des fichiers CSV:', err);
    }
}

init();
