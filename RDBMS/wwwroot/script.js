let currentDb = 'default';

// --- Navigation ---
function showSection(id) {
    document.querySelectorAll('.section').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.tab-item').forEach(el => el.classList.remove('active'));
    document.getElementById(id).classList.add('active');
    
    // Highlight nav
    const navIndex = ['home', 'tutorial', 'console', 'debug'].indexOf(id);
    if(navIndex >= 0) document.querySelectorAll('.tab-item')[navIndex].classList.add('active');
}

// --- Database Management ---
async function fetchDbs() {
    const res = await fetch('/api/dbs');
    const dbs = await res.json();
    const list = document.getElementById('dbList');
    list.innerHTML = '';
    dbs.forEach(db => {
        const div = document.createElement('div');
        div.className = `db-item ${db === currentDb ? 'active' : ''}`;
        div.textContent = db;
        div.onclick = () => switchDb(db);
        list.appendChild(div);
    });
}

async function createNewDb() {
    const name = prompt("Enter new database name:");
    if (!name) return;
    const res = await fetch(`/api/dbs/${name}`, { method: 'POST' });
    if (res.ok) {
        await fetchDbs();
        switchDb(name);
        alert("Database created!");
    } else {
        alert("Error creating database (might already exist).");
    }
}

function switchDb(name) {
    currentDb = name;
    document.getElementById('consoleTitle').textContent = `Console (${name})`;
    document.getElementById('debugTitle').textContent = `Table View (${name})`;
    document.getElementById('output').innerText += `\n[Switched to ${name}]\n`;
    fetchDbs(); 
    refreshTables();
}

// --- Console ---
document.addEventListener('DOMContentLoaded', () => {
    const cmdInput = document.getElementById('cmdInput');
    if (cmdInput) {
        cmdInput.addEventListener('keypress', async function (e) {
            if (e.key === 'Enter') {
                const sql = this.value;
                if (!sql.trim()) return;
                
                // Print Command
                const out = document.getElementById('output');
                out.innerText += `\nSQL> ${sql}\n`;
                this.value = '';

                // Execute
                try {
                    const res = await fetch(`/api/query?db=${currentDb}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(sql)
                    });
                    const text = await res.json();
                    out.innerText += text + "\n";

                    // Refresh table list if command was successful (could be CREATE/DROP)
                    if(res.ok) refreshTables(); 

                } catch (err) {
                    out.innerText += "Error connecting to server.\n";
                }
                
                // Scroll to bottom
                out.scrollTop = out.scrollHeight;
            }
        });
    }

    // Init
    fetchDbs();
    refreshTables();
});

// --- Table View ---

async function refreshTables() {
    const container = document.getElementById('tableList');
    if (!container) return;

    try {
        const res = await fetch(`/api/dbs/${currentDb}/tables`);
        if(res.ok) {
            const tables = await res.json();
            container.innerHTML = '';
            if(tables.length === 0) {
                container.innerHTML = '<span style="color: #64748b;">No tables found.</span>';
                return;
            }
            tables.forEach(t => {
                const btn = document.createElement('button');
                btn.className = 'btn btn-outline';
                btn.style = 'font-size: 0.8rem; padding: 5px 10px; margin-right: 5px; margin-bottom: 5px;';
                btn.textContent = t;
                btn.onclick = () => loadTable(t);
                container.appendChild(btn);
            });
        }
    } catch (err) {
        console.error("Failed to fetch tables", err);
    }
}

async function loadTable(tableName) {
    const container = document.getElementById('tableContainer');
    container.innerHTML = 'Loading...';
    
    try {
        const res = await fetch(`/api/dbs/${currentDb}/tables/${tableName}`);
        if (!res.ok) {
            container.innerHTML = '<span style="color:red">Table not found or empty.</span>';
            return;
        }
        const data = await res.json();
        
        if (data.length === 0) {
            container.innerHTML = 'Table is empty.';
            return;
        }

        const cols = Object.keys(data[0]);
        let html = '<table><thead><tr>';
        cols.forEach(c => html += `<th>${c}</th>`);
        html += '</tr></thead><tbody>';
        
        data.forEach(row => {
            html += '<tr>';
            cols.forEach(c => html += `<td>${row[c]}</td>`);
            html += '</tr>';
        });
        html += '</tbody></table>';
        container.innerHTML = html;

    } catch (err) {
        container.innerHTML = `Error loading table: ${err}`;
    }
}