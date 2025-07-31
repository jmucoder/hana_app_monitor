// This is the complete and corrected script.js file

// --- HELPER FUNCTION for executing scripts ---
async function executeHanaScript(script) {
    // ... (This function remains unchanged)
    const resultsCard = document.getElementById('script-results-card');
    const resultsContent = document.getElementById('script-results-content');
    if (!resultsCard || !resultsContent) {
        console.error("Result display elements not found.");
        const response = await fetch('/api/execute-script', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ script: script })
        });
        return await response.json();
    }
    resultsContent.innerHTML = `<div class="spinner-border spinner-border-sm" role="status"></div> Executing...`;
    resultsCard.style.display = 'block';
    const response = await fetch('/api/execute-script', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({ script: script })
    });
    const result = await response.json();
    if (result.success) {
        if (result.rows) {
            let table = '<div class="table-responsive"><table class="table table-sm table-bordered"><thead><tr>';
            result.columns.forEach(col => table += `<th>${col}</th>`);
            table += '</tr></thead><tbody>';
            result.rows.forEach(row => {
                table += '<tr>';
                result.columns.forEach(col => table += `<td>${row[col]}</td>`);
                table += '</tr>';
            });
            table += '</tbody></table></div>';
            resultsContent.innerHTML = table;
        } else {
            resultsContent.innerHTML = `<div class="alert alert-success mb-0">${result.message}</div>`;
        }
    } else {
        resultsContent.innerHTML = `<div class="alert alert-danger mb-0">${result.error}</div>`;
    }
    return result;
}


document.addEventListener('DOMContentLoaded', function() {

    // --- LIVE DASHBOARD PAGE LOGIC (RESILIENT VERSION) ---
    if (document.getElementById('cpu-usage-bar')) { 
        
        function updateDashboardUI(data) {
            let errorBox = document.getElementById('error-alert-box');
            if (!errorBox) {
                 errorBox = document.createElement('div');
                 errorBox.id = 'error-alert-box';
                 errorBox.className = 'alert alert-danger';
                 errorBox.style.display = 'none';
                 document.querySelector('h1.mb-4').insertAdjacentElement('afterend', errorBox);
            }
            if (data.error) {
                errorBox.style.display = 'block';
                errorBox.textContent = `Database Error: ${data.error}`;
                return;
            }
            errorBox.style.display = 'none';
    
            // Update CPU, handling potential errors
            const cpuText = document.getElementById('cpu-usage-text');
            const cpuBar = document.getElementById('cpu-usage-bar');
            if (data.cpu_usage === 'Error' || data.cpu_usage === 'N/A') {
                cpuText.textContent = 'Error';
                cpuBar.style.width = '100%';
                cpuBar.classList.add('bg-danger');
                cpuBar.textContent = 'Error';
            } else {
                cpuText.textContent = `${data.cpu_usage}%`;
                cpuBar.style.width = `${data.cpu_usage}%`;
                cpuBar.classList.remove('bg-danger');
                cpuBar.textContent = `${data.cpu_usage}%`;
            }
    
            // Update Memory, handling potential errors
            const memText = document.getElementById('memory-usage-text');
            const memBar = document.getElementById('memory-usage-bar');
            if (data.memory_usage.used === 'Error' || data.memory_usage.used === 'N/A') {
                 memText.textContent = 'Error';
                 memBar.style.width = '100%';
                 memBar.classList.add('bg-danger');
                 memBar.textContent = 'Error';
            } else {
                const memUsed = data.memory_usage.used;
                const memTotal = data.memory_usage.total;
                const memPercent = memTotal > 0 ? (memUsed / memTotal) * 100 : 0;
                memText.textContent = `${memUsed}GB / ${memTotal}GB`;
                memBar.style.width = `${memPercent}%`;
                memBar.classList.remove('bg-danger');
                memBar.textContent = `${memUsed}GB`;
            }
    
            // Update Sessions, handling potential errors
            const sessionsText = document.getElementById('active-sessions-text');
            if (data.active_sessions === 'Error' || data.active_sessions === 'N/A') {
                sessionsText.textContent = 'X';
            } else {
                sessionsText.textContent = data.active_sessions;
            }
        }
    
        async function fetchDashboardData() {
            try {
                const response = await fetch('/api/kpis');
                const data = await response.json();
                updateDashboardUI(data);
            } catch (error) {
                console.error('Error fetching dashboard data:', error);
            }
        }
    
        setInterval(fetchDashboardData, 5000);
        fetchDashboardData();
    }

    // --- SCRIPT GENERATOR PAGE LOGIC ---
    const executeBtn = document.getElementById('execute-script-btn');
    if (executeBtn) {
        executeBtn.addEventListener('click', () => {
            const script = document.getElementById('generatedScript').value;
            executeHanaScript(script);
        });
    }

    // --- ACTIVE SESSIONS PAGE LOGIC (CORRECTED) ---
    const sessionsTableBody = document.getElementById('sessions-table-body');
    if (sessionsTableBody) {
        async function loadSessions() {
            const response = await fetch('/api/sessions');
            const data = await response.json();
            sessionsTableBody.innerHTML = '';
            if (data.error) {
                sessionsTableBody.innerHTML = `<tr><td colspan="4" class="text-center text-danger">${data.error}</td></tr>`;
                return;
            }
            if(data.length === 0) {
                sessionsTableBody.innerHTML = `<tr><td colspan="4" class="text-center text-muted">No active sessions found.</td></tr>`;
                return;
            }
            data.forEach(s => {
                sessionsTableBody.innerHTML += `
                    <tr>
                        <td>${s.CONNECTION_ID}</td>
                        <td>${s.CLIENT_HOST || 'N/A'} (${s.CLIENT_IP || 'N/A'})</td>
                        <td><span class="badge bg-secondary">${s.CONNECTION_STATUS}</span></td>
                        <td><button class="btn btn-danger btn-sm" onclick="killSession(${s.CONNECTION_ID})">Kill</button></td>
                    </tr>
                `;
            });
        }
        loadSessions();
        window.loadSessions = loadSessions; // Make it globally accessible for the kill function
    }


    // --- TABLE EXPLORER PAGE ---
    // ... (This section is unchanged)
    const tablesTableBody = document.getElementById('tables-table-body');
    if (tablesTableBody) {
        async function loadTables() {
            const response = await fetch('/api/tables');
            const data = await response.json();
            tablesTableBody.innerHTML = '';
            if (data.error) {
                tablesTableBody.innerHTML = `<tr><td colspan="4" class="text-center text-danger">${data.error}</td></tr>`;
                return;
            }
            data.forEach(t => {
                tablesTableBody.innerHTML += `
                    <tr>
                        <td>${t.SCHEMA_NAME}</td>
                        <td>${t.TABLE_NAME}</td>
                        <td>${t.RECORD_COUNT.toLocaleString()}</td>
                        <td>${t.MEMORY_MB.toLocaleString()}</td>
                    </tr>
                `;
            });
        }
        loadTables();
    }

    // --- HISTORY PAGE ---
    // ... (This section is unchanged)
    const kpiChart = document.getElementById('kpi-history-chart');
    if (kpiChart) {
        async function loadChart() {
            const response = await fetch('/api/historical-kpis');
            const data = await response.json();
            const ctx = kpiChart.getContext('2d');
            new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.labels,
                    datasets: [
                        { label: 'CPU Usage (%)', data: data.cpu, borderColor: 'rgba(75, 192, 192, 1)', yAxisID: 'y' },
                        { label: 'Memory Usage (GB)', data: data.memory, borderColor: 'rgba(153, 102, 255, 1)', yAxisID: 'y1' }
                    ]
                },
                options: { scales: { y: { position: 'left', title: { display: true, text: 'CPU (%)' }}, y1: { position: 'right', title: { display: true, text: 'Memory (GB)' }, grid: { drawOnChartArea: false }}}}
            });
        }
        loadChart();
    }

    // --- HEALTH CHECK PAGE ---
    // ... (This section is unchanged)
    const healthCheckBtn = document.getElementById('run-health-check-btn');
    if (healthCheckBtn) {
        healthCheckBtn.addEventListener('click', async () => {
            const resultsBody = document.getElementById('health-check-results');
            resultsBody.innerHTML = '<tr><td colspan="3" class="text-center"><div class="spinner-border" role="status"></div> Running checks...</td></tr>';
            
            const response = await fetch('/api/health-check');
            const report = await response.json();
            resultsBody.innerHTML = '';
            
            report.forEach(check => {
                let statusClass = 'text-success';
                if (check.status === 'Warning') statusClass = 'text-warning';
                if (check.status === 'Error') statusClass = 'text-danger';
                
                resultsBody.innerHTML += `
                    <tr>
                        <td><strong>${check.name}</strong></td>
                        <td class="${statusClass}"><strong>${check.status}</strong></td>
                        <td><small class="text-muted">${check.details}</small></td>
                    </tr>
                `;
            });
        });
    }
});


// --- GLOBAL HELPER FUNCTIONS ---
// ... (This section is unchanged)
function copyScript() {
    const copyText = document.getElementById("generatedScript");
    copyText.select();
    navigator.clipboard.writeText(copyText.value);
    alert("Copied the script to clipboard!");
}

async function killSession(connectionId) {
    if (confirm(`Are you sure you want to kill session ${connectionId}?`)) {
        const script = `ALTER SYSTEM DISCONNECT SESSION '${connectionId}';`;
        const result = await executeHanaScript(script);
        if(result.success) {
            alert(`Kill command for session ${connectionId} sent successfully!`);
        } else {
            alert(`Failed to kill session: ${result.error}`);
        }
        
        if(window.loadSessions) {
            setTimeout(window.loadSessions, 1000);
        }
    }
}