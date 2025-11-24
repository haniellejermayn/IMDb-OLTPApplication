const API_URL = 'http://localhost:5000';

// Add log entry
function addLog(message, type = 'info') {
    const logPanel = document.getElementById('log-panel');
    const timestamp = new Date().toLocaleTimeString();
    const logEntry = document.createElement('div');
    logEntry.className = `log-entry log-${type}`;
    logEntry.textContent = `[${timestamp}] ${message}`;
    logPanel.appendChild(logEntry);
    logPanel.scrollTop = logPanel.scrollHeight;
}

// Check node status
async function checkNodeStatus() {
    try {
        const response = await fetch(`${API_URL}/health`);
        const data = await response.json();
        
        const statusDiv = document.getElementById('node-status');
        statusDiv.innerHTML = '';
        
        for (const [node, status] of Object.entries(data)) {
            const span = document.createElement('span');
            span.className = `node-status ${status.healthy ? 'node-online' : 'node-offline'}`;
            span.textContent = `${node}: ${status.status}`;
            statusDiv.appendChild(span);
        }
        
        addLog('Node status check completed', 'success');
    } catch (error) {
        addLog(`Error checking node status: ${error.message}`, 'error');
    }
}

// Load titles
async function loadTitles() {
    try {
        addLog('Fetching titles from Node 1...', 'info');
        const response = await fetch(`${API_URL}/titles?limit=20`);
        const data = await response.json();
        
        const tbody = document.getElementById('titles-body');
        tbody.innerHTML = '';
        
        if (data.data && data.data.length > 0) {
            data.data.forEach(title => {
                const row = document.createElement('tr');
                row.innerHTML = `
                    <td>${title.tconst}</td>
                    <td>${title.title_type}</td>
                    <td>${title.primary_title}</td>
                    <td>${title.start_year}</td>
                    <td>${title.runtime_minutes} min</td>
                    <td>${title.genres}</td>
                `;
                tbody.appendChild(row);
            });
            addLog(`Loaded ${data.data.length} titles successfully`, 'success');
        } else {
            tbody.innerHTML = '<tr><td colspan="6" style="text-align: center;">No data found</td></tr>';
            addLog('No titles found', 'info');
        }
    } catch (error) {
        addLog(`Error loading titles: ${error.message}`, 'error');
    }
}

// Test concurrent read
async function testConcurrentRead() {
    try {
        addLog('Starting concurrent read test...', 'info');
        const response = await fetch(`${API_URL}/simulate/concurrent-read`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tconst: 'tt0000001' })
        });
        const data = await response.json();
        addLog(`Concurrent read test completed: ${JSON.stringify(data)}`, 'success');
    } catch (error) {
        addLog(`Error in concurrent read test: ${error.message}`, 'error');
    }
}

// Test concurrent write
async function testConcurrentWrite() {
    addLog('Concurrent write test - to be implemented', 'info');
}

// Simulate failure
async function simulateFailure() {
    addLog('Node failure simulation - to be implemented', 'info');
}

// View logs
async function viewLogs() {
    try {
        addLog('Fetching transaction logs...', 'info');
        const response = await fetch(`${API_URL}/logs`);
        const data = await response.json();
        
        if (data.logs && data.logs.length > 0) {
            addLog('=== Recent Transaction Logs ===', 'info');
            data.logs.forEach(log => {
                addLog(`[${log.node_id}] ${log.operation} ${log.tconst} - ${log.status}`, 
                       log.status === 'SUCCESS' ? 'success' : 'error');
            });
        } else {
            addLog('No transaction logs found', 'info');
        }
    } catch (error) {
        addLog(`Error fetching logs: ${error.message}`, 'error');
    }
}

// Initialize on page load
window.addEventListener('load', () => {
    addLog('Frontend initialized', 'success');
    checkNodeStatus();
    setInterval(checkNodeStatus, 10000); // Check status every 10 seconds
});