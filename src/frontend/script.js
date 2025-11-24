const API_BASE = 'http://localhost:5000';

function addResult(title, data, timestamp = new Date().toISOString()) {
    const resultsDiv = document.getElementById('results');
    
    if (resultsDiv.querySelector('p')) {
        resultsDiv.innerHTML = '';
    }
    
    const entry = document.createElement('div');
    entry.className = 'result-entry';
    entry.innerHTML = `
        <strong>${title}</strong>
        <small> - ${timestamp}</small>
        <pre>${JSON.stringify(data, null, 2)}</pre>
    `;
    
    resultsDiv.insertBefore(entry, resultsDiv.firstChild);
}

async function checkHealth() {
    try {
        const response = await fetch(`${API_BASE}/health`);
        const data = await response.json();
        
        const tbody = document.getElementById('healthBody');
        tbody.innerHTML = '';
        
        for (const [nodeName, nodeInfo] of Object.entries(data)) {
            const row = tbody.insertRow();
            row.insertCell(0).textContent = nodeName;
            row.insertCell(1).textContent = nodeInfo.healthy ? 'ONLINE' : 'OFFLINE';
            row.insertCell(2).textContent = nodeInfo.record_count || 'N/A';
            row.insertCell(3).textContent = nodeInfo.error || '-';
        }
        
        addResult('Health Check', data);
    } catch (error) {
        addResult('Health Check Failed', { error: error.message });
    }
}

async function runCase1() {
    const tconst = document.getElementById('case1Tconst').value;
    const isolation = document.getElementById('case1Isolation').value;
    
    try {
        const response = await fetch(`${API_BASE}/test/concurrent-read`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tconst, isolation_level: isolation })
        });
        const data = await response.json();
        addResult('Test Case 1: Concurrent Reads', data);
    } catch (error) {
        addResult('Test Case 1 Failed', { error: error.message });
    }
}

async function runCase2() {
    const tconst = document.getElementById('case2Tconst').value;
    const isolation = document.getElementById('case2Isolation').value;
    const runtime = parseInt(document.getElementById('case2Runtime').value);
    
    try {
        const updates = [{ tconst, data: { runtime_minutes: runtime } }];
        const writeResponse = await fetch(`${API_BASE}/test/concurrent-write`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ updates, isolation_level: isolation })
        });
        
        const readResponse = await fetch(`${API_BASE}/test/concurrent-read`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tconst, isolation_level: isolation })
        });
        
        const data = {
            write: await writeResponse.json(),
            read: await readResponse.json()
        };
        
        addResult('Test Case 2: Read-Write Concurrent', data);
    } catch (error) {
        addResult('Test Case 2 Failed', { error: error.message });
    }
}

async function runCase3() {
    const tconst = document.getElementById('case3Tconst').value;
    const isolation = document.getElementById('case3Isolation').value;
    
    try {
        const updates = [
            { tconst, data: { runtime_minutes: 100 } },
            { tconst, data: { runtime_minutes: 110 } },
            { tconst, data: { runtime_minutes: 120 } }
        ];
        
        const response = await fetch(`${API_BASE}/test/concurrent-write`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ updates, isolation_level: isolation })
        });
        
        const data = await response.json();
        addResult('Test Case 3: Concurrent Writes', data);
    } catch (error) {
        addResult('Test Case 3 Failed', { error: error.message });
    }
}

async function runIsolationComparison() {
    const tconst = document.getElementById('isolationTconst').value;
    const operation = document.getElementById('isolationType').value;
    
    try {
        const response = await fetch(`${API_BASE}/test/isolation-levels`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ tconst, operation })
        });
        
        const data = await response.json();
        addResult('Isolation Levels Comparison', data);
    } catch (error) {
        addResult('Isolation Comparison Failed', { error: error.message });
    }
}

async function runFailureCase1() {
    try {
        const response = await fetch(`${API_BASE}/test/failure/fragment-to-central`, {
            method: 'POST'
        });
        const data = await response.json();
        addResult('Failure Case 1: Fragment to Central', data);
    } catch (error) {
        addResult('Failure Case 1 Failed', { error: error.message });
    }
}

async function runFailureCase2() {
    try {
        const response = await fetch(`${API_BASE}/test/failure/central-recovery`, {
            method: 'POST'
        });
        const data = await response.json();
        addResult('Failure Case 2: Central Recovery', data);
    } catch (error) {
        addResult('Failure Case 2 Failed', { error: error.message });
    }
}

async function runFailureCase3() {
    try {
        const response = await fetch(`${API_BASE}/test/failure/central-to-fragment`, {
            method: 'POST'
        });
        const data = await response.json();
        addResult('Failure Case 3: Central to Fragment', data);
    } catch (error) {
        addResult('Failure Case 3 Failed', { error: error.message });
    }
}

async function runFailureCase4() {
    const node = document.getElementById('case4Node').value;
    
    try {
        const response = await fetch(`${API_BASE}/test/failure/fragment-recovery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node })
        });
        const data = await response.json();
        addResult('Failure Case 4: Fragment Recovery', data);
    } catch (error) {
        addResult('Failure Case 4 Failed', { error: error.message });
    }
}

async function checkRecoveryStatus() {
    try {
        const response = await fetch(`${API_BASE}/recovery/status`);
        const data = await response.json();
        addResult('Recovery Status', data);
    } catch (error) {
        addResult('Recovery Status Failed', { error: error.message });
    }
}

async function viewLogs() {
    const limit = document.getElementById('logLimit').value;
    
    try {
        const response = await fetch(`${API_BASE}/logs?limit=${limit}`);
        const data = await response.json();
        addResult('Transaction Logs', data);
    } catch (error) {
        addResult('View Logs Failed', { error: error.message });
    }
}

async function showTitles() {
    try {
        const response = await fetch(`${API_BASE}/titles?limit=10`);
        const data = await response.json();
        addResult('Titles List', data);
    } catch (error) {
        addResult('Show Titles Failed', { error: error.message });
    }
}

function clearResults() {
    document.getElementById('results').innerHTML = '<p>Results cleared.</p>';
}

checkHealth();
setInterval(checkHealth, 30000);