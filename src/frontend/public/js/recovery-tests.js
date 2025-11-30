
const API_BASE_URL = 'http://localhost';

const caseButtons = document.querySelectorAll('.case-option');
const caseContainers = document.querySelectorAll('.case-data-container');

caseButtons.forEach((button, index) => {
    button.addEventListener('click', () => {

        caseButtons.forEach(btn => {
            btn.style.backgroundColor = 'var(--color-bg)';
            btn.style.color = 'var(--color-main)';
        });

        button.style.backgroundColor = 'var(--color-sub)';
        button.style.color = 'var(--color-secondary)';

        caseContainers.forEach(container => {
            container.style.display = 'none';
        });

        caseContainers[index].style.display = 'flex';
    });
});

if (caseContainers.length > 0) {
    caseContainers[0].style.display = 'flex';
    caseContainers.forEach((container, index) => {
        if (index !== 0) container.style.display = 'none';
    });
}

const case1Form = document.getElementById('case-1-form');

case1Form.addEventListener('submit', async (e) => {
    e.preventDefault();
    await handleCase1();
});

async function handleCase1() {
    try {
        const button = case1Form.querySelector('button');
        button.disabled = true;
        button.textContent = 'Loading...';
        
        const response = await fetch(`${API_BASE_URL}/test/failure/fragment-to-central`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        populateCase1Instructions(data.steps);
        
        await updateCase1Status();
        
        button.disabled = false;
        button.textContent = 'Get Instructions';
        
    } catch (error) {
        console.error('Error fetching Case 1 instructions:', error);
        alert('Failed to load instructions.');
        
        const button = case1Form.querySelector('button');
        button.disabled = false;
        button.textContent = 'Get Instructions';
    }
}

function populateCase1Instructions(steps) {
    const container = document.querySelector('#case-1 .instructions-container');
    container.innerHTML = '';
    
    steps.forEach((step, index) => {
        const cleanStep = step.replace(/^\d+\.\s*/, '');
        
        const parts = cleanStep.split(':');
        const description = parts[0].trim();
        const command = parts[1] ? parts[1].trim() : null;
        
        let codeBlock = '';
        if (command !== null) {
            codeBlock = `<div class="code-block">${command}</div>`;
        }

        const stepHTML = `
            <div class="instruction-step">
                <div class="step-number">${index + 1}</div>
                <div class="step-content">
                    <h3>${description}</h3>
                    ${codeBlock}
                </div>
            </div>
            `;
        
        container.innerHTML += stepHTML;
    });
}

async function updateCase1Status() {
    try {
        const [healthResponse, recoveryResponse] = await Promise.all([
            fetch(`${API_BASE_URL}/health`),
            fetch(`${API_BASE_URL}/recovery/status`)
        ]);
        
        if (!healthResponse.ok || !recoveryResponse.ok) {
            throw new Error('Failed to fetch status');
        }
        
        const healthData = await healthResponse.json();
        const recoveryData = await recoveryResponse.json();
        
        const statusCards = document.querySelectorAll('#case-1 .status-value');
        
        const node1_health_data = healthData.node1;
        if(node1_health_data){
            statusCards[0].textContent =  node1_health_data.status.charAt(0).toUpperCase() + node1_health_data.status.slice(1);
            statusCards[0].className = `status-value ${node1_health_data.status}`;
        }

        const node_recovery_data = recoveryData.by_node

        if(node_recovery_data){
            let totalpending = 0

            totalpending += node_recovery_data.node2.pending_count
            totalpending += node_recovery_data.node3.pending_count

            statusCards[1].textContent = totalpending 
        }
    } catch (error) {
        console.error('Error updating Case 1 status:', error);
    }
}


// CASE 2
const case2Form = document.getElementById('case-2-form');

case2Form.addEventListener('submit', async (e) => {
    e.preventDefault();
    await handleCase2();
});

async function handleCase2() {
    try {
        const button = case2Form.querySelector('button');
        
        button.disabled = true;
        button.textContent = 'Loading...';
        
        const response = await fetch(`${API_BASE_URL}/test/failure/central-recovery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        updateCase2RecoveryResults(data);

        await populateCase2Logs(data.recovered);

        button.disabled = false;
        button.textContent = 'Trigger Recovery';
        
    } catch (error) {
        console.error('Error Central Recovery', error);
        alert('Make sure the backend is running.');
        
        const button = case2Form.querySelector('button');
        button.disabled = false;
        button.textContent = 'Trigger Recovery';
    }
}

function updateCase2RecoveryResults(recoveryData) {
    const summaryCards = document.querySelectorAll('#case-2 .summary-value');
    
    const statusCard = document.querySelector('#case-2 .summary-card');
    if (recoveryData.failed === 0) {
        statusCard.classList.add('success');
        statusCard.classList.remove('failure');
        summaryCards[0].textContent = 'Recovery Successful';
    } else {
        statusCard.classList.add('failure');
        statusCard.classList.remove('success');
        summaryCards[0].textContent = 'Recovery Failed';
    }
    
    summaryCards[1].textContent = recoveryData.recovered; 
    
}

async function getCase2ReplicatedLogs(recoveredCount) {
    try {
        const response = await fetch(`${API_BASE_URL}/logs?limit=50`);
        if (!response.ok) throw new Error('Failed to fetch logs');
        
        const data = await response.json();

        const nodeLogs = data.logs.filter(log => 
            log.target_node === 'node1' && 
            log.status === 'SUCCESS' &&
            log.completed_at !== null
        );
        
        return nodeLogs.slice(0, recoveredCount);
        
    } catch (error) {
        console.error('Error fetching logs:', error);
        return [];
    }
}

async function populateCase2Logs(recoveredCount) {
    const logsContainer = document.querySelector('#case-2 .logs-container');
    
    if (recoveredCount === 0) {
        logsContainer.innerHTML = `
            <div style="text-align: center; padding: 2rem; color: var(--color-sub);">
                <p>No transactions were replicated in this recovery.</p>
            </div>
        `;
        return;
    }
    
    const logs = await getCase2ReplicatedLogs(recoveredCount);
    logsContainer.innerHTML = '';
    
    logs.forEach(log => {
        let title = 'N/A';
        try {
            const params = JSON.parse(log.query_params);
            title = params[2] || 'N/A';
        } catch (e) {}
        
        const timestamp = new Date(log.created_at).toLocaleString();
        const operationClass = log.operation_type.toLowerCase();
        
        let titleHTML = '';
        if (log.operation_type === 'INSERT') {
            titleHTML = `<div class="log-detail"><b>Title:</b> ${title}</div>`;
        }
        
        logsContainer.innerHTML += `
            <div class="log-entry">
                <div class="log-header">
                    <div class="log-timestamp">${timestamp}</div>
                    <div class="log-operation ${operationClass}">${log.operation_type}</div>
                </div>
                <div class="log-content">
                    <div class="log-detail"><b>TCONST:</b> ${log.record_id}</div>
                    ${titleHTML}
                    <div class="log-detail"><b>Target Node:</b> ${log.target_node}</div>
                </div>
            </div>
        `;
    });
}

// CASE 3
const case3Form = document.getElementById('case-3-form');

case3Form.addEventListener('submit', async (e) => {
    e.preventDefault();
    await handleCase3();
});

async function handleCase3() {
    try {
        const button = case3Form.querySelector('button');
        const nodeSelect = document.getElementById('node-select');
        const selectedNode = nodeSelect.value; 
        
        button.disabled = true;
        button.textContent = 'Loading...';
        
        const response = await fetch(`${API_BASE_URL}/test/failure/central-to-fragment`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node: selectedNode })
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        populateCase3Instructions(data.steps);
        
        await updateCase3Status(selectedNode);
        
        button.disabled = false;
        button.textContent = 'Get Instructions';
        
    } catch (error) {
        console.error('Error fetching Case 3 instructions:', error);
        alert('Failed to load instructions. Make sure the backend is running.');
        
        const button = case3Form.querySelector('button');
        button.disabled = false;
        button.textContent = 'Get Instructions';
    }
}

function populateCase3Instructions(steps) {
    const container = document.querySelector('#case-3 .instructions-container');
    container.innerHTML = '';
    
    steps.forEach((step, index) => {
        const cleanStep = step.replace(/^\d+\.\s*/, '');
        const parts = cleanStep.split(':');
        const description = parts[0].trim();
        const command = parts[1] ? parts[1].trim() : null;
        
       let codeBlock = '';
        if (command !== null) {
            codeBlock = `<div class="code-block">${command}</div>`;
        }

        const stepHTML = `
            <div class="instruction-step">
                <div class="step-number">${index + 1}</div>
                <div class="step-content">
                    <h3>${description}</h3>
                    ${codeBlock}
                </div>
            </div>
            `;
        
        container.innerHTML += stepHTML;
        
    });
}

async function updateCase3Status(selectedNode) {
    try {
        const [healthResponse, recoveryResponse] = await Promise.all([
            fetch(`${API_BASE_URL}/health`),
            fetch(`${API_BASE_URL}/recovery/status`)
        ]);
        
        if (!healthResponse.ok || !recoveryResponse.ok) {
            throw new Error('Failed to fetch status');
        }
        
        const healthData = await healthResponse.json();
        const recoveryData = await recoveryResponse.json();

        const statusCards = document.querySelectorAll('#case-3 .status-value');
        
        const node_health = healthData[selectedNode];
        if(node_health){
            statusCards[0].textContent =  selectedNode.charAt(0).toUpperCase() + selectedNode.slice(1) + " - " + node_health.status.charAt(0).toUpperCase() + node_health.status.slice(1);
            statusCards[0].className = `status-value ${node_health.status}`;
        }

        const node1_recovery = recoveryData.by_node.node1;

        if(node1_recovery){
            statusCards[1].textContent = node1_recovery.pending_count
        }
        
        
    } catch (error) {
        console.error('Error updating Case 3 status:', error);
    }
}

// CASE 4

const case4Form = document.getElementById('case-4-form');

case4Form.addEventListener('submit', async (e) => {
    e.preventDefault();
    await handleCase4();
});

async function handleCase4() {
    try {
        const button = case4Form.querySelector('button');
        const nodeSelect = document.getElementById('node-select-recovery');
        const selectedNode = nodeSelect.value; 
        
        button.disabled = true;
        button.textContent = 'Loading...';
        
        const response = await fetch(`${API_BASE_URL}/test/failure/fragment-recovery`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node: selectedNode })
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const data = await response.json();
        
        updateCase4RecoveryResults(data);

        await populateCase4Logs(selectedNode, data.recovered);

        button.disabled = false;
        button.textContent = 'Trigger Recovery';
        
    } catch (error) {
        console.error('Error Fragment Recovery', error);
        alert('Make sure the backend is running.');
        
        const button = case4Form.querySelector('button');
        button.disabled = false;
        button.textContent = 'Trigger Recovery';
    }
}

function updateCase4RecoveryResults(recoveryData) {
    const summaryCards = document.querySelectorAll('#case-4 .summary-value');
    
    const statusCard = document.querySelector('#case-4 .summary-card');
    if (recoveryData.failed === 0) {
        statusCard.classList.add('success');
        statusCard.classList.remove('failure');
        summaryCards[0].textContent = 'Recovery Successful';
    } else {
        statusCard.classList.add('failure');
        statusCard.classList.remove('success');
        summaryCards[0].textContent = 'Recovery Failed';
    }
    
    summaryCards[1].textContent = recoveryData.node.charAt(0).toUpperCase() + recoveryData.node.slice(1);
    
    summaryCards[2].textContent = recoveryData.recovered;
    
}

async function getCase4ReplicatedLogs(targetNode, recoveredCount) {
    try {
        const response = await fetch(`${API_BASE_URL}/logs?limit=50`);
        if (!response.ok) throw new Error('Failed to fetch logs');
        
        const data = await response.json();

        const nodeLogs = data.logs.filter(log => 
            log.target_node === targetNode && 
            log.status === 'SUCCESS' &&
            log.completed_at !== null
        );
        
        return nodeLogs.slice(0, recoveredCount);
        
    } catch (error) {
        console.error('Error fetching logs:', error);
        return [];
    }
}

async function populateCase4Logs(targetNode, recoveredCount) {
    const logsContainer = document.querySelector('#case-4 .logs-container');
    
    if (recoveredCount === 0) {
        logsContainer.innerHTML = `
            <div style="text-align: center; padding: 2rem; color: var(--color-sub);">
                <p>No transactions were replicated in this recovery.</p>
            </div>
        `;
        return;
    }
    
    const logs = await getCase4ReplicatedLogs(targetNode, recoveredCount);
    logsContainer.innerHTML = '';
    
    logs.forEach(log => {
        let title = 'N/A';
        try {
            const params = JSON.parse(log.query_params);
            title = params[2] || 'N/A';
        } catch (e) {}
        
        const timestamp = new Date(log.created_at).toLocaleString();
        const operationClass = log.operation_type.toLowerCase();
        
        let titleHTML = '';
        if (log.operation_type === 'INSERT') {
            titleHTML = `<div class="log-detail"><b>Title:</b> ${title}</div>`;
        }
        
        logsContainer.innerHTML += `
            <div class="log-entry">
                <div class="log-header">
                    <div class="log-timestamp">${timestamp}</div>
                    <div class="log-operation ${operationClass}">${log.operation_type}</div>
                </div>
                <div class="log-content">
                    <div class="log-detail"><b>TCONST:</b> ${log.record_id}</div>
                    ${titleHTML}
                    <div class="log-detail"><b>Target Node:</b> ${log.target_node}</div>
                </div>
            </div>
        `;
    });
}