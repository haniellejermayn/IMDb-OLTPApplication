// const API_URL = "http://localhost:80"; // Change to your backend URL

async function loadPendingReplications() {
    try {
        const res = await fetch(`/recovery/status`);
        const data = await res.json();
        console.log("API response:", data);

        // Update pending replications
        updatePendingReplications("node-1", data.by_node.node1.pending_count ?? 0);
        updatePendingReplications("node-2", data.by_node.node2.pending_count ?? 0);
        updatePendingReplications("node-3", data.by_node.node3.pending_count ?? 0);

    } catch (error) {
        console.error("Error fetching pending replications:", error);
    }
}

async function loadNodeStatus() {
    try {
        const res = await fetch(`/health`);
        const data = await res.json();
        console.log("API response:", data);

        // Update node statuses
        updateNodeStatus("node-1", data.node1.status);
        updateNodeStatus("node-2", data.node2.status);
        updateNodeStatus("node-3", data.node3.status);

    } catch (error) {
        console.error("Error fetching node status:", error);
    }
}

function updatePendingReplications(nodeId, count) {
    const pendingRepElem = document.querySelector(`#${nodeId} .pending-rep`);
    pendingRepElem.textContent = `${count} Pending Replications`;
}

function updateNodeStatus(nodeId, status) {
    const statusElems = document.querySelectorAll(`#${nodeId} .node-status`);
    statusElems.textContent = status;

    // Update health indicator color
    const indicator = document.querySelector(`#${nodeId} .node-health-indicator`);
    if (status === "online") {
        indicator.style.backgroundColor = "#4CAF50";
    } else {
        indicator.style.backgroundColor = "#E83D3D";
    } 
}