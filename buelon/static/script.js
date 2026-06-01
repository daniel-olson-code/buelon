// Global state
let currentData = null;
let currentWorker = null;
let currentJob = null;

// Theme management
function initTheme() {
    const savedTheme = localStorage.getItem('darkMode') === 'true' ? 'dark' : 'light';
    document.body.setAttribute('data-theme', savedTheme);
}

function toggleTheme() {
    const currentTheme = document.body.getAttribute('data-theme');
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    document.body.setAttribute('data-theme', newTheme);
    localStorage.setItem('darkMode', newTheme === 'dark');
}

// Page navigation
function showPage(pageNum, updateBreadcrumb = true) {
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.getElementById(`page${pageNum}`).classList.add('active');

    if (updateBreadcrumb) {
        updateBreadcrumbNav(pageNum);
    }
}

function updateBreadcrumbNav(pageNum) {
    const breadcrumb = document.getElementById('breadcrumb');
    let content = '';

    switch(pageNum) {
        case 1:
            content = '<span>Dashboard</span>';
            break;
        case 2:
            content = `
                <span onclick="showPage(1)" style="cursor: pointer; color: var(--accent);">Dashboard</span>
                <span class="breadcrumb-separator">›</span>
                <span>${currentWorker ? currentWorker.name : 'Worker'}</span>
            `;
            break;
        case 3:
            content = `
                <span onclick="showPage(1)" style="cursor: pointer; color: var(--accent);">Dashboard</span>
                <span class="breadcrumb-separator">›</span>
                <span onclick="showPage(2)" style="cursor: pointer; color: var(--accent);">${currentWorker ? currentWorker.name : 'Worker'}</span>
                <span class="breadcrumb-separator">›</span>
                <span>${currentJob ? currentJob.name : 'Job'}</span>
            `;
            break;
    }

    breadcrumb.innerHTML = content;
}

// API functions
const getData = async () => {
    try {
        const r = await fetch('/data', {
            method: 'POST',
            body: '{}',
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        const j = await r.json();
        console.log('getData:', j);
        return j;
    } catch (error) {
        console.error('Error fetching data:', error);
        return null;
    }
};

const getErrorData = async () => {
    try {
        const r = await fetch('/errors', {
            method: 'POST',
            body: '{}',
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        const j = await r.json();
        console.log('getErrorData:', j);
        return j;
    } catch (error) {
        console.error('Error fetching error data:', error);
        return null;
    }
};


const resetErrors = async () => {
    try {
        const r = await fetch('/reset-errors', {
            method: 'POST',
            body: '{}',
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        const j = await r.json();
        console.log('resetErrors:', j);
        return j;
    } catch (error) {
        console.error('Error fetching error data:', error);
        return null;
    }
};

const getJobParentAndResults = async (jobId) => {
    try {
        const r = await fetch('/job-parents-and-results', {
            method: 'POST',
            body: JSON.stringify({id: jobId}),
            headers: {'Content-Type': 'application/json', 'Accept': 'application/json'}
        });
        const j = await r.json();
        console.log('getJobParentAndResults:', j);
        return j;
    } catch (error) {
        console.error('Error fetching job parents:', error);
        return null;
    }
};

// Log streaming function
async function runJob(jobId) {
    const runButton = document.getElementById('runJobBtn');
    const loader = document.getElementById('runJobLoader');
    const logOutput = document.getElementById('runJobLogOutput');

    if (!runButton || !loader || !logOutput) return;

    runButton.disabled = true;
    loader.style.display = 'block';
    logOutput.textContent = 'Starting job execution...\n';

    try {
        const response = await fetch('/run-job', {
            method: 'POST',
            body: JSON.stringify({ id: jobId }),
            headers: { 'Content-Type': 'application/json' }
        });

        if (!response.body) {
            throw new Error("Response body is null.");
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();

        while (true) {
            const { done, value } = await reader.read();
            if (done) {
                logOutput.textContent += '\nExecution finished.';
                break;
            }
            const chunk = decoder.decode(value, { stream: true });
            logOutput.textContent += chunk;
            logOutput.scrollTop = logOutput.scrollHeight; // Auto-scroll
        }
    } catch (error) {
        console.error('Error running job:', error);
        logOutput.textContent += `\n--- ERROR ---\n${error.message}`;
    } finally {
        runButton.disabled = false;
        loader.style.display = 'none';
    }
}


// Dashboard rendering
function renderStats(counts) {
    const statsGrid = document.getElementById('statsGrid');
    const stats = [
        { label: 'Total Jobs', value: counts.total, color: 'var(--accent)' },
        { label: 'Remaining', value: counts.remaining, color: 'var(--text-secondary)' },
        { label: 'Completed', value: counts.done, color: 'var(--success)' },
        { label: 'Pending', value: counts.jobs, color: 'var(--text-secondary)' },
        { label: 'Queued', value: counts.queued, color: 'var(--warning)' },
        { label: 'On Hold', value: counts.holds, color: 'var(--warning)' },
        { label: 'Errors', value: counts.errors, color: 'var(--danger)' },
    ];

    statsGrid.innerHTML = stats.map(stat => `
        <div class="card stat-card">
            <div class="stat-value" style="color: ${stat.color}">${stat.value.toLocaleString()}</div>
            <div class="stat-label">${stat.label}</div>
        </div>
    `).join('');
}

function renderWorkers(workers) {
    const workersGrid = document.getElementById('workersGrid');
    if (!workers) {
        workersGrid.innerHTML = '';
        return;
    }

    workersGrid.innerHTML = Object.entries(workers).map(([id, worker]) => {
        const statValueClassName = id === 'buelon_errors' ? 'worker-stat-value-error' : 'worker-stat-value';

        return `
        <div class="card card-clickable worker-card" onclick="selectWorker('${id}')">
            <div class="worker-header">
                <div>
                    <div class="worker-name">${worker.name}</div>
                    <div class="worker-id">${id}</div>
                </div>
            </div>
            <div class="worker-stats">
                <div class="worker-stat">
                    <div class="${statValueClassName}">${worker.jobs?.length || 0}</div>
                    <div class="worker-stat-label">Jobs</div>
                </div>
                <div class="worker-stat">
                    <div class="${statValueClassName}">${typeof worker?.holds !== 'number' ? 0 : (worker?.holds || 0)}</div>
                    <div class="worker-stat-label">Holds</div>
                </div>
            </div>
        </div>
    `}).join('');
}

// Worker page rendering
function selectWorker(workerId) {
    currentWorker = {
        id: workerId,
        ...currentData.workers[workerId]
    };
    renderWorkerJobs();
    showPage(2);
}

function renderWorkerJobs() {
    if (!currentWorker) return;

    document.getElementById('workerTitle').textContent = `${currentWorker.name} - ${currentWorker.jobs?.length || 0} Jobs`;

    const jobsList = document.getElementById('jobsList');
    jobsList.innerHTML = currentWorker.jobs.map(job => `
        <div class="job-item" onclick="selectJob('${job.id}')">
            <div class="job-info">
                <div class="job-name">${job.name}</div>
                <div class="job-id">${job.id}</div>
            </div>
            <div class="job-meta">
                <div class="job-type">${job.type}</div>
                <div class="job-priority">Priority: ${job.priority}</div>
            </div>
        </div>
    `).join('');
}

// Job details rendering
function selectJob(jobId) {
    // Find job in any worker, including the error worker
    for (const workerId in currentData.workers) {
        const worker = currentData.workers[workerId];
        const job = worker.jobs.find(j => j.id === jobId);
        if (job) {
            currentJob = job;
            currentWorker = { id: workerId, ...worker }; // Set current worker context
            break;
        }
    }
    renderJobDetails();
    showPage(3);
}

function renderJobDetails() {
    if (!currentJob) return;

    const jobDetail = document.getElementById('jobDetail');

    jobDetail.innerHTML = `
        <div class="detail-section">
            <div class="section-title">Job Information</div>
            <div class="detail-grid">
                <div class="detail-item">
                    <div class="detail-label">ID</div>
                    <div class="detail-value">${currentJob.id}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Name</div>
                    <div class="detail-value">${currentJob.name}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Type</div>
                    <div class="detail-value">${currentJob.type}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Function</div>
                    <div class="detail-value">${currentJob.func}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Priority</div>
                    <div class="detail-value">${currentJob.priority}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Retries</div>
                    <div class="detail-value">${currentJob.retries}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Scope</div>
                    <div class="detail-value">${currentJob.scope}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Timeout</div>
                    <div class="detail-value">${currentJob.timeout}s</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Local</div>
                    <div class="detail-value">${currentJob.local ? 'Yes' : 'No'}</div>
                </div>
            </div>
        </div>

        <div class="detail-section">
            <div class="section-title">Relationships</div>
            <div class="detail-grid">
                <div class="detail-item">
                    <div class="detail-label">Parents</div>
                    <div class="detail-value">${currentJob.parents.length > 0 ? currentJob.parents.join(', ') : 'None'}</div>
                </div>
                <div class="detail-item">
                    <div class="detail-label">Children</div>
                    <div class="detail-value">${currentJob.children.length > 0 ? currentJob.children.join(', ') : 'None'}</div>
                </div>
            </div>
        </div>

        <div class="detail-section">
            <div class="section-title">Code</div>
            <pre class="code-block"><code>${currentJob.code}</code></pre>
        </div>

        ${currentJob.error ? `
        <div class="detail-section error-section">
            <div class="section-title error-title">Error Information</div>
            <div class="detail-item" style="grid-column: 1 / -1;">
                <div class="detail-label">Message</div>
                <div class="detail-value error-message">${currentJob.error}</div>
            </div>
            ${currentJob.trace ? `
            <div class="detail-item" style="grid-column: 1 / -1;">
                <div class="detail-label">Traceback</div>
                <pre class="code-block error-trace"><code>${currentJob.trace}</code></pre>
            </div>
            ` : ''}
        </div>
        ` : ''}

        <div class="detail-section">
            <div class="section-title">Actions</div>
            <button class="btn btn-primary" onclick="showParentTree()">
                View Parent Tree & Results
            </button>
        </div>

        <div class="detail-section">
            <div class="section-title">Run Job</div>
            <div class="run-job-controls">
                <button id="runJobBtn" class="btn btn-primary" onclick="runJob('${currentJob.id}')">
                    Run
                </button>
                <!-- <div id="runJobLoader" class="maze-loader" style="display: none;"></div> -->
                <button id="runJobLoader" class="btn" style="display: none;" onclick="window.open('/static/maze.html', '_blank')">
                    <div class="maze-loader"></div>
                </button>
            </div>
            <pre id="runJobLogOutput" class="code-block"></pre>
        </div>
    `;
}

// Parent tree modal
async function showParentTree() {
    if (!currentJob) return;

    const modal = document.getElementById('parentTreeModal');
    const content = document.getElementById('parentTreeContent');

    content.innerHTML = '<div class="loading"><div class="spinner"></div>Loading parent tree...</div>';
    modal.classList.add('show');

    const treeData = await getJobParentAndResults(currentJob.id);
    if (treeData) {
        renderParentTree(treeData, content);
    } else {
        content.innerHTML = '<div class="loading">Failed to load parent tree data.</div>';
    }
}

function renderParentTree(treeData, container) {
    function renderNode(nodeData, depth = 0) {
        if (!nodeData || !nodeData.job) return '';

        const job = nodeData.job;
        const result = nodeData.result;
        const parents = nodeData.parents || {};

        let html = `
            <div class="tree-node" style="margin-left: ${depth * 1.5}rem">
                <div class="tree-content">
                    <div class="tree-job-name">${job.name} (${job.func})</div>
                    <div><strong>ID:</strong> ${job.id}</div>
                    <div><strong>Type:</strong> ${job.type}</div>
                    <div><strong>Priority:</strong> ${job.priority}</div>
                    ${result !== null ? `<div class="tree-result"><strong>Result:</strong>\n${JSON.stringify(result, null, 2)}</div>` : ''}
                </div>
            </div>
        `;

        // Render parent nodes
        Object.values(parents).forEach(parent => {
            html += renderNode(parent, depth + 1);
        });

        return html;
    }

    container.innerHTML = `
        <div class="tree-view">
            <h4 style="margin-bottom: 1rem;">Job Execution Tree</h4>
            ${renderNode(treeData)}
        </div>
    `;
}

function closeModal() {
    document.getElementById('parentTreeModal').classList.remove('show');
}

// Initialize app
async function initApp() {
    try {
        currentData = await getData();
        if (currentData) {
            // If there are errors, fetch the error details and merge them
            if (currentData.counts && currentData.counts.errors > 0) {
                const errorData = await getErrorData();
                if (errorData && errorData.jobs && errorData.jobs.length > 0) {
                    const errorJobs = errorData.jobs.map((job, index) => ({
                        ...job,
                        error: errorData.errors[index]?.error,
                        trace: errorData.errors[index]?.trace
                    }));

                    // Create a synthetic worker for errors
                    if (!currentData.workers) currentData.workers = {};
//                    currentData.workers['buelon_errors'] = {
//                        name: 'Buelon Errors',
//                        jobs: errorJobs,
//                        holds: 0 // Default value to prevent render issues
//                    };
                    currentData.workers = {
                      buelon_errors: {
                        name: 'Buelon Errors',
                        jobs: errorJobs,
                        holds: 0 // Default value to prevent render issues
                      },
                      ...currentData.workers
                    };
                }
            }
            renderStats(currentData.counts);
            renderWorkers(currentData.workers);
        } else {
            document.getElementById('statsGrid').innerHTML = `
                <div class="card">
                    <div style="text-align: center; color: var(--danger);">
                        Failed to load data. Please try again.
                        <br><br>
                        <button class="btn btn-primary" onclick="initApp()">Retry</button>
                    </div>
                </div>
            `;
        }
    } catch (error) {
        console.error('Error initializing app:', error);
    }
}

// Refresh Dashboard
async function refreshDashboard() {
    const buttonElement =  document.getElementById('dash-refresh');
    // innerHTML
    if(buttonElement) {
        try {
            buttonElement.disabled = true;
            buttonElement.innerHTML = '<div class="crazy-eyes-loader"></div>';//'<div class="maze-loader"></div>';
            await initApp();
            await new Promise(resolve => setTimeout(resolve, 1000));
        } finally {
            buttonElement.innerText = 'Refresh';
            buttonElement.disabled = false;
        }
    }
}

// Event listeners
document.getElementById('themeToggle').addEventListener('click', toggleTheme);

// Close modal on background click
document.getElementById('parentTreeModal').addEventListener('click', (e) => {
    if (e.target.id === 'parentTreeModal') {
        closeModal();
    }
});

// Close modal on escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeModal();
    }
});

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    initTheme();
    initApp();
});

// Auto-refresh data every 30 seconds
setInterval(() => {
    if (document.querySelector('#page1.active')) {
        refreshDashboard();
    }
}, 30 * 1000);
