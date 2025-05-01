// app.js

const API_BASE = 'http://ec2-51-20-86-200.eu-north-1.compute.amazonaws.com:5000';

document.addEventListener('DOMContentLoaded', () => {
  // --- Elements ---
  const seedUrlInput = document.getElementById('seedUrl');
  const depthInput = document.getElementById('depth');
  const startBtn = document.getElementById('startBtn');
  const statusDiv = document.getElementById('status');
  const statusText = document.getElementById('statusText');
  const searchQueryInput = document.getElementById('searchQuery');
  const searchBtn = document.getElementById('searchBtn');
  const resultsDiv = document.getElementById('results');

  // Thread monitor elements
  const refreshMonitorBtn = document.getElementById('refreshMonitorBtn');
  const crawlerThreadsDiv = document.getElementById('crawlerThreads');
  const indexerThreadsDiv = document.getElementById('indexerThreads');
  const crawlQueueVisible = document.getElementById('crawlQueueVisible');
  const crawlQueueInFlight = document.getElementById('crawlQueueInFlight');
  const indexQueueVisible = document.getElementById('indexQueueVisible');
  const indexQueueInFlight = document.getElementById('indexQueueInFlight');

  // --- State ---
  let currentJobId = null;
  let statusInterval = null;
  let monitorInterval = null;

  // --- Handlers ---
  startBtn.addEventListener('click', async () => {
    const seedUrl = seedUrlInput.value.trim();
    if (!seedUrl) return alert('Please enter a seed URL.');

    const depth = parseInt(depthInput.value) || 2;

    startBtn.disabled = true;
    statusText.textContent = 'Starting job...';
    resultsDiv.innerHTML = '';
    if (statusInterval) clearInterval(statusInterval);

    try {
      const resp = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ seedUrl, depthLimit: depth })
      });
      if (!resp.ok) throw new Error(`Error: ${resp.statusText}`);
      const { jobId } = await resp.json();

      currentJobId = jobId;
      statusText.textContent = `Job started with ID: ${jobId}. Fetching status...`;

      // start polling status and thread monitors
      statusInterval = setInterval(fetchStatus, 2000);
      if (!monitorInterval) {
        monitorInterval = setInterval(fetchMonitorData, 3000);
      }
      fetchStatus();
      fetchMonitorData();
    } catch (err) {
      console.error(err);
      statusText.textContent = `Failed to start crawl: ${err.message}`;
    } finally {
      startBtn.disabled = false;
    }
  });

  async function fetchStatus() {
    if (!currentJobId) return;

    try {
      const resp = await fetch(`${API_BASE}/jobs/${currentJobId}`);
      if (!resp.ok) throw new Error(resp.statusText);
      const data = await resp.json();

      statusText.textContent = `Job: ${currentJobId}
Status: ${data.status || 'UNKNOWN'}
Discovered: ${data.discoveredCount || 0} pages
Indexed: ${data.indexedCount || 0} pages`;

      if (data.status === 'COMPLETED' || data.status === 'FAILED') {
        clearInterval(statusInterval);
      }
    } catch (err) {
      console.error('Status fetch error:', err);
      statusText.innerHTML = `Status fetch error: ${err.message}`;
    }
  }

  searchBtn.addEventListener('click', async () => {
    const query = searchQueryInput.value.trim();
    if (!query) return alert('Please enter a search term.');

    resultsDiv.innerHTML = '<p>Searching...</p>';

    try {
      const resp = await fetch(
        `${API_BASE}/search?query=${encodeURIComponent(query)}`
      );
      if (!resp.ok) throw new Error(resp.statusText);
      const results = await resp.json();
      displayResults(results);
    } catch (err) {
      console.error('Search error:', err);
      resultsDiv.innerHTML = '<p class="text-red-500">Search failed.</p>';
    }
  });

  function displayResults(results) {
    resultsDiv.innerHTML = '';
    if (!Array.isArray(results) || results.length === 0) {
      resultsDiv.innerHTML = '<p>No results found.</p>';
      return;
    }

    const ul = document.createElement('ul');
    ul.className = 'list-decimal pl-5 space-y-1';

    results.forEach(({ pageUrl }) => {
      const li = document.createElement('li');
      const a = document.createElement('a');
      a.href = pageUrl;
      a.textContent = pageUrl;
      a.target = '_blank';
      a.className = 'text-blue-600 hover:underline';
      li.appendChild(a);
      ul.appendChild(li);
    });

    resultsDiv.appendChild(ul);
  }

  // --- Thread Monitor Functions ---
  refreshMonitorBtn.addEventListener('click', () => {
    fetchMonitorData();
  });

  async function fetchMonitorData() {
    try {
      const resp = await fetch(`${API_BASE}/status`);
      if (!resp.ok) throw new Error(resp.statusText);
      const data = await resp.json();

      // Update queue stats
      const crawlQueue = data.queues?.crawl || {};
      const indexQueue = data.queues?.index || {};

      crawlQueueVisible.textContent = crawlQueue.visible || '0';
      crawlQueueInFlight.textContent = crawlQueue.inFlight || '0';
      indexQueueVisible.textContent = indexQueue.visible || '0';
      indexQueueInFlight.textContent = indexQueue.inFlight || '0';

      // Update crawler threads
      displayThreads(crawlerThreadsDiv, data.crawlers || {});

      // Update indexer threads
      displayThreads(indexerThreadsDiv, data.indexers || {});
    } catch (err) {
      console.error('Monitor fetch error:', err);
      crawlerThreadsDiv.innerHTML = '<p class="text-red-500">Failed to fetch crawler thread data.</p>';
      indexerThreadsDiv.innerHTML = '<p class="text-red-500">Failed to fetch indexer thread data.</p>';
    }
  }

  function displayThreads(container, threads) {
    if (Object.keys(threads).length === 0) {
      container.innerHTML = '<p class="text-sm text-gray-500">No threads available</p>';
      return;
    }

    const table = document.createElement('table');
    table.className = 'min-w-full text-sm';

    // Create table header
    const thead = document.createElement('thead');
    thead.innerHTML = `
      <tr class="border-b border-gray-300">
        <th class="text-left p-1">Thread ID</th>
        <th class="text-left p-1">Status</th>
        <th class="text-left p-1">State</th>
        <th class="text-left p-1">Current URL</th>
      </tr>
    `;
    table.appendChild(thead);

    // Create table body
    const tbody = document.createElement('tbody');

    Object.entries(threads).forEach(([threadId, info]) => {
      const row = document.createElement('tr');
      row.className = 'border-b border-gray-200 hover:bg-gray-50';

      // Thread ID cell
      const idCell = document.createElement('td');
      idCell.className = 'p-1';
      idCell.textContent = threadId.split('-').slice(-2).join('-'); // Show only the last parts for brevity

      // Status cell with colored indicator
      const statusCell = document.createElement('td');
      statusCell.className = 'p-1';
      const statusSpan = document.createElement('span');
      statusSpan.textContent = info.status || 'unknown';
      statusSpan.className = info.status === 'alive'
        ? 'px-2 py-0.5 bg-green-100 text-green-800 rounded-full'
        : 'px-2 py-0.5 bg-red-100 text-red-800 rounded-full';
      statusCell.appendChild(statusSpan);

      // State cell
      const stateCell = document.createElement('td');
      stateCell.className = 'p-1';
      const stateSpan = document.createElement('span');
      stateSpan.textContent = info.state || 'unknown';
      stateSpan.className = getStateClass(info.state);
      stateCell.appendChild(stateSpan);

      // URL cell (truncated)
      const urlCell = document.createElement('td');
      urlCell.className = 'p-1 max-w-xs truncate';
      urlCell.title = info.url || '';
      urlCell.textContent = info.url || '-';

      row.appendChild(idCell);
      row.appendChild(statusCell);
      row.appendChild(stateCell);
      row.appendChild(urlCell);

      tbody.appendChild(row);
    });

    table.appendChild(tbody);

    // Clear and add the table
    container.innerHTML = '';
    container.appendChild(table);
  }

  function getStateClass(state) {
    switch (state) {
      case 'processing':
        return 'px-2 py-0.5 bg-blue-100 text-blue-800 rounded-full';
      case 'waiting':
        return 'px-2 py-0.5 bg-yellow-100 text-yellow-800 rounded-full';
      case 'idle':
        return 'px-2 py-0.5 bg-gray-100 text-gray-800 rounded-full';
      default:
        return 'px-2 py-0.5 bg-gray-100 text-gray-800 rounded-full';
    }
  }

  // Start monitoring immediately if on the page
  fetchMonitorData();
  monitorInterval = setInterval(fetchMonitorData, 3000);
});
