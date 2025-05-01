// app.js

// Your Master API base (include protocol, domain, and port)
const API_BASE = 'http://ec2-13-61-154-38.eu-north-1.compute.amazonaws.com:5000';

document.addEventListener('DOMContentLoaded', () => {
  // Elements
  const startBtn = document.getElementById('startBtn');
  const seedUrlInput = document.getElementById('seedUrl');
  const depthInput = document.getElementById('depth');
  const statusText = document.getElementById('statusText');
  const searchBtn = document.getElementById('searchBtn');
  const searchInput = document.getElementById('searchQuery');
  const resultsDiv = document.getElementById('results');

  let jobId = null, pollInterval = null;

  // 1) Start Crawl
  startBtn.addEventListener('click', async () => {
    const url = seedUrlInput.value.trim();
    const depth = parseInt(depthInput.value, 10) || 1;
    if (!url) return alert('Please enter a seed URL.');

    startBtn.disabled = true;
    statusText.textContent = 'Starting…';
    clearInterval(pollInterval);
    resultsDiv.innerHTML = '';

    try {
      const resp = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ seedUrl: url, depthLimit: depth })
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      jobId = data.jobId;
      statusText.textContent = `Job ${jobId} started.`;
      pollInterval = setInterval(fetchStatus, 2000);
      fetchStatus();
    } catch (e) {
      console.error(e);
      alert('Failed to start crawl.');
    } finally {
      startBtn.disabled = false;
    }
  });

  // 2) Poll Job Status
  async function fetchStatus() {
    if (!jobId) return;
    try {
      const resp = await fetch(`${API_BASE}/jobs/${encodeURIComponent(jobId)}`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const { discoveredCount = 0, indexedCount = 0, status } = await resp.json();
      statusText.textContent =
        `Job ${jobId} — Discovered: ${discoveredCount}, Indexed: ${indexedCount}, Status: ${status}`;
      if (status === 'COMPLETED' || status === 'FAILED') {
        clearInterval(pollInterval);
      }
    } catch (e) {
      console.error('Status error', e);
    }
  }

  // 3) Search
  searchBtn.addEventListener('click', async () => {
    const term = searchInput.value.trim();
    if (!term) return alert('Please enter a search term.');

    resultsDiv.innerHTML = '<p>Searching…</p>';
    try {
      const resp = await fetch(`${API_BASE}/search?query=${encodeURIComponent(term)}`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const list = await resp.json();
      if (!Array.isArray(list) || list.length === 0) {
        resultsDiv.innerHTML = '<p>No results found.</p>';
        return;
      }
      const ul = document.createElement('ul');
      ul.className = 'list-decimal pl-5 space-y-1';
      list.forEach(({ pageUrl }) => {
        const li = document.createElement('li');
        const a = document.createElement('a');
        a.href = pageUrl;
        a.textContent = pageUrl;
        a.target = '_blank';
        a.className = 'text-blue-600 hover:underline';
        li.appendChild(a);
        ul.appendChild(li);
      });
      resultsDiv.innerHTML = '';
      resultsDiv.appendChild(ul);
    } catch (e) {
      console.error('Search error', e);
      resultsDiv.innerHTML = '<p class="text-red-500">Search failed.</p>';
    }
  });

  // 4) Monitoring: fetch threads, queues, and node heartbeats
  async function fetchMonitor() {
    try {
      const resp = await fetch(`${API_BASE}/status`);
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const { crawlers, indexers, queues, threads } = await resp.json();

      // Threads
      document.getElementById('crawlerThreads').textContent = threads.crawler;
      document.getElementById('indexerThreads').textContent = threads.indexer;

      // Queue depths
      document.getElementById('crawlQueueVisible').textContent = queues.crawl.visible;
      document.getElementById('crawlQueueInFlight').textContent = queues.crawl.inFlight;
      document.getElementById('indexQueueVisible').textContent = queues.index.visible;
      document.getElementById('indexQueueInFlight').textContent = queues.index.inFlight;

      // Node statuses
      const cList = document.getElementById('crawlerStatus');
      const iList = document.getElementById('indexerStatus');
      cList.innerHTML = '';
      iList.innerHTML = '';

      Object.entries(crawlers).forEach(([id, status]) => {
        const li = document.createElement('li');
        li.textContent = `${id}: ${status}`;
        li.className = status === 'alive' ? 'text-green-600' : 'text-red-600';
        cList.appendChild(li);
      });
      Object.entries(indexers).forEach(([id, status]) => {
        const li = document.createElement('li');
        li.textContent = `${id}: ${status}`;
        li.className = status === 'alive' ? 'text-green-600' : 'text-red-600';
        iList.appendChild(li);
      });
    } catch (e) {
      console.error('Monitor error', e);
    }
  }

  // Kick off monitoring polls
  fetchMonitor();
  setInterval(fetchMonitor, 5000);
});
