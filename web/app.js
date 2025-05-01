// web/app.js

const API_BASE = 'https://ec2-51-20-86-200.eu-north-1.compute.amazonaws.com';

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

  // --- State ---
  let currentJobId = null;
  let statusInterval = null;

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

      // start polling status
      statusInterval = setInterval(fetchStatus, 2000);
      fetchStatus();
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
});
