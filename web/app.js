// app.js
// -----------------------
// Your Master API endpoint (include protocol and port)
const API_BASE = 'http://ec2-13-61-154-38.eu-north-1.compute.amazonaws.com:5000';  // ← make sure this matches how you’re serving master

document.addEventListener('DOMContentLoaded', () => {
  const startBtn = document.getElementById('startBtn');
  const seedUrl = document.getElementById('seedUrl');
  const depth = document.getElementById('depth');
  const statusText = document.getElementById('statusText');
  const searchBtn = document.getElementById('searchBtn');
  const searchQuery = document.getElementById('searchQuery');
  const resultsDiv = document.getElementById('results');

  let jobId = null;
  let pollInterval = null;

  startBtn.addEventListener('click', async () => {
    const url = seedUrl.value.trim();
    const d = parseInt(depth.value, 10) || 1;
    if (!url) {
      alert('Please enter a seed URL');
      return;
    }

    startBtn.disabled = true;
    statusText.textContent = 'Starting crawl job...';

    try {
      const res = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ seedUrl: url, depthLimit: d })
      });
      const data = await res.json();

      if (!res.ok) {
        statusText.textContent = `Error: ${data.error || res.status}`;
        startBtn.disabled = false;
        return;
      }

      jobId = data.jobId;
      statusText.textContent = `Job started: ${jobId}`;

      // Poll job status every 2 seconds
      pollInterval = setInterval(async () => {
        try {
          const statusRes = await fetch(`${API_BASE}/jobs/${jobId}`);
          const statusData = await statusRes.json();

          if (statusRes.ok) {
            statusText.textContent =
              `Discovered: ${statusData.discoveredCount} | Indexed: ${statusData.indexedCount}`;
          } else {
            statusText.textContent = `Status error: ${statusData.error || statusRes.status}`;
            clearInterval(pollInterval);
            startBtn.disabled = false;
          }
        } catch (e) {
          console.error('Polling error', e);
          statusText.textContent = 'Polling error';
          clearInterval(pollInterval);
          startBtn.disabled = false;
        }
      }, 2000);

    } catch (e) {
      console.error('Start job error', e);
      statusText.textContent = 'Failed to start job';
      startBtn.disabled = false;
    }
  });

  searchBtn.addEventListener('click', async () => {
    const q = searchQuery.value.trim();
    if (!q) return;

    resultsDiv.innerHTML = '<p>Searching...</p>';

    try {
      const res = await fetch(`${API_BASE}/search?query=${encodeURIComponent(q)}`);
      const items = await res.json();
      if (!res.ok) {
        resultsDiv.innerHTML = `<p class="text-red-500">Error: ${items.error || res.status}</p>`;
        return;
      }
      if (items.length === 0) {
        resultsDiv.innerHTML = '<p>No results found.</p>';
        return;
      }

      const ul = document.createElement('ul');
      ul.className = 'list-disc pl-5';
      items.forEach(item => {
        const li = document.createElement('li');
        const a = document.createElement('a');
        a.href = item.pageUrl;
        a.textContent = item.pageUrl;
        a.target = '_blank';
        a.className = 'text-blue-600 hover:underline mr-2';
        const span = document.createElement('span');
        span.textContent = `(${item.frequency})`;
        li.appendChild(a);
        li.appendChild(span);
        ul.appendChild(li);
      });
      resultsDiv.innerHTML = '';
      resultsDiv.appendChild(ul);

    } catch (e) {
      console.error('Search error', e);
      resultsDiv.innerHTML = '<p class="text-red-500">Search failed.</p>';
    }
  });
});
