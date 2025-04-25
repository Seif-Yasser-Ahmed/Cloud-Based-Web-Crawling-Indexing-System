// web/app.js

const API_BASE = 'ec2-51-20-86-200.eu-north-1.compute.amazonaws.com';

document.addEventListener('DOMContentLoaded', () => {
  // --- Elements ---
  const startForm        = document.getElementById('startForm');
  const startButton      = document.getElementById('startButton');
  const seedUrlInput     = document.getElementById('seedUrl');

  const statusPanel      = document.getElementById('statusPanel');
  const jobIdSpan        = document.getElementById('jobId');
  const discoveredSpan   = document.getElementById('discoveredCount');
  const indexedSpan      = document.getElementById('indexedCount');
  const currentStatusSpan= document.getElementById('currentStatus');

  const searchForm       = document.getElementById('searchForm');
  const searchInput      = document.getElementById('searchQuery');
  const searchResultsDiv = document.getElementById('searchResults');

  // --- State ---
  let currentJobId = null;
  let statusInterval = null;

  // --- Handlers ---
  startForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const seedUrl = seedUrlInput.value.trim();
    if (!seedUrl) return alert('Please enter a seed URL.');

    startButton.disabled = true;
    statusPanel.classList.add('hidden');
    searchResultsDiv.innerHTML = '';
    if (statusInterval) clearInterval(statusInterval);

    try {
      const resp = await fetch(`${API_BASE}/jobs`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ seedUrl })
      });
      if (!resp.ok) throw new Error(`Error: ${resp.statusText}`);
      const { jobId } = await resp.json();

      currentJobId = jobId;
      jobIdSpan.textContent = jobId;
      statusPanel.classList.remove('hidden');

      // start polling status
      statusInterval = setInterval(fetchStatus, 2000);
      fetchStatus();
    } catch (err) {
      console.error(err);
      alert('Failed to start crawl.');
    } finally {
      startButton.disabled = false;
    }
  });

  async function fetchStatus() {
    if (!currentJobId) return;

    try {
      const resp = await fetch(`${API_BASE}/jobs/${currentJobId}`);
      if (!resp.ok) throw new Error(resp.statusText);
      const data = await resp.json();

      discoveredSpan.textContent    = data.discoveredCount  || 0;
      indexedSpan.textContent       = data.indexedCount     || 0;
      currentStatusSpan.textContent = data.status           || 'UNKNOWN';

      if (data.status === 'COMPLETED' || data.status === 'FAILED') {
        clearInterval(statusInterval);
      }
    } catch (err) {
      console.error('Status fetch error:', err);
    }
  }

  searchForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const query = searchInput.value.trim();
    if (!query) return alert('Please enter a search term.');

    searchResultsDiv.innerHTML = 'Searching…';

    try {
      const resp = await fetch(
        `${API_BASE}/search?query=${encodeURIComponent(query)}`
      );
      if (!resp.ok) throw new Error(resp.statusText);
      const results = await resp.json();
      displayResults(results);
    } catch (err) {
      console.error('Search error:', err);
      searchResultsDiv.innerHTML = '<p class="text-red-500">Search failed.</p>';
    }
  });

  function displayResults(results) {
    searchResultsDiv.innerHTML = '';
    if (!Array.isArray(results) || results.length === 0) {
      searchResultsDiv.innerHTML = '<p>No results found.</p>';
      return;
    }

    const ul = document.createElement('ul');
    ul.className = 'list-decimal pl-5 space-y-1';

    results.forEach(({ pageUrl }) => {
      const li = document.createElement('li');
      const a  = document.createElement('a');
      a.href        = pageUrl;
      a.textContent = pageUrl;
      a.target      = '_blank';
      a.className   = 'text-blue-600 hover:underline';
      li.appendChild(a);
      ul.appendChild(li);
    });

    searchResultsDiv.appendChild(ul);
  }
});
