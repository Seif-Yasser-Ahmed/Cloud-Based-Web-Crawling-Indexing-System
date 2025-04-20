import json
import pickle
import threading
import re
import logging
from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [Indexer] - [%(levelname)s] - %(message)s")

def index_document(url, filepath):
    try:
        with open(filepath, encoding='utf-8') as f:
            html = f.read()
    except Exception as e:
        logging.error(f"Error reading {filepath}: {e}")
        return {}

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=' ')
    terms = set(re.findall(r'\b\w+\b', text.lower()))

    return {term: {url} for term in terms}

def build_index_parallel(map_path="url_map.json", max_workers=None):
    with open(map_path, encoding='utf-8') as f:
        url_map = json.load(f)

    inverted_index = {}
    lock = threading.Lock()

    def worker(item):
        url, path = item
        local_idx = index_document(url, path)
        with lock:
            for term, urls in local_idx.items():
                inverted_index.setdefault(term, set()).update(urls)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(worker, url_map.items())

    for term in inverted_index:
        inverted_index[term] = list(inverted_index[term])

    with open("inverted_index.pkl", "wb") as pf:
        pickle.dump(inverted_index, pf)

    logging.info(f"Indexed {len(inverted_index)} unique terms into inverted_index.pkl")
    return inverted_index
