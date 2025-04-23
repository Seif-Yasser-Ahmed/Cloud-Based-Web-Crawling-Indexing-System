import threading
import queue
import json
import time
import logging
import os
from collections import defaultdict

from crawler import WebCrawler
from search_gui import SearchApp
from heartbeat_monitor import HeartbeatManager
from whoosh.writing import AsyncWriter
from whoosh.index import open_dir
from indexer import Indexer



logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
INDEX_DIR = "indexdir"
ix = open_dir(INDEX_DIR)
indexer_instance = Indexer(ix)

#woh
# def indexer_worker(shared_index, index_queue, index_lock, heartbeat_manager, node_id="Indexer"):
def indexer_worker( index_queue,heartbeat_manager, node_id="Indexer"):
    while True:
        try:
            url, text = index_queue.get(timeout=0.1)
        except queue.Empty:
            heartbeat_manager.update(node_id)
            time.sleep(0.1)
            continue
        # words = set(text.lower().split())
        # with index_lock:
        #     for word in words:
        #         shared_index.setdefault(word, set()).add(url)
       
      indexer_instance.index_document(url=url, html_content=text)
           



        index_queue.task_done()
        heartbeat_manager.update(node_id)

def load_dynamic_urls(task_queue, visited, visited_lock):
    dynamic_urls_file = "dynamic_urls.json"
    if not os.path.exists(dynamic_urls_file):
        return

    with open(dynamic_urls_file, "r") as f:
        try:
            dynamic_urls = json.load(f)
        except json.JSONDecodeError:
            logging.warning("Failed to decode dynamic_urls.json. Skipping.")
            return

    with visited_lock:
        for url in dynamic_urls:
            if url not in visited:
                visited.add(url)
                task_queue.put((url, 1))  # Default depth of 1 for dynamic URLs
                logging.info(f"Added dynamic URL to task queue: {url}")

    # Clear the file after loading URLs
    with open(dynamic_urls_file, "w") as f:
        json.dump([], f)

def main():
    starting_url = "http://quotes.toscrape.com"
    max_depth = 2
    delay = 0
    allow_external = True
    num_crawlers = 4

    task_queue = queue.Queue()
    visited = set()
    visited_lock = threading.Lock()
    url_map = {}
    node_url_log = defaultdict(list)

    index_queue = queue.Queue()
    # shared_index = {}
    # index_lock = threading.Lock()
    # # Load existing index so past results persist
    # try:
    #     with open("index.json", "r", encoding="utf-8") as f:
    #         data = json.load(f)
    #         for word, urls in data.items():
    #             shared_index[word] = set(urls)
    #     logging.info(f"Loaded {len(shared_index)} entries from index.json.")
    # except FileNotFoundError:
    #     logging.info("index.json not found. Starting with empty index.")

    heartbeat_manager = HeartbeatManager(timeout=6)
    in_flight = defaultdict(list)
    # Shared failure count across all nodes
    failed_attempts = defaultdict(int)
    failed_lock = threading.Lock()

    # Load existing URL map so old pages are reused
    try:
        with open("url_map.json", "r", encoding="utf-8") as f:
            data = json.load(f)
            url_map.update(data)
            # Load URL map without marking them as visited so depth-based traversal still enqueues children
            logging.info(f"Loaded {len(data)} URLs from url_map.json (not marked visited for depth traversal).")
    except FileNotFoundError:
        logging.info("url_map.json not found. Starting fresh.")

    # Removed master heartbeat thread; only worker nodes send heartbeats

    def heartbeat_monitor():
        while True:
            # Dump heartbeat timestamps to file for GUI
            with heartbeat_manager.lock:
                try:
                    with open("heartbeat_log.json", "w", encoding="utf-8") as hb_file:
                        json.dump(heartbeat_manager.last_heartbeat, hb_file)
                except Exception as e:
                    logging.warning(f"Failed to write heartbeat log: {e}")
            dead_nodes = heartbeat_manager.check_dead_nodes()
            for node in dead_nodes:
                logging.warning(f"{node} unresponsive. Reassigning its tasks...")
                for task in in_flight.get(node, []):
                    task_queue.put(task)
                    logging.info(f"Requeued {task} from {node}")
                heartbeat_manager.remove_node(node)
                del in_flight[node]
            time.sleep(0.1)

    threading.Thread(target=heartbeat_monitor, daemon=True).start()

    def dynamic_url_loader():
        while True:
            load_dynamic_urls(task_queue, visited, visited_lock)
            time.sleep(0.1)

    threading.Thread(target=dynamic_url_loader, daemon=True).start()

    def crawler_worker(node_id):
        crawler = WebCrawler(delay=delay, allow_external=allow_external, node_id=node_id)
        while True:
            try:
                task = task_queue.get(timeout=0.1)
                url, depth = task
                # Skip URLs that have already failed 3 times
                with failed_lock:
                    if failed_attempts[url] >= 3:
                        logging.warning(f"{node_id} skipping {url} after 3 failed attempts")
                        try:
                            in_flight[node_id].remove(task)
                        except ValueError:
                            pass
                        task_queue.task_done()
                        continue
                in_flight[node_id].append(task)
            except queue.Empty:
                heartbeat_manager.update(node_id)  # Send heartbeat
                continue

            # Disabled cached-page branch to force fresh crawling and index updates
            # if url in url_map:
            #     ...cached logic removed...
            #     continue

            heartbeat_manager.update(node_id)
            logging.info(f"{node_id} crawling {url} (depth={depth})")
            node_url_log[node_id].append(url)
            children = crawler.process_url(url, starting_url, depth, url_map)
            # If blocked by robots.txt, process_url returns None -> drop immediately
            if children is None:
                logging.info(f"{node_id} dropping {url} due to robots.txt block")
                try:
                    in_flight[node_id].remove(task)
                except ValueError:
                    pass
                task_queue.task_done()
                continue

            # Remove or requeue logic
            if not children and url not in url_map:
                with failed_lock:
                    failed_attempts[url] += 1
                    count = failed_attempts[url]
                if count >= 3:
                    logging.warning(f"{node_id} failed to crawl {url} {count} times. Removing.")
                else:
                    logging.warning(f"{node_id} failed to crawl {url}. Requeueing attempt {count}.")
                    task_queue.put(task)

            path = url_map.get(url)
            if path:
                try:
                    with open(path, encoding='utf-8') as f:
                        html = f.read()
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "html.parser")
                    text = soup.get_text(separator=' ')
                    index_queue.put((url, text))
                except Exception as e:
                    logging.warning(f"{node_id} failed to send to indexer: {e}")

            if depth > 0:
                for child in children:
                    with visited_lock:
                        if child not in visited:
                            visited.add(child)
                            task_queue.put((child, depth - 1))

            try:
                in_flight[node_id].remove(task)
            except ValueError:
                logging.warning(f"{node_id} missing task removal: {task}")

            task_queue.task_done()

    def gui_launcher():
        # app = SearchApp(shared_index)
        app = SearchApp(ix)
        app.mainloop()

    threading.Thread(target=gui_launcher, daemon=True).start()
    # threading.Thread(target=indexer_worker, args=(shared_index, index_queue, index_lock, heartbeat_manager), daemon=True).start()
    threading.Thread(target=indexer_worker, args=( index_queue,heartbeat_manager), daemon=True).start()
    with visited_lock:
        visited.add(starting_url)
    task_queue.put((starting_url, max_depth))

    for i in range(num_crawlers):
        t = threading.Thread(target=crawler_worker, args=(f"Node{i+1}",), name=f"Node{i+1}")
        t.start()

    while True:
        task_queue.join()
        index_queue.join()

        # Save persistent index for future sessions
        # try:
        #     with open("index.json", "w", encoding="utf-8") as f:
        #         json.dump({word: list(urls) for word, urls in shared_index.items()}, f, indent=2)
        #     logging.info(f"Saved {len(shared_index)} entries to index.json.")
        # except Exception as e:
        #     logging.warning(f"Failed to save index.json: {e}")
        # try:
        #     import pickle
        #     with open("inverted_index.pkl", "wb") as pf:
        #         pickle.dump({word: list(urls) for word, urls in shared_index.items()}, pf)
        #     logging.info(f"Saved {len(shared_index)} entries to inverted_index.pkl.")
        # except Exception as e:
        #     logging.warning(f"Failed to save inverted_index.pkl: {e}")

        try:
            with open("url_map.json", "w", encoding="utf-8") as f:
                json.dump(url_map, f, indent=2)
            with open("node_url_log.json", "w", encoding="utf-8") as f:
                json.dump(node_url_log, f, indent=2)
        except Exception as e:
            logging.warning(f"Failed to save url logs: {e}")
        logging.info("Crawl cycle complete. Waiting for new tasks.")
        while task_queue.empty():
            time.sleep(0.1)

if __name__ == "__main__":
    main()
