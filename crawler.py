import os
import time
import threading
import urllib.parse
import requests
import logging

from urllib.robotparser import RobotFileParser
from urllib.error import URLError, HTTPError
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")

class WebCrawler:
    def __init__(self, *, delay=1, allow_external=False, node_id="Node", user_agent="MyCrawler"):
        self.delay = delay
        self.allow_external = allow_external
        self.node_id = node_id
        self.user_agent = user_agent
        self.robots_parsers = {}
        self.output_dir = "crawled_pages"
        os.makedirs(self.output_dir, exist_ok=True)

    def can_fetch(self, url):
        parsed = urllib.parse.urlparse(url)
        base_url = f"{parsed.scheme}://{parsed.netloc}"

        if base_url not in self.robots_parsers:
            rp = RobotFileParser()
            robots_url = urllib.parse.urljoin(base_url, "/robots.txt")
            rp.set_url(robots_url)
            try:
                rp.read()
            except Exception as e:
                logging.warning(f"[{self.node_id}] robots.txt error for {robots_url}: {e}")
                rp = None
            self.robots_parsers[base_url] = rp

        rp = self.robots_parsers[base_url]
        return True if rp is None else rp.can_fetch(self.user_agent, url)

    def process_url(self, url, starting_url, depth, url_map):
        if not self.can_fetch(url):
            logging.info(f"[{self.node_id}] Blocked by robots.txt: {url}")
            return None

        time.sleep(self.delay)
        headers = {"User-Agent": self.user_agent}
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(url, headers=headers, timeout=10)
                resp.raise_for_status()
                html = resp.text
                break
            except Exception as e:
                wait = 2 ** (attempt - 1)
                logging.warning(f"[{self.node_id}] Retry {attempt}/{max_retries} for {url} in {wait}s due to: {e}")
                time.sleep(wait)
        else:
            logging.error(f"[{self.node_id}] Failed to fetch after {max_retries} attempts: {url}")
            return []

        ts = int(time.time() * 1000)
        tid = threading.get_ident()
        filename = f"{self.node_id}_{tid}_{ts}.html"
        path = os.path.join(self.output_dir, filename)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)

        url_map[url] = path

        soup = BeautifulSoup(html, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            full = urllib.parse.urljoin(url, href).split("#")[0]
            if not full.lower().startswith(("http://", "https://")):
                continue
            if not self.allow_external and urllib.parse.urlparse(full).netloc != urllib.parse.urlparse(starting_url).netloc:
                continue
            links.append(full)

        return links
