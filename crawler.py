# crawler.py
import os
import time
import json
import logging
from uuid import uuid4
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from aws_adapter import SqsQueue, S3Storage, DynamoState, HeartbeatManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - CRAWLER - %(levelname)s - %(message)s")

def crawler_worker():
    node_id = os.environ.get('NODE_ID', f"crawler-{uuid4().hex[:6]}")
    crawl_q = SqsQueue(os.environ['CRAWL_QUEUE_URL'])
    index_q = SqsQueue(os.environ['INDEX_QUEUE_URL'])
    storage = S3Storage(os.environ['S3_BUCKET'])
    state = DynamoState(os.environ['URL_TABLE'])
    hb_mgr = HeartbeatManager(os.environ['HEARTBEAT_TABLE'], timeout=int(os.environ.get('HEARTBEAT_TIMEOUT','60')))

    delay = float(os.environ.get('DELAY', '1'))
    max_retries = int(os.environ.get('MAX_RETRIES', '3'))
    allow_ext = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'
    start_net = None

    while True:
        # heartbeat
        hb_mgr.update(node_id)

        msgs = crawl_q.receive(max_messages=1, wait=20)
        if not msgs:
            continue

        for m in msgs:
            task = json.loads(m['Body'])
            url = task['url']
            depth = task.get('depth', 1)
            receipt = m['ReceiptHandle']

            if start_net is None:
                start_net = urlparse(url).netloc

            if not state.claim_crawl(url):
                crawl_q.delete(receipt)
                continue

            item = state.get(url)
            if item.get('tries', 0) > max_retries:
                logging.warning(f"{node_id} giving up {url} after {item['tries']} tries")
                state.update(url, crawl_status="FAILED")
                crawl_q.delete(receipt)
                continue

            time.sleep(delay)
            success = False
            for attempt in range(1, max_retries+1):
                try:
                    resp = requests.get(url, headers={'User-Agent': node_id}, timeout=10)
                    resp.raise_for_status()
                    html = resp.text
                    success = True
                    break
                except Exception as e:
                    backoff = 2**(attempt-1)
                    logging.warning(f"{node_id} retry {attempt}/{max_retries} in {backoff}s: {e}")
                    time.sleep(backoff)
            if not success:
                state.update(url, crawl_status="OPEN")
                crawl_q.delete(receipt)
                continue

            s3_key = f"pages/{node_id}/{uuid4().hex}.html"
            storage.upload(s3_key, html)
            state.complete_crawl(url, s3_key)

            soup = BeautifulSoup(html, 'html.parser')
            children = []
            for a in soup.find_all('a', href=True):
                full = urljoin(url, a['href'].split('#')[0])
                p = urlparse(full)
                if p.scheme not in ('http','https'):
                    continue
                if not allow_ext and p.netloc != start_net:
                    continue
                children.append(full)

            index_q.send({'url': url, 's3_key': s3_key})

            if depth > 0:
                for c in children:
                    if not state.get(c):
                        crawl_q.send({'url': c, 'depth': depth-1})

            crawl_q.delete(receipt)
            logging.info(f"{node_id} crawled {url}, found {len(children)} links")

if __name__ == "__main__":
    crawler_worker()
