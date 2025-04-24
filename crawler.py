import os
import time
import json
import logging
from uuid import uuid4
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from aws_adapter import SqsQueue, S3Storage, DynamoState

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - CRAWLER - %(levelname)s - %(message)s")

def crawler_worker():
    node_id    = os.environ.get('NODE_ID', f"crawler-{uuid4().hex[:6]}")
    crawl_q    = SqsQueue(os.environ['CRAWL_QUEUE_URL'])
    index_q    = SqsQueue(os.environ['INDEX_QUEUE_URL'])
    storage    = S3Storage(os.environ['S3_BUCKET'])
    state      = DynamoState(os.environ['URL_TABLE'])

    delay       = float(os.environ.get('DELAY', '1'))
    max_retries = int(os.environ.get('MAX_RETRIES', '3'))
    allow_ext   = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'
    start_net   = None

    logging.info(f"{node_id} started; polling {crawl_q.url}")

    while True:
        # 1) Poll crawl queue
        print(f"DEBUG: polling crawl queue → {crawl_q.url}")
        msgs = crawl_q.receive(1, 20)
        print(f"DEBUG: received messages: {msgs}")
        if not msgs:
            continue

        m    = msgs[0]
        body = m['Body']
        # Handle non-JSON messages gracefully
        try:
            task = json.loads(body)
        except json.JSONDecodeError:
            print(f"DEBUG: skipping non-JSON message body: {body!r}")
            crawl_q.delete(m['ReceiptHandle'])
            continue

        url   = task.get('url')
        depth = task.get('depth', 1)
        rh    = m['ReceiptHandle']

        if start_net is None:
            start_net = urlparse(url).netloc

        # 2) Claim in Dynamo
        print(f"DEBUG: attempting claim_crawl for {url}")
        if not state.claim_crawl(url):
            print(f"DEBUG: claim_crawl failed, deleting message")
            crawl_q.delete(rh)
            continue

        print(f"DEBUG: state after claim: {state.get(url)}")

        # 3) Politeness delay
        print(f"DEBUG: sleeping for {delay}s before fetch")
        time.sleep(delay)

        # 4) Fetch with retries
        success = False
        for attempt in range(1, max_retries + 1):
            try:
                print(f"DEBUG: fetch attempt {attempt}/{max_retries} for {url}")
                resp = requests.get(url, headers={'User-Agent': node_id}, timeout=10)
                resp.raise_for_status()
                html = resp.text
                print(f"DEBUG: fetch succeeded for {url}")
                success = True
                break
            except Exception as e:
                backoff = 2 ** (attempt - 1)
                print(f"DEBUG: fetch error on attempt {attempt}: {e}; backoff {backoff}s")
                time.sleep(backoff)

        if not success:
            print(f"DEBUG: all retries failed; marking OPEN and deleting message")
            state.update(url, crawl_status="OPEN")
            crawl_q.delete(rh)
            continue

        # 5) Upload to S3
        s3_key = f"pages/{node_id}/{uuid4().hex}.html"
        print(f"DEBUG: uploading HTML to S3 at key={s3_key}")
        storage.upload(s3_key, html)
        state.complete_crawl(url, s3_key)

        # 6) Parse links
        soup     = BeautifulSoup(html, 'html.parser')
        children = []
        for a in soup.find_all('a', href=True):
            full = urljoin(url, a['href'].split('#')[0])
            p    = urlparse(full)
            if p.scheme not in ('http', 'https'):
                continue
            if not allow_ext and p.netloc != start_net:
                continue
            children.append(full)

        # 7) Enqueue for indexing
        print(f"DEBUG: sending to index queue → url={url}, s3_key={s3_key}")
        index_q.send({'url': url, 's3_key': s3_key})

        # 8) Enqueue children for further crawling
        if depth > 0:
            for c in children:
                print(f"DEBUG: enqueuing child → url={c}, depth={depth-1}")
                crawl_q.send({'url': c, 'depth': depth - 1})

        # 9) Delete processed message
        print(f"DEBUG: deleting message for {url}")
        crawl_q.delete(rh)

        logging.info(f"{node_id} crawled {url}, found {len(children)} links")


if __name__ == '__main__':
    crawler_worker()
