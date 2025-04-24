import os, time, json, logging
from uuid import uuid4
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from aws_adapter import SqsQueue, S3Storage, DynamoState

# Configure logging
to=logging.basicConfig(level=logging.INFO, format="%(asctime)s - CRAWLER - %(levelname)s - %(message)s")

def crawler_worker():
    node_id = os.environ.get('NODE_ID', f"crawler-{uuid4().hex[:6]}")
    crawl_q = SqsQueue(os.environ['CRAWL_QUEUE_URL'])
    index_q = SqsQueue(os.environ['INDEX_QUEUE_URL'])
    storage = S3Storage(os.environ['S3_BUCKET'])
    state = DynamoState(os.environ['URL_TABLE'])

    delay = float(os.environ.get('DELAY', '1'))
    max_retries = int(os.environ.get('MAX_RETRIES', '3'))
    allow_ext = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'
    start_net = None

    logging.info(f"{node_id} started, polling queue: {crawl_q.url}")

    while True:
        # Debug: poll the crawl queue
        print(f"DEBUG: polling crawl queue URL={crawl_q.url} for messages...")
        msgs = crawl_q.receive(1, 20)
        print(f"DEBUG: received messages: {msgs}")
        if not msgs:
            continue

        m = msgs[0]
        task = json.loads(m['Body'])
        url = task.get('url')
        depth = task.get('depth', 1)
        rh = m['ReceiptHandle']

        if start_net is None:
            start_net = urlparse(url).netloc

        print(f"DEBUG: attempting to claim crawl for URL={url}")
        if not state.claim_crawl(url):
            print(f"DEBUG: claim_crawl failed for {url}, deleting message")
            crawl_q.delete(rh)
            continue

        item = state.get(url)
        print(f"DEBUG: state after claim: {item}")

        # Politeness delay
        print(f"DEBUG: sleeping for {delay}s before fetch")
        time.sleep(delay)

        # Fetch with retries
        success = False
        for attempt in range(1, max_retries + 1):
            try:
                print(f"DEBUG: fetching URL={url}, attempt={attempt}/{max_retries}")
                resp = requests.get(url, headers={'User-Agent': node_id}, timeout=10)
                resp.raise_for_status()
                html = resp.text
                success = True
                print(f"DEBUG: fetch succeeded for {url}")
                break
            except Exception as e:
                backoff = 2 ** (attempt - 1)
                print(f"DEBUG: fetch error on attempt {attempt} for {url}: {e}")
                time.sleep(backoff)

        if not success:
            print(f"DEBUG: all retries failed for {url}, marking OPEN and deleting message")
            state.update(url, crawl_status="OPEN")
            crawl_q.delete(rh)
            continue

        # Upload HTML to S3
        s3_key = f"pages/{node_id}/{uuid4().hex}.html"
        print(f"DEBUG: uploading HTML to S3 at key={s3_key}")
        storage.upload(s3_key, html)
        state.complete_crawl(url, s3_key)

        # Extract links
        soup = BeautifulSoup(html, 'html.parser')
        children = []
        for a in soup.find_all('a', href=True):
            full = urljoin(url, a['href'].split('#')[0])
            p = urlparse(full)
            if p.scheme not in ('http', 'https'):
                continue
            if not allow_ext and p.netloc != start_net:
                continue
            children.append(full)

        # Enqueue for indexing
        print(f"DEBUG: sending to index queue URL={url}, s3_key={s3_key}")
        index_q.send({'url': url, 's3_key': s3_key})

        # Enqueue children for further crawling
        if depth > 0:
            for c in children:
                if not state.get(c):
                    print(f"DEBUG: enqueuing child URL={c} with depth={depth-1}")
                    crawl_q.send({'url': c, 'depth': depth - 1})

        # Delete the processed message
        print(f"DEBUG: deleting message for URL={url}")
        crawl_q.delete(rh)
        logging.info(f"{node_id} crawled {url}, found {len(children)} links")


if __name__ == '__main__':
    crawler_worker()
