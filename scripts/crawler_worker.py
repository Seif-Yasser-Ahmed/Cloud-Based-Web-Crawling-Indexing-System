# crawler_worker.py
#!/usr/bin/env python3
"""
crawler_worker.py — Multi-threaded crawler with SQS & RDS heartbeats,
S3 storage, RDS job counting, robots.txt politeness, retry/backoff, and depth control.
"""

import os
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from uuid import uuid4
from urllib.robotparser import RobotFileParser

import boto3
import requests
from bs4 import BeautifulSoup

from aws_adapter import S3Storage
from db import get_connection

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
S3_BUCKET = os.environ.get('S3_BUCKET')

MIN_THREADS = int(os.environ.get('MIN_THREADS', 2))
MAX_THREADS = int(os.environ.get('MAX_THREADS', 20))
SCALE_INTERVAL = int(os.environ.get('SCALE_INTERVAL_SEC', 30))
MSG_BATCH_SIZE = int(os.environ.get('MSG_BATCH_SIZE', 5))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', 20))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', 120))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', 30))
DEFAULT_DELAY = float(os.environ.get('DELAY', '1'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'

NODE_ID = os.environ['NODE_ID']            # unique per worker
HEARTBEAT_TABLE = os.environ['HEARTBEAT_TABLE']    # e.g. 'crawler_heartbeats'

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='[CRAWLER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
s3 = S3Storage(S3_BUCKET) if S3_BUCKET else None

robot_parsers = {}


def send_node_heartbeat():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE} (node_id, last_heartbeat)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE last_heartbeat = NOW()
        """, (NODE_ID,))
    conn.commit()
    conn.close()


def crawl_task(msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    url = body.get('url')
    depth = int(body.get('depth', 0))
    if not job_id or not url:
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
        return

    # SQS-visibility heartbeat
    stop_vis = threading.Event()

    def vis_heartbeat():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except Exception:
                logger.exception("Failed to extend SQS visibility")
    threading.Thread(target=vis_heartbeat, daemon=True).start()

    # RDS node heartbeat
    stop_node_hb = threading.Event()

    def node_heartbeat_loop():
        while not stop_node_hb.wait(HEARTBEAT_INTERVAL):
            try:
                send_node_heartbeat()
            except Exception:
                logger.exception("Node heartbeat error")
    threading.Thread(target=node_heartbeat_loop, daemon=True).start()

    try:
        # robots.txt politeness
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = RobotFileParser()
            rp.set_url(origin + "/robots.txt")
            try:
                rp.read()
            except Exception:
                logger.warning("Could not fetch robots.txt for %s", origin)
            robot_parsers[origin] = rp
        if rp and not rp.can_fetch("*", url):
            logger.info("Blocked by robots.txt: %s", url)
            sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
            return
        delay = rp.crawl_delay("*") or DEFAULT_DELAY
        time.sleep(delay)

        # fetch + retry/backoff
        success, html = False, ""
        for attempt in range(1, MAX_RETRIES+1):
            try:
                r = requests.get(
                    url, headers={'User-Agent': 'CrawlerWorker'}, timeout=10)
                r.raise_for_status()
                html, success = r.text, True
                break
            except Exception as e:
                backoff = 2**(attempt-1)
                logger.warning(
                    "Fetch error %s (attempt %d), retry in %ds", e, attempt, backoff)
                time.sleep(backoff)
        if not success:
            logger.error("Failed to fetch %s", url)
            return

        # optionally store raw HTML
        if s3:
            key = f"pages/{job_id}/{uuid4().hex}.html"
            s3.upload(key, html)

        # update discovered_count
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET discovered_count = discovered_count + 1 WHERE job_id = %s", (job_id,))
        conn.commit()
        conn.close()

        # extract & enqueue index
        soup = BeautifulSoup(html, 'html.parser')
        content = soup.get_text()
        sqs.send_message(
            QueueUrl=INDEX_QUEUE_URL,
            MessageBody=json.dumps({
                'jobId':   job_id,
                'pageUrl': url,
                'content': content
            })
        )

        # enqueue deeper links
        seed_netloc = fetch_seed_netloc(job_id)
        children = []
        for a in soup.find_all('a', href=True):
            link = urljoin(url, a['href'].split('#')[0])
            pp = urlparse(link)
            if pp.scheme not in ('http', 'https'):
                continue
            if not ALLOW_EXTERNAL and pp.netloc != seed_netloc:
                continue
            children.append(link)

        if depth < fetch_depth_limit(job_id):
            for child in children:
                sqs.send_message(
                    QueueUrl=CRAWL_QUEUE_URL,
                    MessageBody=json.dumps({
                        'jobId': job_id,
                        'url':   child,
                        'depth': depth + 1
                    })
                )

        logger.info("Crawled %s (depth %d), found %d links",
                    url, depth, len(children))

    except Exception:
        logger.exception("Error processing %s", url)

    finally:
        stop_vis.set()
        stop_node_hb.set()
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)


def fetch_depth_limit(job_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT depth_limit FROM jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone() or {}
    conn.close()
    return int(row.get('depth_limit', 1))


def fetch_seed_netloc(job_id):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT seed_url FROM jobs WHERE job_id=%s", (job_id,))
        row = cur.fetchone() or {}
    conn.close()
    return urlparse(row.get('seed_url', '')).netloc


def adjust_threads(executor):
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=CRAWL_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )['Attributes']
        backlog = int(attrs.get('ApproximateNumberOfMessages', 0))
        target = min(max(backlog//5 + 1, MIN_THREADS), MAX_THREADS)
        if executor._max_workers != target:
            logger.info("Resizing threads: %d → %d",
                        executor._max_workers, target)
            executor._max_workers = target
    except Exception:
        logger.exception("Thread adjust error")


def main():
    executor = ThreadPoolExecutor(max_workers=MIN_THREADS)
    auto_scale = True

    logger.info("Starting crawler_worker (min=%d,max=%d)",
                MIN_THREADS, MAX_THREADS)
    while True:
        resp = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=MSG_BATCH_SIZE,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for m in resp.get('Messages', []):
            executor.submit(crawl_task, m)
        if auto_scale:
            adjust_threads(executor)
        time.sleep(SCALE_INTERVAL)


if __name__ == '__main__':
    main()
