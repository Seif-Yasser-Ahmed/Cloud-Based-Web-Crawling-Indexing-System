#!/usr/bin/env python3
"""
crawler_worker.py — Multi-threaded crawler merging your old crawler.py logic
with SQS heartbeats, thread auto-scaling, S3 storage, and RDS job counting.
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

# ─── Configuration ────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
# S3_BUCKET = os.environ['S3_BUCKET']

# Thread pool / SQS
MIN_THREADS = int(os.environ.get('MIN_THREADS', 2))
MAX_THREADS = int(os.environ.get('MAX_THREADS', 20))
SCALE_INTERVAL = int(os.environ.get('SCALE_INTERVAL_SEC', 30))

MSG_BATCH_SIZE = int(os.environ.get('MSG_BATCH_SIZE', 5))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', 20))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', 120))
HEARTBEAT_INTERVAL = VISIBILITY_TIMEOUT // 2
THREAD_COUNT = os.environ.get('THREAD_COUNT')  # if set, disables auto-scale

# Crawl politeness & retry
DEFAULT_DELAY = float(os.environ.get('DELAY', '1'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[CRAWLER] %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# ─── AWS Clients & Adapters ────────────────────────────────────────────────────
sqs = boto3.client('sqs')
# s3 = S3Storage(S3_BUCKET)

# robots.txt parsers per origin
robot_parsers = {}


def crawl_task(msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    url = body.get('url')
    depth = int(body.get('depth', 0))
    if not job_id or not url:
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
        return

    # 1) Heartbeat thread to extend visibility
    stop_evt = threading.Event()

    def heartbeat():
        while not stop_evt.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except Exception:
                logger.exception("Heartbeat failed")
    threading.Thread(target=heartbeat, daemon=True).start()

    try:
        # 2) robots.txt
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = RobotFileParser()
            rp.set_url(origin + "/robots.txt")
            try:
                rp.read()
            except Exception:
                logger.warning("Could not read robots.txt for %s", origin)
            robot_parsers[origin] = rp
        if rp and not rp.can_fetch("*", url):
            logger.info("Skipping by robots.txt: %s", url)
            sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
            return
        delay = rp.crawl_delay("*") or DEFAULT_DELAY
        time.sleep(delay)

        # 3) Fetch with retries
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(
                    url, headers={'User-Agent': 'CrawlerWorker'}, timeout=10)
                resp.raise_for_status()
                html = resp.text
                success = True
                break
            except Exception as e:
                backoff = 2**(attempt-1)
                logger.warning(
                    "Fetch error %s (attempt %d), backoff %ds", e, attempt, backoff)
                time.sleep(backoff)
        if not success:
            logger.error("Failed all retries for %s", url)
            sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
            return

        # 4) Upload HTML to S3
        key = f"pages/{job_id}/{uuid4().hex}.html"
        # s3.upload(key, html)

        # 5) Update RDS discovered_count
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET discovered_count = discovered_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.close()

        # 6) Extract text and enqueue index task
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

        # 7) Parse links & enqueue deeper crawls
        depth_limit = fetch_depth_limit(job_id)
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
        if depth < depth_limit:
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
        # ack + stop heartbeat
        stop_evt.set()
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
        target = min(max(backlog // 5 + 1, MIN_THREADS), MAX_THREADS)
        if executor._max_workers != target:
            logger.info("Resizing threads: %d → %d",
                        executor._max_workers, target)
            executor._max_workers = target
    except Exception:
        logger.exception("Thread adjust error")


def main():
    # pick initial size
    if THREAD_COUNT:
        size, auto_scale = int(THREAD_COUNT), False
    else:
        size, auto_scale = MIN_THREADS, True

    executor = ThreadPoolExecutor(max_workers=size)
    logger.info("Starting crawler (%d threads, auto_scale=%s)",
                size, auto_scale)

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
