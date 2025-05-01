#!/usr/bin/env python3
"""
crawler_worker.py — Each thread acts as its own “node,” long-polling SQS,
processing one URL at a time, and writing per-thread heartbeats into the unified
`heartbeats` table. Includes SQS visibility heartbeats, robots.txt politeness,
retry/backoff, optional S3 storage, and per-job config caching.
"""

import os
import time
import logging
import threading
import json
import uuid
import socket

from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import boto3
import requests
from bs4 import BeautifulSoup

from db import get_connection
from aws_adapter import S3Storage

# ────────────────────────────────────────────────────────────────────────────────
# Configuration
CRAWL_QUEUE_URL     = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL     = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT        = int(os.environ.get('CRAWLER_THREAD_COUNT',
                         os.environ.get('THREAD_COUNT', '5')))
POLL_WAIT_TIME      = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT  = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL  = int(os.environ.get('HEARTBEAT_POLL_INTERVAL',
                         str(VISIBILITY_TIMEOUT // 2)))
DEFAULT_DELAY       = float(os.environ.get('DELAY', '1'))
MAX_RETRIES         = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL      = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'
S3_BUCKET           = os.environ.get('S3_BUCKET')
HEARTBEAT_TABLE = 'heartbeats'


# ────────────────────────────────────────────────────────────────────────────────
# Logging
logging.basicConfig(level=logging.DEBUG,
                    format='[CRAWLER] %(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("crawler")

# AWS clients
sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
s3  = S3Storage(S3_BUCKET) if S3_BUCKET else None

# In-memory caches
robot_parsers = {}               # origin -> RobotFileParser
job_config    = {}               # job_id -> (depth_limit, seed_netloc)
config_lock   = threading.Lock()

def send_heartbeat(role='crawler'):
    """
    Write a heartbeat for this thread into the unified heartbeats table.
    Uses the thread’s name as node_id.
    """
    node_id = threading.current_thread().name
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE} (node_id, role, last_heartbeat)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE last_heartbeat = NOW()
        """, (node_id, role))
    conn.commit()
    conn.close()

def crawl_task(msg):
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    job_id  = body.get('jobId')
    url     = body.get('url')
    depth   = int(body.get('depth', 0))

    if not job_id or not url:
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
        return

    # 1) SQS visibility heartbeat
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

    # 2) Per-thread RDS heartbeat
    stop_hb = threading.Event()
    def node_heartbeat():
        while not stop_hb.wait(HEARTBEAT_INTERVAL):
            try:
                send_heartbeat('crawler')
            except Exception:
                logger.exception("Failed to send crawler heartbeat")
    threading.Thread(target=node_heartbeat, daemon=True).start()

    try:
        # Initial heartbeat
        send_heartbeat('crawler')

        # 3) Fetch per-job config once
        with config_lock:
            if job_id not in job_config:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT depth_limit, seed_url FROM jobs WHERE job_id = %s",
                        (job_id,)
                    )
                    row = cur.fetchone() or {}
                conn.close()
                dl = int(row.get('depth_limit', 1))
                su = row.get('seed_url', '')
                seed_netloc = urlparse(su).netloc
                job_config[job_id] = (dl, seed_netloc)
        depth_limit, seed_netloc = job_config[job_id]

        # 4) robots.txt politeness
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = RobotFileParser(); rp.set_url(origin + "/robots.txt")
            try: rp.read()
            except Exception: logger.warning("Could not fetch robots.txt for %s", origin)
            robot_parsers[origin] = rp
        if rp and not rp.can_fetch("*", url):
            logger.info("Blocked by robots.txt: %s", url)
            sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
            return
        time.sleep(rp.crawl_delay("*") or DEFAULT_DELAY)

        # 5) Fetch with retries and backoff
        success, html = False, None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = requests.get(url, timeout=10, headers={'User-Agent': 'CrawlerWorker'})
                resp.raise_for_status()
                html, success = resp.text, True
                break
            except Exception as e:
                backoff = 2 ** (attempt - 1)
                logger.warning("Fetch error %s (attempt %d), retrying in %ds", e, attempt, backoff)
                time.sleep(backoff)
        if not success:
            logger.error("Failed to fetch %s after %d attempts", url, MAX_RETRIES)
            sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
            return

        # 6) Optional S3 upload of raw HTML
        if s3:
            key = f"pages/{job_id}/{uuid.uuid4().hex}.html"
            s3.upload(key, html)

        # 7) Update discovered_count
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET discovered_count = discovered_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        # 8) Extract text & enqueue indexing
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

        # 9) Parse links & enqueue deeper crawls
        children = []
        for a in soup.find_all('a', href=True):
            link = urljoin(url, a['href'].split('#')[0])
            p    = urlparse(link)
            if p.scheme not in ('http', 'https'):
                continue
            if not ALLOW_EXTERNAL and p.netloc != seed_netloc:
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

        logger.info("Crawled %s (depth=%d), found %d links", url, depth, len(children))

    except Exception:
        logger.exception("crawl_task error for %s", url)

    finally:
        # Stop heartbeats & ack the message
        stop_vis.set()
        stop_hb.set()
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)

def worker_loop():
    """Each thread polls for one message, processes it, then immediately loops."""
    thread_name = threading.current_thread().name
    while True:
        logger.debug("%s polling for messages…", thread_name)
        resp = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        msgs = resp.get('Messages', [])
        logger.debug("%s received %d messages", thread_name, len(msgs))
        for m in msgs:
            crawl_task(m)

def main():
    hostname = socket.gethostname()
    logger.info("Spawning %d crawler threads", THREAD_COUNT)
    for i in range(THREAD_COUNT):
        name = f"{hostname}-crawler-{i}"
        t = threading.Thread(target=worker_loop, name=name, daemon=True)
        t.start()
    # Keep main alive
    while True:
        time.sleep(60)

if __name__ == '__main__':
    main()
