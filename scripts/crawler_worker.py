#!/usr/bin/env python3
"""
crawler_worker.py — High‐throughput crawler worker with per-thread RDS monitoring.

Each thread long‐polls SQS, processes one message end-to-end, and immediately loops again.
Each thread writes its own heartbeat (node_id=<host>-t<index>) with role, state, and current URL.
"""

import os
import json
import time
import logging
import threading
import socket
from uuid import uuid4
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import boto3
import requests
from bs4 import BeautifulSoup

from aws_adapter import S3Storage
from db import get_connection

# ─── Configuration ─────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
S3_BUCKET = os.environ.get('S3_BUCKET')

# Concurrency settings
THREAD_COUNT = int(os.environ.get('THREAD_COUNT',
                                  os.environ.get('MAX_THREADS', '5')))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', '30'))
DEFAULT_DELAY = float(os.environ.get('DELAY', '1'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'

# Monitoring / identification
NODE_BASE = os.environ.get('NODE_ID') or socket.gethostname()
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')
ROLE = 'crawler'

# ─── Logging & AWS clients ─────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='[CRAWLER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
s3 = S3Storage(S3_BUCKET) if S3_BUCKET else None

# ─── Caches ────────────────────────────────────────────────────────────────────
robot_parsers = {}   # origin → RobotFileParser
job_config = {}   # job_id → (depth_limit, seed_netloc)
job_config_lock = threading.Lock()

# ─── Monitoring helper ─────────────────────────────────────────────────────────


def update_state(node_id: str, state: str, current_url: str = None):
    """
    Insert/update this thread's heartbeat record with role, state, and current URL.
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE}
              (node_id, role, last_heartbeat, state, current_url)
            VALUES (%s, %s, NOW(), %s, %s)
            ON DUPLICATE KEY UPDATE
              last_heartbeat = NOW(),
              state          = VALUES(state),
              current_url    = VALUES(current_url)
        """, (node_id, ROLE, state, current_url))
    conn.commit()
    conn.close()

# ─── Core crawl task ────────────────────────────────────────────────────────────


def crawl_task(thread_id: str, msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    url = body.get('url')
    depth = int(body.get('depth', 0))

    if not job_id or not url:
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
        return

    # mark waiting
    update_state(thread_id, 'waiting')

    # SQS visibility heartbeat
    stop_vis = threading.Event()

    def vis_heartbeat():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except:
                logger.exception("[%s] visibility heartbeat failed", thread_id)
    threading.Thread(target=vis_heartbeat, daemon=True).start()

    try:
        # mark processing
        update_state(thread_id, 'processing', url)

        # --- Fetch per-job config once ---
        with job_config_lock:
            if job_id not in job_config:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT depth_limit, seed_url FROM jobs WHERE job_id = %s",
                        (job_id,)
                    )
                    row = cur.fetchone() or {}
                conn.close()
                depth_limit = int(row.get('depth_limit', 1))
                seed_netloc = urlparse(row.get('seed_url', '')).netloc
                job_config[job_id] = (depth_limit, seed_netloc)
            depth_limit, seed_netloc = job_config[job_id]

        # robots.txt politeness
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = RobotFileParser()
            rp.set_url(origin + "/robots.txt")
            try:
                rp.read()
            except:
                logger.warning(
                    "[%s] can't read robots.txt for %s", thread_id, origin)
            robot_parsers[origin] = rp
        if rp and not rp.can_fetch("*", url):
            logger.info("[%s] blocked by robots.txt: %s", thread_id, url)
            return
        time.sleep(rp.crawl_delay("*") or DEFAULT_DELAY)

        # fetch + retry/backoff
        success, html = False, None
        for i in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, timeout=10, headers={
                                 'User-Agent': 'CrawlerWorker'})
                r.raise_for_status()
                html, success = r.text, True
                break
            except Exception as e:
                backoff = 2 ** (i - 1)
                logger.warning(
                    "[%s] fetch error %s (attempt %d), retry in %ds", thread_id, e, i, backoff)
                time.sleep(backoff)
        if not success:
            logger.error("[%s] failed to fetch %s", thread_id, url)
            return

        # optional S3 upload
        if s3:
            key = f"pages/{job_id}/{uuid4().hex}.html"
            s3.upload(key, html)

        # update discovered_count
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET discovered_count = discovered_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        # extract & enqueue indexing
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        sqs.send_message(
            QueueUrl=INDEX_QUEUE_URL,
            MessageBody=json.dumps({
                'jobId':   job_id,
                'pageUrl': url,
                'content': text
            })
        )

        # enqueue deeper crawls
        children = []
        for a in soup.find_all('a', href=True):
            link = urljoin(url, a['href'].split('#')[0])
            p2 = urlparse(link)
            if p2.scheme not in ('http', 'https'):
                continue
            if not ALLOW_EXTERNAL and p2.netloc != seed_netloc:
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

        logger.info("[%s] crawled %s (depth %d → %d links)",
                    thread_id, url, depth, len(children))

    except Exception:
        logger.exception("[%s] error processing %s", thread_id, url)

    finally:
        stop_vis.set()
        update_state(thread_id, 'waiting')
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)

# ─── Worker loop ───────────────────────────────────────────────────────────────


def worker_loop(thread_id: str):
    # initial heartbeat
    update_state(thread_id, 'waiting')
    while True:
        resp = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            crawl_task(thread_id, msg)
        # immediately loop again


# ─── Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    logger.info("Starting %d crawler threads on %s", THREAD_COUNT, NODE_BASE)
    for i in range(THREAD_COUNT):
        tid = f"{NODE_BASE}-t{i}"
        t = threading.Thread(target=worker_loop, args=(tid,), daemon=True)
        t.start()
    while True:
        time.sleep(60)
