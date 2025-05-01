# crawler_worker.py
#!/usr/bin/env python3
"""
crawler_worker.py — Each thread is its own "node," long-polling SQS,
running crawl_task(msg), and heartbeating into `heartbeats` via thread-name.
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
from enum import Enum

import boto3
import requests
from bs4 import BeautifulSoup

from db import get_connection
from aws_adapter import S3Storage

# Define thread states as an Enum


class ThreadState(Enum):
    WAITING = "waiting"
    PROCESSING = "processing"
    IDLE = "idle"


# ────────────────────────────────────────────────────────────────────────────────
# Config
CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT = int(os.environ.get('CRAWLER_THREAD_COUNT',
                                  os.environ.get('THREAD_COUNT', '5')))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL',
                                        str(VISIBILITY_TIMEOUT // 2)))
DEFAULT_DELAY = float(os.environ.get('DELAY', '1'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'
S3_BUCKET = os.environ.get('S3_BUCKET')
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')

# ────────────────────────────────────────────────────────────────────────────────
# Logging
logging.basicConfig(level=logging.INFO,
                    format='[CRAWLER] %(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("crawler_worker")

# AWS clients
sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
s3 = S3Storage(S3_BUCKET) if S3_BUCKET else None

# In-memory caches
robot_parsers = {}      # origin → RobotFileParser
job_config = {}      # jobId → (depth_limit, seed_netloc)
config_lock = threading.Lock()
thread_states = {}      # thread_name → state


def send_heartbeat(role='crawler', state=ThreadState.IDLE, current_url=None):
    """Insert/update a row in `heartbeats` using thread-name as node_id."""
    node_id = threading.current_thread().name
    thread_states[node_id] = state.value

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE} 
            (node_id, role, last_heartbeat, state, current_url)
            VALUES (%s, %s, NOW(), %s, %s)
            ON DUPLICATE KEY UPDATE 
            last_heartbeat = NOW(), state = %s, current_url = %s
        """, (node_id, role, state.value, current_url, state.value, current_url))
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

    # Update thread state to processing with current URL
    send_heartbeat('crawler', ThreadState.PROCESSING, url)

    # 1) SQS visibility extender
    stop_vis = threading.Event()

    def extend_visibility():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except:
                logger.exception("extend_visibility failed")
    threading.Thread(target=extend_visibility, daemon=True).start()

    # 2) Per-thread RDS heartbeat
    stop_hb = threading.Event()

    def thread_heartbeat():
        while not stop_hb.wait(HEARTBEAT_INTERVAL):
            try:
                # Use the same state and URL as the main thread
                current_state = thread_states.get(
                    threading.current_thread().name, ThreadState.PROCESSING)
                send_heartbeat('crawler', current_state, url)
            except:
                logger.exception("send_heartbeat failed")
    thread_name = threading.current_thread().name
    hb_thread = threading.Thread(
        target=thread_heartbeat,
        name=f"{thread_name}-heartbeat",
        daemon=True
    )
    hb_thread.start()

    try:
        # 3) Load or cache job config
        with config_lock:
            if job_id not in job_config:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute("SELECT depth_limit, seed_url FROM jobs WHERE job_id=%s",
                                (job_id,))
                    row = cur.fetchone() or {}
                conn.close()
                dl = int(row.get('depth_limit', 1))
                su = row.get('seed_url', '')
                job_config[job_id] = (dl, urlparse(su).netloc)
        depth_limit, seed_netloc = job_config[job_id]

        # 4) robots.txt politeness
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = RobotFileParser()
            rp.set_url(origin + "/robots.txt")
            try:
                rp.read()
            except:
                logger.warning("robots.txt read fail %s", origin)
            robot_parsers[origin] = rp
        if rp and not rp.can_fetch("*", url):
            logger.info("blocked by robots.txt %s", url)
            sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
            return
        time.sleep(rp.crawl_delay("*") or DEFAULT_DELAY)

        # 5) Fetch + backoff
        success, html = False, None
        for i in range(1, MAX_RETRIES+1):
            try:
                r = requests.get(url, timeout=10,
                                 headers={'User-Agent': 'CrawlerWorker'})
                r.raise_for_status()
                html, success = r.text, True
                break
            except Exception as e:
                backoff = 2**(i-1)
                logger.warning(
                    "fetch error %s (attempt %d), backoff %ds", e, i, backoff)
                time.sleep(backoff)
        if not success:
            logger.error("give up fetching %s", url)
            return

        # 6) Optional S3 store
        if s3:
            key = f"pages/{job_id}/{uuid.uuid4().hex}.html"
            s3.upload(key, html)

        # 7) Update discovered_count
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET discovered_count = discovered_count + 1 WHERE job_id=%s",
                        (job_id,))
        conn.commit()
        conn.close()

        # 8) Extract & enqueue index
        text = BeautifulSoup(html, 'html.parser').get_text()
        sqs.send_message(
            QueueUrl=INDEX_QUEUE_URL,
            MessageBody=json.dumps(
                {'jobId': job_id, 'pageUrl': url, 'content': text})
        )

        # 9) Enqueue children if depth < limit
        children = []
        for a in BeautifulSoup(html, 'html.parser').find_all('a', href=True):
            link = urljoin(url, a['href'].split('#')[0])
            pp = urlparse(link)
            if pp.scheme not in ('http', 'https'):
                continue
            if not ALLOW_EXTERNAL and pp.netloc != seed_netloc:
                continue
            children.append(link)
        if depth < depth_limit:
            for ch in children:
                sqs.send_message(
                    QueueUrl=CRAWL_QUEUE_URL,
                    MessageBody=json.dumps(
                        {'jobId': job_id, 'url': ch, 'depth': depth+1})
                )

        logger.info("crawled %s depth=%d links=%d", url, depth, len(children))

    except Exception:
        logger.exception("crawl_task error %s", url)
    finally:
        stop_vis.set()
        stop_hb.set()
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
        # Reset state to waiting when done
        send_heartbeat('crawler', ThreadState.WAITING, None)


def worker_loop():
    name = threading.current_thread().name
    thread_states[name] = ThreadState.WAITING.value
    send_heartbeat('crawler', ThreadState.WAITING, None)

    while True:
        logger.debug("%s polling…", name)
        thread_states[name] = ThreadState.WAITING.value
        send_heartbeat('crawler', ThreadState.WAITING, None)

        resp = sqs.receive_message(QueueUrl=CRAWL_QUEUE_URL,
                                   MaxNumberOfMessages=1,
                                   WaitTimeSeconds=POLL_WAIT_TIME)
        msgs = resp.get('Messages', [])
        logger.debug("%s got %d msgs", name, len(msgs))
        for m in msgs:
            crawl_task(m)


def main():
    host = socket.gethostname()
    logger.info("starting %d crawler threads", THREAD_COUNT)

    # Create a heartbeat table if it doesn't exist
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {HEARTBEAT_TABLE} (
                node_id VARCHAR(255) PRIMARY KEY,
                role VARCHAR(50) NOT NULL,
                last_heartbeat DATETIME NOT NULL,
                state VARCHAR(50) DEFAULT 'waiting',
                current_url TEXT NULL
            ) ENGINE=InnoDB
        """)
    conn.commit()
    conn.close()

    for i in range(THREAD_COUNT):
        t = threading.Thread(target=worker_loop,
                             name=f"{host}-crawler-{i}",
                             daemon=True)
        t.start()
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
