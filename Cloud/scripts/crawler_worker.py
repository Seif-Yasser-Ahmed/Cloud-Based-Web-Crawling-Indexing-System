#!/usr/bin/env python3
"""
crawler_worker.py — High‐throughput crawler with dynamic thread scaling
and per-thread RDS monitoring.

Spawns an initial pool of MIN_THREADS threads. A scaler thread polls SQS
every SCALE_INTERVAL seconds and, based on the backlog, spins up new threads
(on demand) up to MAX_THREADS. Each thread long‐polls SQS, processes one message
end-to-end, updates its own heartbeat (node_id="<host>-t<index>"), then loops.
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

MIN_THREADS = int(os.environ.get('MIN_THREADS', '2'))
MAX_THREADS = int(os.environ.get('MAX_THREADS', '20'))
SCALE_INTERVAL = int(os.environ.get('SCALE_INTERVAL_SEC', '30'))
MSG_BATCH_SIZE = int(os.environ.get('MSG_BATCH_SIZE', '5'))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', '30'))
DEFAULT_DELAY = float(os.environ.get('DELAY', '1'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'

NODE_BASE = os.environ.get('NODE_ID') or socket.gethostname()
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')
ROLE = 'crawler'

# ─── Logging & AWS clients ─────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='[CRAWLER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
s3 = S3Storage(S3_BUCKET) if S3_BUCKET else None

# ─── Caches & state ────────────────────────────────────────────────────────────
robot_parsers = {}   # origin → RobotFileParser
job_config = {}   # job_id → (depth_limit, seed_netloc)
job_config_lock = threading.Lock()

threads_lock = threading.Lock()
current_threads = []   # list of thread IDs, e.g. ["ip-...-t0", "ip-...-t1", ...]

# ─── Monitoring helper ─────────────────────────────────────────────────────────


def update_state(thread_id: str, state: str, current_url: str = None):
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
        """, (thread_id, ROLE, state, current_url))
    conn.commit()
    conn.close()

# ─── Core crawl task ────────────────────────────────────────────────────────────


def crawl_task(thread_id: str, msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    url = body.get('url')
    depth = int(body.get('depth', 0))

    # Mandatory fields
    if not job_id or not url:
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
        return

    update_state(thread_id, 'waiting')

    # Start visibility heartbeat
    stop_vis = threading.Event()
    def vis_hb():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except Exception:
                logger.exception(f"[{thread_id}] SQS heartbeat failed")
    threading.Thread(target=vis_hb, daemon=True).start()

    try:
        # Fetch job configuration (depth_limit, seed_netloc, domain_flag)
        with job_config_lock:
            if job_id not in job_config:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT depth_limit, seed_url, domain FROM jobs WHERE job_id=%s",
                        (job_id,)
                    )
                    row = cur.fetchone() or {}
                conn.close()
                depth_limit = int(row.get('depth_limit', 1))
                seed_netloc = urlparse(row.get('seed_url', '')).netloc
                domain_flag = bool(row.get('domain', False))
                job_config[job_id] = (depth_limit, seed_netloc, domain_flag)
            depth_limit, seed_netloc, domain_flag = job_config[job_id]

        update_state(thread_id, 'processing', url)

        # robots.txt handling
        p = urlparse(url)
        origin = f"{p.scheme}://{p.netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = RobotFileParser()
            rp.set_url(origin + "/robots.txt")
            try:
                rp.read()
            except Exception:
                logger.warning(f"[{thread_id}] can't read robots.txt for {origin}")
            robot_parsers[origin] = rp
        if rp and not rp.can_fetch("*", url):
            logger.info(f"[{thread_id}] blocked by robots.txt: {url}")
            return
        time.sleep(rp.crawl_delay("*") or DEFAULT_DELAY)

        # Fetch page with retries
        success, html = False, None
        for i in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, timeout=10, headers={'User-Agent': 'CrawlerWorker'})
                r.raise_for_status()
                html, success = r.text, True
                break
            except Exception:
                time.sleep(2 ** (i - 1))
        if not success:
            logger.error(f"[{thread_id}] fetch failed: {url}")
            return

        # Optional upload to S3
        if s3:
            key = f"pages/{job_id}/{uuid4().hex}.html"
            s3.upload(key, html)

        # Update discovered count
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET discovered_count = discovered_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        # Extract and enqueue index task (include domain_flag)
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        sqs.send_message(
            QueueUrl=INDEX_QUEUE_URL,
            MessageBody=json.dumps({
                'jobId': job_id,
                'pageUrl': url,
                'content': text,
                'domain': domain_flag
            })
        )

        # Enqueue deeper links based on domain_flag
        children = []
        for a in soup.find_all('a', href=True):
            link = urljoin(url, a['href'].split('#')[0])
            pp = urlparse(link)
            if pp.scheme not in ('http', 'https'):
                continue
            if not domain_flag and pp.netloc != seed_netloc:
                continue
            children.append(link)
        if depth < depth_limit:
            for link in children:
                sqs.send_message(
                    QueueUrl=CRAWL_QUEUE_URL,
                    MessageBody=json.dumps({
                        'jobId': job_id,
                        'url': link,
                        'depth': depth + 1,
                        "domain": domain_flag
                    })
                )

        logger.info(f"[{thread_id}] crawled {url} depth={depth} links={len(children)}")

    except Exception:
        logger.exception(f"[{thread_id}] error processing {url}")

    finally:
        # Clean up visibility heartbeat and DB state
        stop_vis.set()
        update_state(thread_id, 'waiting')
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)

# ─── Worker loop ───────────────────────────────────────────────────────────────


def worker_loop(thread_id: str):
    update_state(thread_id, 'waiting')
    while True:
        resp = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for m in resp.get('Messages', []):
            crawl_task(thread_id, m)

# ─── Scaler loop ───────────────────────────────────────────────────────────────


def scaler_loop():
    next_index = len(current_threads)
    while True:
        time.sleep(SCALE_INTERVAL)
        try:
            # fetch backlog
            attrs = sqs.get_queue_attributes(
                QueueUrl=CRAWL_QUEUE_URL,
                AttributeNames=['ApproximateNumberOfMessages']
            )['Attributes']
            backlog = int(attrs.get('ApproximateNumberOfMessages', 0))
            # desired threads = between MIN and MAX, 1 thread per batch
            desired = min(max(MIN_THREADS, backlog //
                          MSG_BATCH_SIZE+1), MAX_THREADS)
        except Exception:
            logger.exception("Scaler failed to fetch backlog")
            continue

        with threads_lock:
            current = len(current_threads)
            if desired > current:
                for _ in range(desired-current):
                    tid = f"{NODE_BASE}-t{next_index}"
                    t = threading.Thread(
                        target=worker_loop, args=(tid,), daemon=True)
                    t.start()
                    current_threads.append(tid)
                    next_index += 1
                logger.info("Scaled up threads: %d → %d",
                            current, len(current_threads))


# ─── Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # start with MIN_THREADS
    for i in range(MIN_THREADS):
        tid = f"{NODE_BASE}-t{i}"
        t = threading.Thread(target=worker_loop, args=(tid,), daemon=True)
        t.start()
        current_threads.append(tid)
    logger.info("Launched %d initial threads", MIN_THREADS)

    # start scaler
    threading.Thread(target=scaler_loop, daemon=True).start()

    # keep alive
    while True:
        time.sleep(60)
