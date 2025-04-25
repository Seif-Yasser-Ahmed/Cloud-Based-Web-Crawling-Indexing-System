#!/usr/bin/env python3
"""
crawler_worker.py — Merged multi-threaded crawler with:
  • SQS heartbeat & auto-scaling threads
  • robots.txt compliance & retry/back-off
  • S3 storage and DynamoState tracking
  • RDS job discovered_count updates
  • Enqueueing both crawl & index tasks
"""

import os, json, time, logging, threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
from uuid import uuid4
from datetime import datetime, timezone
import hashlib

import boto3
import requests
from bs4 import BeautifulSoup
from aws_adapter import SqsQueue, S3Storage, DynamoState
from db import get_connection

# ─── Config ───────────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL    = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL    = os.environ['INDEX_TASK_QUEUE']
S3_BUCKET          = os.environ['S3_BUCKET']
STATE_TABLE        = os.environ['URL_TABLE']

MIN_THREADS        = int(os.environ.get('MIN_THREADS', 2))
MAX_THREADS        = int(os.environ.get('MAX_THREADS', 20))
SCALE_INTERVAL     = int(os.environ.get('SCALE_INTERVAL_SEC', 30))

MSG_BATCH_SIZE     = int(os.environ.get('MSG_BATCH_SIZE', 5))
POLL_WAIT_TIME     = int(os.environ.get('POLL_WAIT_TIME_SEC', 20))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', 120))
HEARTBEAT_INTERVAL = VISIBILITY_TIMEOUT // 2

THREAD_COUNT       = os.environ.get('THREAD_COUNT')  # if set, disables auto-scale

# crawl politeness & retry
DEFAULT_DELAY      = float(os.environ.get('DELAY', '1'))
MAX_RETRIES        = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL     = os.environ.get('ALLOW_EXTERNAL','false').lower()=='true'

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [CRAWLER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ─── AWS Clients & Adapters ────────────────────────────────────────────────────
sqs        = boto3.client('sqs')
crawl_q    = SqsQueue(CRAWL_QUEUE_URL)
index_q    = SqsQueue(INDEX_QUEUE_URL)
storage    = S3Storage(S3_BUCKET)
state      = DynamoState(STATE_TABLE)

# robots.txt parsers cache
robot_parsers = {}
start_netlock = threading.Lock()

def crawl_task(msg):
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    url     = body.get('url')
    depth   = int(body.get('depth', 0))
    job_id  = body.get('jobId')
    if not url or job_id is None:
        crawl_q.delete(receipt)
        return

    # start the heartbeat thread
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
                logger.exception("Heartbeat error")
    threading.Thread(target=heartbeat, daemon=True).start()

    try:
        # 1) Claim URL in DynamoState
        if not state.claim_crawl(url):
            crawl_q.delete(receipt)
            return

        # 2) robots.txt
        origin = f"{urlparse(url).scheme}://{urlparse(url).netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = requests.get(origin + "/robots.txt", timeout=5)
            parser = []
            try:
                from urllib.robotparser import RobotFileParser
                rfp = RobotFileParser()
                rfp.parse(rp.text.splitlines())
                robot_parsers[origin] = rfp
            except:
                robot_parsers[origin] = None
        rfp = robot_parsers.get(origin)
        if rfp and not rfp.can_fetch("*", url):
            state.update(url, crawl_status="SKIPPED_ROBOT")
            crawl_q.delete(receipt)
            return
        delay = rfp.crawl_delay("*") if (rfp and rfp.crawl_delay("*")) else DEFAULT_DELAY
        time.sleep(delay)

        # 3) Fetch with retries
        success = False
        for attempt in range(1, MAX_RETRIES+1):
            try:
                resp = requests.get(url, headers={'User-Agent':'CrawlerWorker'}, timeout=10)
                resp.raise_for_status()
                html = resp.text
                success = True
                break
            except Exception as e:
                backoff = 2**(attempt-1)
                logger.warning("Fetch error %s (attempt %d), backoff %ds", e, attempt, backoff)
                time.sleep(backoff)
        if not success:
            state.update(url, crawl_status="FAILED")
            crawl_q.delete(receipt)
            return

        # 4) Persist HTML to S3
        s3_key = f"pages/{job_id}/{uuid4().hex}.html"
        storage.upload(s3_key, html)
        state.complete_crawl(url, s3_key)

        # 5) Text extraction & RDS discovered_count
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("UPDATE jobs SET discovered_count = discovered_count+1 WHERE job_id=%s", (job_id,))
        conn.close()

        # 6) Enqueue indexing
        index_q.send({ 'jobId': job_id, 'pageUrl': url, 'content': text })

        # 7) Parse and enqueue children
        seed = state.get_seed_domain(job_id)
        children = []
        for a in soup.find_all('a', href=True):
            full = urljoin(url, a['href'].split('#')[0])
            p = urlparse(full)
            if p.scheme not in ('http','https'): continue
            if not ALLOW_EXTERNAL and p.netloc != seed: continue
            children.append(full)
        if depth < state.get_depth_limit(job_id):
            for child in children:
                crawl_q.send({'jobId': job_id, 'url': child, 'depth': depth+1})

        logger.info("Crawled %s (depth %d), %d children", url, depth, len(children))

        # 8) Ack
        crawl_q.delete(receipt)

    except Exception:
        logger.exception("Error in crawl_task for %s", url)

    finally:
        stop_evt.set()

def adjust_threads(executor):
    try:
        attrs   = sqs.get_queue_attributes(
            QueueUrl=CRAWL_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )['Attributes']
        backlog = int(attrs.get('ApproximateNumberOfMessages',0))
        target  = min(max(backlog//5+1, MIN_THREADS), MAX_THREADS)
        if executor._max_workers != target:
            logger.info("Resizing threads: %d → %d", executor._max_workers, target)
            executor._max_workers = target
    except Exception:
        logger.exception("Failed to adjust threads")

def main():
    # initial pool size
    if THREAD_COUNT:
        workers = int(THREAD_COUNT); auto_scale=False
    else:
        workers = MIN_THREADS;    auto_scale=True

    executor = ThreadPoolExecutor(max_workers=workers)
    logger.info("Starting crawler (%d threads, auto_scale=%s)", workers, auto_scale)

    while True:
        # long poll
        response = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=MSG_BATCH_SIZE,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for m in response.get('Messages',[]):
            executor.submit(crawl_task, m)

        if auto_scale:
            adjust_threads(executor)
        time.sleep(SCALE_INTERVAL)

if __name__=='__main__':
    main()
