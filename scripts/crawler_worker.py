# crawler_worker.py
#!/usr/bin/env python3
"""
crawler_worker.py — Multi-threaded crawler with SQS heartbeat & RDS state.
"""

import os
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

import boto3
import requests
from bs4 import BeautifulSoup

from db import get_connection

# ─── Configuration ────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL    = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL    = os.environ['INDEX_TASK_QUEUE']

MIN_THREADS        = int(os.environ.get('MIN_THREADS', 2))
MAX_THREADS        = int(os.environ.get('MAX_THREADS', 20))
SCALE_INTERVAL     = int(os.environ.get('SCALE_INTERVAL_SEC', 30))

MSG_BATCH_SIZE     = int(os.environ.get('MSG_BATCH_SIZE', 5))
POLL_WAIT_TIME     = int(os.environ.get('POLL_WAIT_TIME_SEC', 20))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', 120))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_INTERVAL_SEC', VISIBILITY_TIMEOUT // 2))

# If set, disables auto-scaling and fixes thread count
THREAD_COUNT = os.environ.get('THREAD_COUNT')

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ─── AWS Client ────────────────────────────────────────────────────────────────
sqs = boto3.client('sqs')


def crawl_task(msg):
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    job_id  = body['jobId']
    url     = body['url']
    depth   = int(body.get('depth', 0))

    # Heartbeat to extend visibility
    stop_event = threading.Event()
    def heartbeat():
        while not stop_event.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except Exception:
                logger.exception("Heartbeat failed")

    hb_thread = threading.Thread(target=heartbeat, daemon=True)
    hb_thread.start()

    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        html = resp.text

        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Update discovered_count in RDS
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET discovered_count = discovered_count + 1 WHERE job_id = %s",
                (job_id,)
            )
            cur.execute(
                "SELECT depth_limit, seed_url FROM jobs WHERE job_id = %s",
                (job_id,)
            )
            job_item = cur.fetchone()

        depth_limit = job_item['depth_limit']
        seed_netloc = urlparse(job_item['seed_url']).netloc

        # Enqueue deeper links
        if depth < depth_limit:
            for link in soup.find_all('a', href=True):
                abs_url = urljoin(url, link['href'])
                if urlparse(abs_url).netloc != seed_netloc:
                    continue
                sqs.send_message(
                    QueueUrl=CRAWL_QUEUE_URL,
                    MessageBody=json.dumps({
                        'jobId': job_id,
                        'url':   abs_url,
                        'depth': depth + 1
                    })
                )

        # Enqueue for indexing
        sqs.send_message(
            QueueUrl=INDEX_QUEUE_URL,
            MessageBody=json.dumps({
                'jobId':   job_id,
                'pageUrl': url,
                'content': text
            })
        )

        # Delete the message
        sqs.delete_message(
            QueueUrl=CRAWL_QUEUE_URL,
            ReceiptHandle=receipt
        )

        logger.info("Crawled %s (depth %d) for job %s", url, depth, job_id)

    except Exception:
        logger.exception("Error processing crawl message")

    finally:
        stop_event.set()
        hb_thread.join(timeout=0)


def adjust_threads(executor):
    try:
        attrs   = sqs.get_queue_attributes(
            QueueUrl=CRAWL_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )['Attributes']
        backlog = int(attrs.get('ApproximateNumberOfMessages', 0))
        target  = min(max(backlog // 5 + 1, MIN_THREADS), MAX_THREADS)
        current = executor._max_workers

        if current != target:
            logger.info("Resizing threads: %d → %d", current, target)
            executor._max_workers = target

    except Exception:
        logger.exception("Failed to adjust thread pool")


def main():
    if THREAD_COUNT:
        pool_size  = int(THREAD_COUNT)
        auto_scale = False
    else:
        pool_size  = MIN_THREADS
        auto_scale = True

    executor = ThreadPoolExecutor(max_workers=pool_size)
    logger.info("Starting crawler (threads=%d, auto_scale=%s)", pool_size, auto_scale)

    while True:
        resp = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=MSG_BATCH_SIZE,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            executor.submit(crawl_task, msg)

        if auto_scale:
            adjust_threads(executor)

        time.sleep(SCALE_INTERVAL)


if __name__ == '__main__':
    main()
