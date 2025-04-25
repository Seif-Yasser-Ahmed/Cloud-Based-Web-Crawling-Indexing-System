#!/usr/bin/env python3
"""
crawler_worker.py — Multi-threaded Crawler Worker with SQS visibility-heartbeat
and optional manual thread-count override.

Continuously polls the crawlTaskQueue SQS queue, spawns threads to fetch pages,
extract links, enqueue new crawl & index tasks, and updates job progress.
Scales its thread pool based on queue backlog unless THREAD_COUNT is set.
Uses a background heartbeat per message to extend SQS visibility timeout.
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

# ─── Configuration ────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL   = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL   = os.environ['INDEX_TASK_QUEUE']
JOBS_TABLE_NAME   = os.environ.get('JOBS_TABLE_NAME', 'Jobs')

# Thread scaling parameters
MIN_THREADS       = int(os.environ.get('MIN_THREADS', 2))
MAX_THREADS       = int(os.environ.get('MAX_THREADS', 20))
SCALE_INTERVAL    = int(os.environ.get('SCALE_INTERVAL_SEC', 30))

# SQS receive / visibility
MSG_BATCH_SIZE    = int(os.environ.get('MSG_BATCH_SIZE', 5))
POLL_WAIT_TIME    = int(os.environ.get('POLL_WAIT_TIME_SEC', 20))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', 120))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_INTERVAL_SEC', VISIBILITY_TIMEOUT // 2))

# Manual override: if set, disables auto-scaling
THREAD_COUNT = os.environ.get('THREAD_COUNT')

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ─── AWS Clients & Resources ─────────────────────────────────────────────────
sqs       = boto3.client('sqs')
dynamodb  = boto3.resource('dynamodb')
jobs_tbl  = dynamodb.Table(JOBS_TABLE_NAME)


def crawl_task(msg):
    """
    Process a single crawl message:
    - Start a heartbeat thread to extend visibility
    - Fetch the URL, parse HTML, extract links
    - Enqueue new crawl tasks & indexing tasks
    - Update discoveredCount in DynamoDB
    - Clean up heartbeat and delete message
    """
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    job_id  = body['jobId']
    url     = body['url']
    depth   = int(body.get('depth', 0))

    # Heartbeat mechanism to extend visibility timeout
    stop_event = threading.Event()

    def heartbeat():
        while not stop_event.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
                logger.debug("Extended visibility for msg %s", receipt)
            except Exception:
                logger.exception("Heartbeat failure for msg %s", receipt)

    hb_thread = threading.Thread(target=heartbeat, daemon=True)
    hb_thread.start()

    try:
        # Fetch page
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        html = resp.text

        # Parse HTML and extract text
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Update discoveredCount
        jobs_tbl.update_item(
            Key={'jobId': job_id},
            UpdateExpression='ADD discoveredCount :inc',
            ExpressionAttributeValues={':inc': 1}
        )

        # Retrieve job item for depthLimit and seed domain
        job_item = jobs_tbl.get_item(Key={'jobId': job_id}).get('Item', {})
        depth_limit = job_item.get('depthLimit', 2)
        seed_netloc = urlparse(job_item.get('seedUrl', '')).netloc

        # Enqueue discovered same-domain links
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

        # Delete the processed message
        sqs.delete_message(
            QueueUrl=CRAWL_QUEUE_URL,
            ReceiptHandle=receipt
        )

        logger.info("Crawled %s (depth %d) for job %s", url, depth, job_id)

    except Exception:
        logger.exception("Error processing crawl msg %s", receipt)

    finally:
        # Stop and clean up heartbeat
        stop_event.set()
        hb_thread.join(timeout=0)


def adjust_threads(executor):
    """
    Auto-scale threads based on queue backlog:
    ~1 thread per 5 messages, bounded by MIN_THREADS and MAX_THREADS.
    """
    try:
        attrs   = sqs.get_queue_attributes(
            QueueUrl=CRAWL_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )['Attributes']
        backlog = int(attrs.get('ApproximateNumberOfMessages', 0))
        target  = min(max(backlog // 5 + 1, MIN_THREADS), MAX_THREADS)
        current = executor._max_workers

        if current != target:
            logger.info("Resizing crawler threads: %d → %d", current, target)
            executor._max_workers = target

    except Exception:
        logger.exception("Failed to adjust thread pool size")


def main():
    # Determine initial pool size
    if THREAD_COUNT:
        pool_size  = int(THREAD_COUNT)
        auto_scale = False
    else:
        pool_size  = MIN_THREADS
        auto_scale = True

    executor = ThreadPoolExecutor(max_workers=pool_size)
    logger.info("Starting crawler worker with %d threads (auto-scale=%s)",
                pool_size, auto_scale)

    while True:
        # Long-poll for messages
        resp = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=MSG_BATCH_SIZE,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            executor.submit(crawl_task, msg)

        # Auto-adjust thread pool if enabled
        if auto_scale:
            adjust_threads(executor)

        time.sleep(SCALE_INTERVAL)


if __name__ == '__main__':
    main()
