#!/usr/bin/env python3
"""
indexer_worker.py — Multi-threaded indexer using RDS + SQS.
Computes MD5 page_url_hash for safe indexing.
"""

import os
import json
import time
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
import hashlib

import boto3

from db import get_connection

# ─── Configuration ────────────────────────────────────────────────────────────
INDEX_QUEUE_URL    = os.environ['INDEX_TASK_QUEUE']

MIN_THREADS        = int(os.environ.get('MIN_THREADS', 4))
MAX_THREADS        = int(os.environ.get('MAX_THREADS', 20))
SCALE_INTERVAL     = int(os.environ.get('SCALE_INTERVAL_SEC', 15))

MSG_BATCH_SIZE     = int(os.environ.get('MSG_BATCH_SIZE', 15))
POLL_WAIT_TIME     = int(os.environ.get('POLL_WAIT_TIME_SEC', 20))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', 30))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_INTERVAL_SEC', VISIBILITY_TIMEOUT // 2))

THREAD_COUNT = os.environ.get('THREAD_COUNT')

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ─── AWS Client ────────────────────────────────────────────────────────────────
sqs = boto3.client('sqs')


def index_task(msg):
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    job_id  = body['jobId']
    page_url= body['pageUrl']
    content = body.get('content', '')

    # Heartbeat to keep the message invisible
    stop_event = threading.Event()
    def heartbeat():
        while not stop_event.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=INDEX_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except Exception:
                logger.exception("Heartbeat failed")

    hb_thread = threading.Thread(target=heartbeat, daemon=True)
    hb_thread.start()

    try:
        # Compute a fixed-length hash for the URL
        url_hash = hashlib.md5(page_url.encode('utf-8')).hexdigest()

        conn = get_connection()
        with conn.cursor() as cur:
            #store 
            terms_data = []
            # Insert one row per unique, non-empty term
            for raw_term in set(content.split()):
                term = raw_term.strip().lower()
                if not term:
                    continue
                terms_data.append((term, job_id, page_url, url_hash, 1))
            if terms_data:
                cur.execute("""
                    INSERT INTO index_entries
                      (term, job_id, page_url, page_url_hash, frequency)
                    VALUES (%s, %s, %s, %s, 1)
                    ON DUPLICATE KEY UPDATE frequency = frequency + 1
                # """, terms_data)
                # (term, job_id, page_url, url_hash)
            # Update indexed_count
            cur.execute(
                "UPDATE jobs SET indexed_count = indexed_count + 1 WHERE job_id = %s",
                (job_id,)
            )

        # Delete the processed message
        sqs.delete_message(
            QueueUrl=INDEX_QUEUE_URL,
            ReceiptHandle=receipt
        )

        logger.info("Indexed %s for job %s", page_url, job_id)

    except Exception:
        logger.exception("Error processing index message")

    finally:
        stop_event.set()
        hb_thread.join(timeout=0)


def adjust_threads(executor):
    try:
        attrs   = sqs.get_queue_attributes(
            QueueUrl=INDEX_QUEUE_URL,
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
    logger.info("Starting indexer (threads=%d, auto_scale=%s)", pool_size, auto_scale)

    while True:
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=MSG_BATCH_SIZE,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            executor.submit(index_task, msg)

        if auto_scale:
            adjust_threads(executor)

        time.sleep(SCALE_INTERVAL)


if __name__ == '__main__':
    main()
