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
INDEX_QUEUE_URL   = os.environ['INDEX_TASK_QUEUE']
MSG_BATCH_SIZE    = int(os.environ.get('MSG_BATCH_SIZE', '5'))
POLL_WAIT_TIME    = int(os.environ.get('POLL_WAIT_TIME_SEC', '20'))
SCALE_INTERVAL    = int(os.environ.get('SCALE_INTERVAL_SEC', '30'))
MIN_THREADS       = int(os.environ.get('MIN_THREADS', '2'))
MAX_THREADS       = int(os.environ.get('MAX_THREADS', '10'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = VISIBILITY_TIMEOUT // 2
THREAD_COUNT      = os.environ.get('THREAD_COUNT')

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='[INDEXER] %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# ─── AWS Client ─────────────────────────────────────────────────────────────────
sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

def index_task(msg):
    """
    Process a single indexing task:
    1) Extend message visibility via heartbeat thread
    2) Compute MD5 hash of page_url
    3) Tokenize content, count term frequencies
    4) Insert/update RDS index_entries table
    5) Increment jobs.indexed_count
    6) Acknowledge (delete) the SQS message
    """
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    job_id  = body.get('jobId')
    page_url= body.get('pageUrl')
    content = body.get('content')

    if not job_id or not page_url:
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)
        return

    # --- Heartbeat thread to keep the message alive ---
    stop_evt = threading.Event()
    def heartbeat():
        while not stop_evt.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=INDEX_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except Exception:
                logger.exception("Failed to extend visibility for indexing task")
    threading.Thread(target=heartbeat, daemon=True).start()

    try:
        # Compute a safe hash of the URL
        url_hash = hashlib.md5(page_url.encode('utf-8')).hexdigest()

        # Simple whitespace tokenization + lowercase
        words = [w.lower() for w in content.split()]
        freqs = {}
        for w in words:
            freqs[w] = freqs.get(w, 0) + 1

        # Persist to RDS
        conn = get_connection()
        with conn.cursor() as cur:
            for term, freq in freqs.items():
                cur.execute(
                    """
                    INSERT INTO index_entries
                      (job_id, page_url, page_url_hash, term, frequency)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY
                      UPDATE frequency = frequency + VALUES(frequency)
                    """,
                    (job_id, page_url, url_hash, term, freq)
                )
            # Update the indexed_count for this job
            cur.execute(
                "UPDATE jobs SET indexed_count = indexed_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        logger.info("Indexed %s (%d unique terms)", page_url, len(freqs))

    except Exception:
        logger.exception("Error processing indexing for %s", page_url)

    finally:
        # Stop heartbeat and delete message
        stop_evt.set()
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)

def adjust_threads(executor):
    """
    Auto-scale thread pool based on queue backlog.
    """
    try:
        attrs   = sqs.get_queue_attributes(
            QueueUrl=INDEX_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )['Attributes']
        backlog = int(attrs.get('ApproximateNumberOfMessages', 0))
        # target = 1 thread per 10 messages, bounded
        target  = min(max(backlog // 10 + 1, MIN_THREADS), MAX_THREADS)
        if executor._max_workers != target:
            logger.info("Resizing indexer threads: %d → %d",
                        executor._max_workers, target)
            executor._max_workers = target
    except Exception:
        logger.exception("Failed to adjust indexer thread pool")

def main():
    if THREAD_COUNT:
        size, auto_scale = int(THREAD_COUNT), False
    else:
        size, auto_scale = MIN_THREADS, True

    executor = ThreadPoolExecutor(max_workers=size)
    logger.info("Starting indexer (%d threads, auto_scale=%s)",
                size, auto_scale)

    while True:
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=MSG_BATCH_SIZE,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for m in resp.get('Messages', []):
            executor.submit(index_task, m)

        if auto_scale:
            adjust_threads(executor)

        time.sleep(SCALE_INTERVAL)

if __name__ == '__main__':
    main()
