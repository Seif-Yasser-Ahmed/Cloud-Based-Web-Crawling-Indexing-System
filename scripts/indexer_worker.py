#!/usr/bin/env python3
"""
indexer_worker.py — High‐throughput multi-threaded indexer.
Each thread polls SQS, processes one message (with SQS-visibility heartbeats),
does a batched INSERT … ON DUPLICATE KEY UPDATE via executemany, updates job count,
then immediately loops again—no central sleep.
"""

import os
import json
import time
import logging
import threading
import hashlib

import boto3
from db import get_connection

# ──────────────────────────────────────────────────────────────────────────────
# Configuration
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT = int(os.environ.get('THREAD_COUNT',
                                  os.environ.get('MAX_THREADS', '10')))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = VISIBILITY_TIMEOUT // 2

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='[INDEXER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

# ─── Core Task ────────────────────────────────────────────────────────────────


def index_task(msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    page_url = body.get('pageUrl')
    content = body.get('content')

    if not job_id or not page_url or not content:
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)
        return

    # --- 1) SQS visibility heartbeat ---
    stop_vis = threading.Event()

    def vis_heartbeat():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=INDEX_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except Exception:
                logger.exception("Failed to extend SQS visibility")
    threading.Thread(target=vis_heartbeat, daemon=True).start()

    try:
        # --- 2) Compute URL hash ---
        url_hash = hashlib.md5(page_url.encode('utf-8')).hexdigest()

        # --- 3) Tokenize & count frequencies ---
        freqs = {}
        for word in content.split():
            w = word.lower()
            freqs[w] = freqs.get(w, 0) + 1

        # --- 4) Batch‐insert into index_entries ###
        rows = [
            (job_id, page_url, url_hash, term, freq)
            for term, freq in freqs.items()
        ]
        insert_sql = """
            INSERT INTO index_entries
              (job_id, page_url, page_url_hash, term, frequency)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              frequency = frequency + VALUES(frequency)
        """

        conn = get_connection()
        with conn.cursor() as cur:
            cur.executemany(insert_sql, rows)

            # --- 5) Update indexed_count once per page ---
            cur.execute(
                "UPDATE jobs SET indexed_count = indexed_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        logger.info("Indexed %s (%d terms)", page_url, len(rows))

    except Exception:
        logger.exception("Error indexing %s", page_url)

    finally:
        # stop heartbeats & ack message
        stop_vis.set()
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)


# ─── Worker Loop ──────────────────────────────────────────────────────────────
def worker_loop():
    while True:
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            index_task(msg)
        # loop immediately—no sleep()


# ─── Entrypoint ───────────────────────────────────────────────────────────────
def main():
    logger.info("Starting %d indexer threads", THREAD_COUNT)
    for _ in range(THREAD_COUNT):
        t = threading.Thread(target=worker_loop, daemon=True)
        t.start()
    # keep the main thread alive
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
