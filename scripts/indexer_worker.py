#!/usr/bin/env python3
"""
indexer_worker.py — High‐throughput indexer worker with RDS‐based monitoring.

Each thread long‐polls the INDEX_TASK_QUEUE, processes one message end‐to‐end,
updates the `heartbeats` table with its role, state, and current URL, then loops again.
"""

import os
import json
import time
import logging
import threading
import hashlib

import boto3
from db import get_connection

# ─── Configuration ─────────────────────────────────────────────────────────────
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
S3_BUCKET = os.environ.get('S3_BUCKET')       # if you choose to store raw HTML
THREAD_COUNT = int(os.environ.get('THREAD_COUNT',
                                  os.environ.get('MAX_THREADS', '5')))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', '30'))

NODE_ID = os.environ.get('NODE_ID') or __import__('socket').gethostname()
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')
ROLE = 'indexer'

# ─── Logging & AWS clients ─────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='[INDEXER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

# ─── Monitoring helper ─────────────────────────────────────────────────────────


def update_state(state: str, current_url: str = None):
    """
    Insert/update this node's heartbeat record with role, state, and current URL.
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE}
              (node_id, role, last_heartbeat, state, current_url)
            VALUES (%s, %s, NOW(), %s, %s)
            ON DUPLICATE KEY UPDATE
              last_heartbeat = NOW(),
              state = VALUES(state),
              current_url = VALUES(current_url)
        """, (NODE_ID, ROLE, state, current_url))
    conn.commit()
    conn.close()

# ─── Core indexing task ─────────────────────────────────────────────────────────


def index_task(msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    page_url = body.get('pageUrl')
    content = body.get('content', '')

    if not job_id or not page_url:
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)
        return

    # mark waiting
    update_state('waiting', None)

    # --- SQS visibility heartbeat ---
    stop_vis = threading.Event()

    def vis_heartbeat():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=INDEX_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except:
                logger.exception("Failed to extend SQS visibility")
    threading.Thread(target=vis_heartbeat, daemon=True).start()

    try:
        # mark processing
        update_state('processing', page_url)

        # compute safe URL hash
        url_hash = hashlib.md5(page_url.encode('utf-8')).hexdigest()

        # tokenize & count frequencies
        freqs = {}
        for w in content.split():
            w_lower = w.lower()
            freqs[w_lower] = freqs.get(w_lower, 0) + 1

        # prepare batch insert
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
            # batch insert terms
            if rows:
                cur.executemany(insert_sql, rows)
            # update indexed_count
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
        # stop heartbeat threads, mark waiting, delete msg
        stop_vis.set()
        update_state('waiting', None)
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)

# ─── Worker loop ────────────────────────────────────────────────────────────────


def worker_loop():
    while True:
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            index_task(msg)
        # immediately loop again


# ─── Entrypoint ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    logger.info("Starting %d indexer threads", THREAD_COUNT)
    for _ in range(THREAD_COUNT):
        t = threading.Thread(target=worker_loop, daemon=True)
        t.start()
    while True:
        time.sleep(60)
