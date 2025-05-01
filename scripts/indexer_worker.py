#!/usr/bin/env python3
"""
indexer_worker.py — High-throughput indexer worker with per-thread RDS monitoring.

Now skips any page_url already indexed (by job_id + page_url_hash) and logs the skip.
Each thread long-polls SQS, processes one message end-to-end, updates the
`heartbeats` table with its role, state, and current URL, then loops again.
"""

import os
import json
import time
import logging
import threading
import socket
import hashlib

import boto3
from db import get_connection

# ─── Configuration ─────────────────────────────────────────────────────────────
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT = int(os.environ.get('THREAD_COUNT',
                                  os.environ.get('MAX_THREADS', '5')))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', '30'))

# Monitoring / identification
NODE_BASE = os.environ.get('NODE_ID') or socket.gethostname()
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')
ROLE = 'indexer'

# ─── Logging & AWS clients ─────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format='[INDEXER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

# ─── Monitoring helper ─────────────────────────────────────────────────────────


def update_state(thread_id: str, state: str, current_url: str = None):
    """
    Insert or update this thread's heartbeat record in the `heartbeats` table.
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
        """, (thread_id, ROLE, state, current_url))
    conn.commit()
    conn.close()

# ─── Core indexing task ─────────────────────────────────────────────────────────


def index_task(thread_id: str, msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    page_url = body.get('pageUrl')
    content = body.get('content', '')

    if not job_id or not page_url:
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)
        return

    # mark waiting
    update_state(thread_id, 'waiting')

    # SQS visibility heartbeat
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
                logger.exception("[%s] visibility heartbeat failed", thread_id)
    threading.Thread(target=vis_heartbeat, daemon=True).start()

    try:
        # mark processing
        update_state(thread_id, 'processing', page_url)

        # compute URL hash
        url_hash = hashlib.md5(page_url.encode('utf-8')).hexdigest()

        # check if already indexed for this job
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM index_entries
                 WHERE job_id = %s AND page_url_hash = %s
                 LIMIT 1
            """, (job_id, url_hash))
            already = cur.fetchone() is not None
        conn.close()

        if already:
            logger.info("[%s] skipping already indexed %s",
                        thread_id, page_url)
            return

        # tokenize & count frequencies
        freqs = {}
        for w in content.split():
            term = w.lower()
            freqs[term] = freqs.get(term, 0) + 1

        # batch insert terms
        rows = [(job_id, page_url, url_hash, term, freq)
                for term, freq in freqs.items()]
        insert_sql = """
            INSERT INTO index_entries
              (job_id, page_url, page_url_hash, term, frequency)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
              frequency = frequency + VALUES(frequency)
        """

        conn = get_connection()
        with conn.cursor() as cur:
            if rows:
                cur.executemany(insert_sql, rows)
            # update indexed_count once
            cur.execute(
                "UPDATE jobs SET indexed_count = indexed_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        logger.info("[%s] indexed %s (%d terms)",
                    thread_id, page_url, len(rows))

    except Exception:
        logger.exception("[%s] error indexing %s", thread_id, page_url)

    finally:
        stop_vis.set()
        update_state(thread_id, 'waiting')
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)

# ─── Worker loop ───────────────────────────────────────────────────────────────


def worker_loop(thread_id: str):
    # initial heartbeat
    update_state(thread_id, 'waiting')
    while True:
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            index_task(thread_id, msg)
        # immediately loop again


# ─── Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == '__main__':
    logger.info("Starting %d indexer threads on %s", THREAD_COUNT, NODE_BASE)
    for i in range(THREAD_COUNT):
        thread_id = f"{NODE_BASE}-t{i}"
        t = threading.Thread(target=worker_loop,
                             args=(thread_id,), daemon=True)
        t.start()
    while True:
        time.sleep(60)
