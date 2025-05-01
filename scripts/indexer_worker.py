#!/usr/bin/env python3
"""
indexer_worker.py — High-throughput indexer worker with per-thread RDS monitoring
and bigram support for multi-word search.

Each thread long-polls SQS, processes one message end-to-end, updates the
`heartbeats` table with its role, state, and current URL, then loops again.

To avoid duplicates:
- We atomically claim each URL in the `seen_urls` table (page_url_hash PRIMARY KEY).
- We index unigrams **and** adjacent bigrams (two-word phrases).
"""

import os
import json
import time
import logging
import threading
import socket
import hashlib

import boto3
from pymysql.err import IntegrityError
from db import get_connection

# ─── Configuration ─────────────────────────────────────────────────────────────
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT = int(os.environ.get(
    'THREAD_COUNT', os.environ.get('MAX_THREADS', '5')))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', '30'))

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
    """Insert or update this thread's heartbeat record in the `heartbeats` table."""
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
                logger.exception("[%s] heartbeat failed", thread_id)
    threading.Thread(target=vis_heartbeat, daemon=True).start()

    try:
        update_state(thread_id, 'processing', page_url)

        # compute URL hash
        url_hash = hashlib.md5(page_url.encode('utf-8')).hexdigest()

        # claim URL globally (seen_urls table must exist with PRIMARY KEY(page_url_hash))
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO seen_urls (page_url_hash)
                    VALUES (%s)
                """, (url_hash,))
            conn.commit()
        except IntegrityError:
            logger.info("[%s] skipping already seen URL: %s",
                        thread_id, page_url)
            return
        finally:
            conn.close()

        # tokenize into words
        tokens = [w.lower() for w in content.split() if w.strip()]
        freqs = {}

        # unigrams
        for w in tokens:
            freqs[w] = freqs.get(w, 0) + 1

        # bigrams (two-word phrases)
        for i in range(len(tokens) - 1):
            bg = tokens[i] + ' ' + tokens[i+1]
            freqs[bg] = freqs.get(bg, 0) + 1

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
            if rows:
                cur.executemany(insert_sql, rows)
            # increment job's indexed count once
            cur.execute(
                "UPDATE jobs SET indexed_count = indexed_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        logger.info("[%s] indexed %s → %d unique tokens (incl. bigrams)",
                    thread_id, page_url, len(rows))

    except Exception:
        logger.exception("[%s] error indexing %s", thread_id, page_url)

    finally:
        stop_vis.set()
        update_state(thread_id, 'waiting')
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)

# ─── Worker loop ───────────────────────────────────────────────────────────────


def worker_loop(thread_id: str):
    update_state(thread_id, 'waiting')
    while True:
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            index_task(thread_id, msg)


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
