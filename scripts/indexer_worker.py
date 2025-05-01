#!/usr/bin/env python3
"""
indexer_worker.py — Each thread is a "node," long-polling SQS for one message,
indexing it, and heartbeating into `heartbeats` under thread-name.
"""

import os
import time
import logging
import threading
import json
import hashlib
import socket
from enum import Enum

import boto3
from db import get_connection

# Define thread states as an Enum


class ThreadState(Enum):
    WAITING = "waiting"
    PROCESSING = "processing"
    IDLE = "idle"


# ────────────────────────────────────────────────────────────────────────────────
# Config
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT = int(os.environ.get('INDEXER_THREAD_COUNT', '10'))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = VISIBILITY_TIMEOUT // 2
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')

# ────────────────────────────────────────────────────────────────────────────────
# Logging
logging.basicConfig(
    level=logging.INFO,
    format='[INDEXER] %(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger("indexer_worker")

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
thread_states = {}  # thread_name → state


def send_heartbeat(role='indexer', state=ThreadState.IDLE, current_url=None):
    """Insert/update a row in `heartbeats` using thread-name as node_id."""
    node_id = threading.current_thread().name
    thread_states[node_id] = state.value

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE} 
            (node_id, role, last_heartbeat, state, current_url)
            VALUES (%s, %s, NOW(), %s, %s)
            ON DUPLICATE KEY UPDATE 
            last_heartbeat = NOW(), state = %s, current_url = %s
        """, (node_id, role, state.value, current_url, state.value, current_url))
    conn.commit()
    conn.close()


def index_task(msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    url = body.get('pageUrl')
    content = body.get('content', '')

    if not job_id or not url:
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)
        return

    # Update thread state to processing with current URL
    send_heartbeat('indexer', ThreadState.PROCESSING, url)

    # 1) SQS visibility heartbeat
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

    # 2) Per-thread RDS heartbeat
    stop_hb = threading.Event()

    def thread_heartbeat():
        while not stop_hb.wait(HEARTBEAT_INTERVAL):
            try:
                # Use the same state and URL as the main thread
                current_state = thread_states.get(
                    threading.current_thread().name, ThreadState.PROCESSING)
                send_heartbeat('indexer', current_state, url)
            except Exception:
                logger.exception("Failed to send indexer heartbeat")

    thread_name = threading.current_thread().name
    hb_thread = threading.Thread(
        target=thread_heartbeat,
        name=f"{thread_name}-heartbeat",
        daemon=True
    )
    hb_thread.start()

    try:
        # compute URL hash
        url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()

        # tokenize & count frequencies
        freqs = {}
        for w in content.split():
            term = w.lower()
            freqs[term] = freqs.get(term, 0) + 1

        rows = [
            (job_id, url, url_hash, term, freq)
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
            cur.execute(
                "UPDATE jobs SET indexed_count = indexed_count + 1 WHERE job_id = %s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        logger.info("Indexed %s (%d terms)", url, len(rows))

    except Exception:
        logger.exception("Error indexing %s", url)

    finally:
        # stop both heartbeats and delete the message
        stop_vis.set()
        stop_hb.set()
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)
        # Reset state to waiting when done
        send_heartbeat('indexer', ThreadState.WAITING, None)


def worker_loop():
    name = threading.current_thread().name
    thread_states[name] = ThreadState.WAITING.value
    send_heartbeat('indexer', ThreadState.WAITING, None)

    while True:
        logger.debug("%s polling…", name)
        thread_states[name] = ThreadState.WAITING.value
        send_heartbeat('indexer', ThreadState.WAITING, None)

        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for msg in resp.get('Messages', []):
            index_task(msg)


def main():
    host = socket.gethostname()
    logger.info("Spawning %d indexer threads", THREAD_COUNT)

    # Create a heartbeat table if it doesn't exist
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {HEARTBEAT_TABLE} (
                node_id VARCHAR(255) PRIMARY KEY,
                role VARCHAR(50) NOT NULL,
                last_heartbeat DATETIME NOT NULL,
                state VARCHAR(50) DEFAULT 'waiting',
                current_url TEXT NULL
            ) ENGINE=InnoDB
        """)
    conn.commit()
    conn.close()

    for i in range(THREAD_COUNT):
        t = threading.Thread(
            target=worker_loop,
            name=f"{host}-indexer-{i}",
            daemon=True
        )
        t.start()
    # Keep main thread alive
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
