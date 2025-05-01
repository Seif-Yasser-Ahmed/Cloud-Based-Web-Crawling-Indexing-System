#!/usr/bin/env python3
"""
indexer_worker.py — Each thread is its own node (hostname-indexer-#),
long-polling SQS, processing messages, and heartbeating continually.
"""

import os, time, logging, threading, json, hashlib, socket

import boto3
from db import get_connection

# ────────────────────────────────────────────────────────────────────────────────
INDEX_QUEUE_URL    = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT       = int(os.environ.get('INDEXER_THREAD_COUNT', '10'))
POLL_WAIT_TIME     = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL = VISIBILITY_TIMEOUT // 2
HEARTBEAT_TABLE    = os.environ.get('HEARTBEAT_TABLE', 'heartbeats')

logging.basicConfig(level=logging.INFO,
                    format='[INDEXER] %(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("indexer_worker")

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

def send_heartbeat(role='indexer'):
    node_id = threading.current_thread().name
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE}(node_id, role, last_heartbeat)
            VALUES (%s,%s,NOW())
            ON DUPLICATE KEY UPDATE last_heartbeat = NOW()
        """, (node_id, role))
    conn.commit(); conn.close()

def index_task(msg):
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    job_id  = body.get('jobId')
    page    = body.get('pageUrl')
    content = body.get('content','')

    if not job_id or not page:
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)
        return

    # per-message visibility heartbeat
    stop_vis = threading.Event()
    def vis_hb():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=INDEX_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except:
                logger.exception("Failed to extend vis")
    threading.Thread(target=vis_hb, daemon=True).start()

    try:
        send_heartbeat('indexer')

        url_hash = hashlib.md5(page.encode()).hexdigest()
        freqs = {}
        for w in content.split():
            t = w.lower(); freqs[t] = freqs.get(t,0) + 1

        rows = [(job_id, page, url_hash, term, freq) for term, freq in freqs.items()]
        sql  = """
            INSERT INTO index_entries
              (job_id, page_url, page_url_hash, term, frequency)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE frequency=frequency+VALUES(frequency)
        """

        conn = get_connection()
        with conn.cursor() as cur:
            if rows:
                cur.executemany(sql, rows)
            cur.execute("UPDATE jobs SET indexed_count=indexed_count+1 WHERE job_id=%s", (job_id,))
        conn.commit(); conn.close()

        logger.info("Indexed %s (%d terms)", page, len(rows))

    except Exception:
        logger.exception("Error in index_task %s", page)
    finally:
        stop_vis.set()
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)

def worker_loop():
    name = threading.current_thread().name

    # continuous heartbeat
    def hb_loop():
        while True:
            send_heartbeat('indexer')
            time.sleep(HEARTBEAT_INTERVAL)
    threading.Thread(target=hb_loop, daemon=True).start()

    while True:
        logger.debug("%s polling…", name)
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for m in resp.get('Messages', []):
            index_task(m)

def main():
    host = socket.gethostname()
    logger.info("Spawning %d indexer threads", THREAD_COUNT)
    for i in range(THREAD_COUNT):
        t = threading.Thread(
            target=worker_loop,
            name=f"{host}-indexer-{i}",
            daemon=True
        )
        t.start()
    while True:
        time.sleep(60)

if __name__=='__main__':
    main()
