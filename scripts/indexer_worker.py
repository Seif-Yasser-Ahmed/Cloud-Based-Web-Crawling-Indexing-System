#!/usr/bin/env python3
"""
indexer_worker.py — Each thread is its own node; heartbeats using thread‐name.
"""

import os, json, time, logging, threading, hashlib
import boto3
from db import get_connection

# ─── Config ───────────────────────────────────────────────────────────────────
INDEX_QUEUE_URL    = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT       = int(os.environ.get('INDEXER_THREAD_COUNT',10))
POLL_WAIT_TIME     = int(os.environ.get('POLL_WAIT_TIME_SEC','5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT','120'))
HEARTBEAT_INTERVAL = VISIBILITY_TIMEOUT//2
HEARTBEAT_TABLE    = os.environ.get('HEARTBEAT_TABLE','heartbeats')

logging.basicConfig(level=logging.INFO, format='[INDEXER] %(levelname)s %(message)s')
logger = logging.getLogger()

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))

def send_heartbeat(role='indexer'):
    node_id = threading.current_thread().name
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE} (node_id, role, last_heartbeat)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE last_heartbeat = NOW()
        """, (node_id, role))
    conn.commit(); conn.close()

def index_task(msg):
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    job_id  = body.get('jobId')
    url     = body.get('pageUrl')
    content = body.get('content')
    if not job_id or not url or not content:
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)
        return

    stop_vis = threading.Event()
    def vis_hb():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=INDEX_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except: logger.exception("vis hb")
    threading.Thread(target=vis_hb, daemon=True).start()

    stop_hb = threading.Event()
    def node_hb():
        while not stop_hb.wait(HEARTBEAT_INTERVAL):
            try: send_heartbeat('indexer')
            except: logger.exception("node hb")
    threading.Thread(target=node_hb, daemon=True).start()

    try:
        # initial heartbeat
        send_heartbeat('indexer')

        url_hash = hashlib.md5(url.encode()).hexdigest()
        freqs    = {}
        for w in content.split():
            t = w.lower(); freqs[t]=freqs.get(t,0)+1

        rows = [(job_id,url,url_hash,term,f) for term,f in freqs.items()]
        sql  = """
            INSERT INTO index_entries
              (job_id, page_url, page_url_hash, term, frequency)
            VALUES (%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE frequency=frequency+VALUES(frequency)
        """

        conn = get_connection()
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
            cur.execute(
                "UPDATE jobs SET indexed_count=indexed_count+1 WHERE job_id=%s",
                (job_id,)
            )
        conn.commit(); conn.close()

        logger.info("indexed %s (%d)",url,len(rows))

    except:
        logger.exception("index_task err")
    finally:
        stop_vis.set()
        stop_hb.set()
        sqs.delete_message(QueueUrl=INDEX_QUEUE_URL, ReceiptHandle=receipt)

def worker_loop():
    while True:
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for m in resp.get('Messages', []):
            index_task(m)

def main():
    logging.info("spawning %d indexer threads", THREAD_COUNT)
    for i in range(THREAD_COUNT):
        name = f"{os.uname().nodename}-indexer-{i}"
        t    = threading.Thread(target=worker_loop, name=name, daemon=True)
        t.start()
    while True: time.sleep(60)

if __name__=='__main__':
    main()
