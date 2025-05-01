# indexer_worker.py

#!/usr/bin/env python3
"""
indexer_worker.py — High-throughput indexer worker with per-thread RDS monitoring,
robust text extraction, and Porter-stemming.

Each thread long-polls SQS, processes one message end-to-end, updates the
`heartbeats` table with its role, state, and current URL, then loops again.

Content processing:
 - Strips <script> and <style> tags
 - Extracts text with BeautifulSoup
 - Tokenizes on word characters via regex
 - Applies Porter stemming to each token
 - Generates both unigrams and bigrams of stems
"""
import os
import json
import time
import logging
import threading
import socket
import hashlib
import re

import boto3
from pymysql.err import IntegrityError
from nltk.stem.porter import PorterStemmer
from bs4 import BeautifulSoup
from db import get_connection

# ─── Configuration ─────────────────────────────────────────────────────────────
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT = int(os.environ.get('THREAD_COUNT',
                                  os.environ.get('MAX_THREADS', '5')))
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

stemmer = PorterStemmer()
word_re = re.compile(r'\w+')

# ─── Monitoring helper ─────────────────────────────────────────────────────────


def update_state(thread_id: str, state: str, current_url: str = None):
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
                logger.exception("[%s] visibility heartbeat failed", thread_id)
    threading.Thread(target=vis_hb, daemon=True).start()

    try:
        update_state(thread_id, 'processing', page_url)

        # compute URL hash and dedup
        url_hash = hashlib.md5(page_url.encode('utf-8')).hexdigest()
        conn = get_connection()
        already = False
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM seen_urls WHERE page_url_hash=%s LIMIT 1
            """, (url_hash,))
            already = cur.fetchone() is not None
        conn.close()
        if already:
            logger.info("[%s] skipping already indexed URL: %s",
                        thread_id, page_url)
            return

        # claim URL globally
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO seen_urls (page_url_hash) VALUES (%s)
                """, (url_hash,))
            conn.commit()
        except IntegrityError:
            logger.info("[%s] racing skip for URL: %s", thread_id, page_url)
            return
        finally:
            conn.close()

        # robust text extraction
        soup = BeautifulSoup(content, 'html.parser')
        for tag in soup(['script', 'style']):
            tag.decompose()
        text = soup.get_text(separator=' ')
        tokens = word_re.findall(text.lower())

        # stemming and frequency
        stems = [stemmer.stem(w) for w in tokens]
        freqs = {}
        # unigrams
        for s in stems:
            freqs[s] = freqs.get(s, 0) + 1
        # bigrams
        for i in range(len(stems)-1):
            bg = stems[i] + ' ' + stems[i+1]
            freqs[bg] = freqs.get(bg, 0) + 1

        # batch insert
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
            cur.execute(
                "UPDATE jobs SET indexed_count = indexed_count + 1 WHERE job_id=%s",
                (job_id,)
            )
        conn.commit()
        conn.close()

        logger.info("[%s] indexed %s (%d unique terms)",
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


if __name__ == '__main__':
    logger.info("Starting %d indexer threads on %s", THREAD_COUNT, NODE_BASE)
    for i in range(THREAD_COUNT):
        tid = f"{NODE_BASE}-t{i}"
        t = threading.Thread(target=worker_loop, args=(tid,), daemon=True)
        t.start()
    while True:
        time.sleep(60)
