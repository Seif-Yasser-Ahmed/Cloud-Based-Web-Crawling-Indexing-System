# crawler_worker.py
# -----------------------
#!/usr/bin/env python3
import os
import json
import time
import logging
import threading
from uuid import uuid4
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import boto3
import requests
from bs4 import BeautifulSoup
from aws_adapter import S3Storage
from db import get_connection

CRAWL_QUEUE_URL = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL = os.environ['INDEX_TASK_QUEUE']
S3_BUCKET = os.environ.get('S3_BUCKET')
THREAD_COUNT = int(os.environ.get('THREAD_COUNT',
                                  os.environ.get('MAX_THREADS', '5')))
POLL_WAIT_TIME = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
SQS_HEARTBEAT_INTERVAL = VISIBILITY_TIMEOUT // 2  # extend every half-timeout
HEARTBEAT_POLL_INTERVAL = int(os.environ.get('HEARTBEAT_POLL_INTERVAL', '30'))
DEFAULT_DELAY = float(os.environ.get('DELAY', '1'))
MAX_RETRIES = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL = os.environ.get('ALLOW_EXTERNAL', 'false').lower() == 'true'

NODE_ID = os.environ.get('NODE_ID') or __import__('socket').gethostname()
HEARTBEAT_TABLE = os.environ.get('HEARTBEAT_TABLE', 'crawler_heartbeats')

logging.basicConfig(level=logging.INFO,
                    format='[CRAWLER] %(levelname)s %(message)s')
logger = logging.getLogger(__name__)
sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
s3 = S3Storage(S3_BUCKET) if S3_BUCKET else None

robot_parsers = {}
job_config = {}
job_config_lock = threading.Lock()


def send_node_heartbeat():
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE} (node_id, last_heartbeat)
            VALUES (%s, NOW())
            ON DUPLICATE KEY UPDATE last_heartbeat = NOW()
        """, (NODE_ID,))
    conn.commit()
    conn.close()


def crawl_task(msg):
    receipt = msg['ReceiptHandle']
    body = json.loads(msg['Body'])
    job_id = body.get('jobId')
    url = body.get('url')
    depth = int(body.get('depth', 0))
    if not job_id or not url:
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
        return

    stop_vis = threading.Event()
    stop_node_hb = threading.Event()

    def vis_heartbeat():
        while not stop_vis.wait(SQS_HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT)
            except:
                logger.exception("Failed to extend SQS visibility")
    threading.Thread(target=vis_heartbeat, daemon=True).start()

    def node_heartbeat_loop():
        while not stop_node_hb.wait(HEARTBEAT_POLL_INTERVAL):
            try:
                send_node_heartbeat()
            except:
                logger.exception("Node heartbeat error")
    threading.Thread(target=node_heartbeat_loop, daemon=True).start()

    try:
        with job_config_lock:
            if job_id not in job_config:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT depth_limit, seed_url FROM jobs WHERE job_id=%s",
                        (job_id,))
                    row = cur.fetchone()
                conn.close()

                if row:
                    depth_limit = int(row['depth_limit'])
                    seed_netloc = urlparse(row['seed_url']).netloc
                else:
                    depth_limit, seed_netloc = 1, ''
                job_config[job_id] = (depth_limit, seed_netloc)

            depth_limit, seed_netloc = job_config[job_id]

        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = RobotFileParser()
            rp.set_url(origin + "/robots.txt")
            try:
                rp.read()
            except:
                logger.warning("Could not read robots.txt for %s", origin)
            robot_parsers[origin] = rp
        if rp and not rp.can_fetch("*", url):
            logger.info("Blocked by robots.txt: %s", url)
            sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
            return
        time.sleep(rp.crawl_delay("*") or DEFAULT_DELAY)

        success = False
        for i in range(1, MAX_RETRIES+1):
            try:
                r = requests.get(url, timeout=10,
                                 headers={'User-Agent': 'CrawlerWorker'})
                r.raise_for_status()
                html, success = r.text, True
                break
            except Exception as e:
                backoff = 2**(i-1)
                logger.warning(
                    "Fetch error %s (attempt %d), retry in %ds", e, i, backoff)
                time.sleep(backoff)
        if not success:
            logger.error("Failed to fetch %s", url)
            sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
            return

        if s3:
            key = f"pages/{job_id}/{uuid4().hex}.html"
            s3.upload(key, html)

        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET discovered_count = discovered_count + 1 WHERE job_id = %s",
                (job_id,))
        conn.commit()
        conn.close()

        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        sqs.send_message(
            QueueUrl=INDEX_QUEUE_URL,
            MessageBody=json.dumps({
                'jobId':    job_id,
                'pageUrl':  url,
                'content':  text
            })
        )

        children = []
        for a in soup.find_all('a', href=True):
            link = urljoin(url, a['href'].split('#')[0])
            p = urlparse(link)
            if p.scheme not in ('http', 'https'):
                continue
            if not ALLOW_EXTERNAL and p.netloc != seed_netloc:
                continue
            children.append(link)

        if depth < depth_limit:
            for child in children:
                sqs.send_message(
                    QueueUrl=CRAWL_QUEUE_URL,
                    MessageBody=json.dumps({
                        'jobId': job_id, 'url': child, 'depth': depth + 1})
                )

        logger.info("Crawled %s (depth %d), found %d links",
                    url, depth, len(children))

    except:
        logger.exception("Error processing %s", url)

    finally:
        stop_vis.set()
        stop_node_hb.set()
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)


def worker_loop():
    while True:
        resp = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        for m in resp.get('Messages', []):
            crawl_task(m)


def main():
    logger.info("Starting %d crawler threads", THREAD_COUNT)
    for _ in range(THREAD_COUNT):
        t = threading.Thread(target=worker_loop, daemon=True)
        t.start()
    while True:
        time.sleep(60)


if __name__ == '__main__':
    main()
