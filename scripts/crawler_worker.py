#!/usr/bin/env python3
"""
crawler_worker.py — Each thread polls SQS, does its work, and heartbeats
into `heartbeats` table using its own thread‐name as node_id.
"""

import os, json, time, logging, threading
from uuid import uuid4
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import boto3, requests
from bs4 import BeautifulSoup

from aws_adapter import S3Storage
from db import get_connection

# ─── Config ───────────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL     = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL     = os.environ['INDEX_TASK_QUEUE']
THREAD_COUNT        = int(os.environ.get('CRAWLER_THREAD_COUNT', 5))
POLL_WAIT_TIME      = int(os.environ.get('POLL_WAIT_TIME_SEC', '5'))
VISIBILITY_TIMEOUT  = int(os.environ.get('VISIBILITY_TIMEOUT', '120'))
HEARTBEAT_INTERVAL  = int(os.environ.get('HEARTBEAT_POLL_INTERVAL',
                                         str(VISIBILITY_TIMEOUT//2)))
DEFAULT_DELAY       = float(os.environ.get('DELAY', '1'))
MAX_RETRIES         = int(os.environ.get('MAX_RETRIES', '3'))
ALLOW_EXTERNAL      = os.environ.get('ALLOW_EXTERNAL','false').lower()=='true'
S3_BUCKET           = os.environ.get('S3_BUCKET')

# unified heartbeat table
HEARTBEAT_TABLE = 'heartbeats'

logging.basicConfig(level=logging.INFO, format='[CRAWLER] %(levelname)s %(message)s')
logger = logging.getLogger()

sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION'))
s3  = S3Storage(S3_BUCKET) if S3_BUCKET else None

robot_parsers = {}
job_config    = {}
config_lock   = threading.Lock()

def send_heartbeat(role='crawler'):
    """Use current_thread().name as node_id."""
    node_id = threading.current_thread().name
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO {HEARTBEAT_TABLE} (node_id, role, last_heartbeat)
            VALUES (%s, %s, NOW())
            ON DUPLICATE KEY UPDATE last_heartbeat = NOW()
        """, (node_id, role))
    conn.commit()
    conn.close()

def crawl_task(msg):
    receipt = msg['ReceiptHandle']
    body    = json.loads(msg['Body'])
    job_id  = body.get('jobId'); url = body.get('url')
    depth   = int(body.get('depth',0))
    if not job_id or not url:
        sqs.delete_message(QueueUrl=CRAWL_QUEUE_URL, ReceiptHandle=receipt)
        return

    # 1) SQS-visibility heartbeat
    stop_vis = threading.Event()
    def vis_hb():
        while not stop_vis.wait(HEARTBEAT_INTERVAL):
            try:
                sqs.change_message_visibility(
                    QueueUrl=CRAWL_QUEUE_URL,
                    ReceiptHandle=receipt,
                    VisibilityTimeout=VISIBILITY_TIMEOUT
                )
            except: logger.exception("vis hb")
    threading.Thread(target=vis_hb, daemon=True).start()

    # 2) Per-thread RDS heartbeat
    stop_hb = threading.Event()
    def node_hb():
        while not stop_hb.wait(HEARTBEAT_INTERVAL):
            try: send_heartbeat('crawler')
            except: logger.exception("node hb")
    threading.Thread(target=node_hb, daemon=True).start()

    try:
        # initial heartbeat
        send_heartbeat('crawler')

        # fetch per-job config once
    # fetch per-job config once
        with config_lock:
            if job_id not in job_config:
                conn = get_connection()
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT depth_limit, seed_url FROM jobs WHERE job_id=%s",
                        (job_id,)
                    )
                    row = cur.fetchone()  # this is a dict now
                conn.close()
    
                if row:
                    dl = int(row.get('depth_limit', 1))
                    su = row.get('seed_url', '')
                else:
                    dl, su = 1, ''
    
                job_config[job_id] = (dl, urlparse(su).netloc)
    
        depth_limit, seed_netloc = job_config[job_id]


        # robots.txt
        p = urlparse(url); origin = f"{p.scheme}://{p.netloc}"
        rp = robot_parsers.get(origin)
        if rp is None:
            rp = RobotFileParser(); rp.set_url(origin+"/robots.txt")
            try: rp.read()
            except: logger.warning("robots fail %s", origin)
            robot_parsers[origin] = rp
        if rp and not rp.can_fetch("*",url):
            logger.info("blocked %s",url)
            return

        time.sleep(rp.crawl_delay("*") or DEFAULT_DELAY)

        # fetch + retry
        success, html = False, None
        for i in range(1, MAX_RETRIES+1):
            try:
                r = requests.get(url, timeout=10, headers={'User-Agent':'CW'})
                r.raise_for_status(); html, success = r.text, True; break
            except Exception as e:
                backoff = 2**(i-1); logger.warning("err %s, retry %d",e,i)
                time.sleep(backoff)
        if not success:
            logger.error("failed %s",url)
            return

        # optional S3
        if s3:
            key = f"pages/{job_id}/{uuid4().hex}.html"
            s3.upload(key, html)

        # update discovered_count
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE jobs SET discovered_count=discovered_count+1 WHERE job_id=%s",
                (job_id,)
            )
        conn.commit(); conn.close()

        # enqueue index
        text = BeautifulSoup(html,'html.parser').get_text()
        sqs.send_message(
            QueueUrl=INDEX_QUEUE_URL,
            MessageBody=json.dumps({'jobId':job_id,'pageUrl':url,'content':text})
        )

        # enqueue children
        children=[]
        for a in BeautifulSoup(html,'html.parser').find_all('a',href=True):
            link=urljoin(url,a['href'].split('#')[0])
            pp=urlparse(link)
            if pp.scheme not in ('http','https'): continue
            if not ALLOW_EXTERNAL and pp.netloc!=seed_netloc: continue
            children.append(link)
        if depth<depth_limit:
            for ch in children:
                sqs.send_message(
                    QueueUrl=CRAWL_QUEUE_URL,
                    MessageBody=json.dumps({'jobId':job_id,'url':ch,'depth':depth+1})
                )

        logger.info("crawled %s depth=%d links=%d",url,depth,len(children))

    except:
        logger.exception("crawl_task err")
    finally:
        stop_vis.set()
        stop_hb.set()
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
        # immediate loop


def main():
    logging.info("spawning %d crawler threads", THREAD_COUNT)
    for i in range(THREAD_COUNT):
        name = f"{os.uname().nodename}-crawler-{i}"
        t    = threading.Thread(target=worker_loop, name=name, daemon=True)
        t.start()
    while True: time.sleep(60)


if __name__=='__main__':
    main()
