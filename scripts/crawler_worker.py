#!/usr/bin/env python3
"""
crawler_worker.py — Multi-threaded Crawler Worker

Continuously polls the crawlTaskQueue SQS queue, spawns threads to fetch pages,
extract links, enqueue new crawl tasks and indexing tasks, and updates job progress.
Scales its thread pool based on queue backlog.
"""

import os
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse

import boto3
import requests
from bs4 import BeautifulSoup

# ─── Configuration ────────────────────────────────────────────────────────────
CRAWL_QUEUE_URL   = os.environ['CRAWL_QUEUE_URL']
INDEX_QUEUE_URL   = os.environ['INDEX_TASK_QUEUE']
JOBS_TABLE_NAME   = os.environ.get('JOBS_TABLE_NAME', 'Jobs')

MIN_THREADS       = int(os.environ.get('MIN_THREADS', 2))
MAX_THREADS       = int(os.environ.get('MAX_THREADS', 20))
SCALE_INTERVAL    = int(os.environ.get('SCALE_INTERVAL_SEC', 30))
MSG_BATCH_SIZE    = int(os.environ.get('MSG_BATCH_SIZE', 5))
POLL_WAIT_TIME    = int(os.environ.get('POLL_WAIT_TIME_SEC', 20))

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)

# ─── AWS Clients & Resources ─────────────────────────────────────────────────
sqs       = boto3.client('sqs')
dynamodb  = boto3.resource('dynamodb')
jobs_tbl  = dynamodb.Table(JOBS_TABLE_NAME)

def crawl_task(msg):
    """
    Process a single crawl message:
    - Fetch the URL
    - Extract text and same-domain links
    - Enqueue new crawl tasks (if depth < limit)
    - Enqueue indexing task
    - Update discoveredCount in DynamoDB
    - Delete the message from the queue
    """
    try:
        body   = json.loads(msg['Body'])
        job_id = body['jobId']
        url    = body['url']
        depth  = int(body.get('depth', 0))

        # Fetch page
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        html = resp.text

        # Parse HTML
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()

        # Update discoveredCount
        jobs_tbl.update_item(
            Key={'jobId': job_id},
            UpdateExpression='ADD discoveredCount :inc',
            ExpressionAttributeValues={':inc': 1}
        )

        # Enqueue discovered links (same-domain, within depth limit)
        job_item = jobs_tbl.get_item(Key={'jobId': job_id}).get('Item', {})
        depth_limit = job_item.get('depthLimit', 2)
        base_netloc = urlparse(job_item.get('seedUrl', '')).netloc

        if depth < depth_limit:
            for link in soup.find_all('a', href=True):
                abs_url = urljoin(url, link['href'])
                if urlparse(abs_url).netloc != base_netloc:
                    continue
                sqs.send_message(
                    QueueUrl=CRAWL_QUEUE_URL,
                    MessageBody=json.dumps({
                        'jobId': job_id,
                        'url':   abs_url,
                        'depth': depth + 1
                    })
                )

        # Enqueue for indexing
        sqs.send_message(
            QueueUrl=INDEX_QUEUE_URL,
            MessageBody=json.dumps({
                'jobId':   job_id,
                'pageUrl': url,
                'content': text
            })
        )

        # Delete the processed message
        sqs.delete_message(
            QueueUrl=CRAWL_QUEUE_URL,
            ReceiptHandle=msg['ReceiptHandle']
        )

        logger.info("Crawled %s (depth %d) for job %s", url, depth, job_id)

    except Exception:
        logger.exception("Failed processing crawl message: %s", msg.get('MessageId'))


def adjust_threads(executor):
    """
    Resize the ThreadPoolExecutor based on the number of visible messages.
    Target roughly 1 thread per 5 messages, bounded by MIN_THREADS and MAX_THREADS.
    """
    try:
        attrs   = sqs.get_queue_attributes(
            QueueUrl=CRAWL_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )['Attributes']
        backlog = int(attrs.get('ApproximateNumberOfMessages', 0))
        target  = min(max(backlog // 5 + 1, MIN_THREADS), MAX_THREADS)
        current = executor._max_workers

        if current != target:
            logger.info("Resizing crawler threads: %d → %d", current, target)
            executor._max_workers = target

    except Exception:
        logger.exception("Error adjusting crawler thread pool size")


def main():
    executor = ThreadPoolExecutor(max_workers=MIN_THREADS)
    logger.info("Starting crawler worker (threads=%d–%d)", MIN_THREADS, MAX_THREADS)

    while True:
        # Long-poll for up to MSG_BATCH_SIZE messages
        resp = sqs.receive_message(
            QueueUrl=CRAWL_QUEUE_URL,
            MaxNumberOfMessages=MSG_BATCH_SIZE,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        msgs = resp.get('Messages', [])
        for msg in msgs:
            executor.submit(crawl_task, msg)

        # Adjust threads based on queue depth
        adjust_threads(executor)

        # Sleep before next scale check
        time.sleep(SCALE_INTERVAL)


if __name__ == '__main__':
    main()
