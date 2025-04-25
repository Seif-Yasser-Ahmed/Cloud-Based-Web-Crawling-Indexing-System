#!/usr/bin/env python3
"""
indexer_worker.py — Multi-threaded Indexer Worker

Continuously polls the indexTaskQueue SQS queue, spawns threads to process
each page’s content into an inverted index in DynamoDB, and updates job progress.
Scales its thread pool based on queue backlog.
"""

import os
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor

import boto3

# ─── Configuration ────────────────────────────────────────────────────────────
INDEX_QUEUE_URL   = os.environ['INDEX_TASK_QUEUE']
JOBS_TABLE_NAME   = os.environ.get('JOBS_TABLE_NAME', 'Jobs')
INDEX_TABLE_NAME  = os.environ.get('INDEX_TABLE_NAME', 'Index')

# Thread‐scaling parameters
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
index_tbl = dynamodb.Table(INDEX_TABLE_NAME)


def index_task(msg):
    """
    Process a single SQS message:
    - Parse jobId, pageUrl, content
    - Tokenize and update inverted-index entries in DynamoDB
    - Increment the job’s indexedCount
    - Delete the message from the queue
    """
    try:
        body    = json.loads(msg['Body'])
        job_id  = body['jobId']
        page_url = body['pageUrl']
        content = body['content']

        # Build inverted-index: unique terms only
        terms = set(content.split())
        for term in terms:
            index_tbl.update_item(
                Key={
                    'term':    term,
                    'jobPage': f"{job_id}#{page_url}"
                },
                UpdateExpression="""
                    SET frequency = if_not_exists(frequency, :zero) + :inc
                """,
                ExpressionAttributeValues={
                    ':inc': 1,
                    ':zero': 0
                }
            )

        # Update the job’s indexedCount counter
        jobs_tbl.update_item(
            Key={'jobId': job_id},
            UpdateExpression='ADD indexedCount :inc',
            ExpressionAttributeValues={':inc': 1}
        )

        # Delete the processed message
        sqs.delete_message(
            QueueUrl=INDEX_QUEUE_URL,
            ReceiptHandle=msg['ReceiptHandle']
        )

        logger.info("Indexed page %s for job %s", page_url, job_id)

    except Exception:
        logger.exception("Failed processing index message: %s", msg.get('MessageId'))


def adjust_threads(executor):
    """
    Resize the ThreadPoolExecutor based on the number of visible messages.
    Target: roughly 1 thread per 5 messages, bounded by MIN_THREADS and MAX_THREADS.
    """
    try:
        attrs = sqs.get_queue_attributes(
            QueueUrl=INDEX_QUEUE_URL,
            AttributeNames=['ApproximateNumberOfMessages']
        )['Attributes']
        backlog = int(attrs.get('ApproximateNumberOfMessages', 0))
        target = min(max(backlog // 5 + 1, MIN_THREADS), MAX_THREADS)

        current = executor._max_workers
        if current != target:
            logger.info("Resizing indexer threads: %d → %d", current, target)
            executor._max_workers = target

    except Exception:
        logger.exception("Error adjusting thread pool size")


def main():
    # Initialize thread pool at minimum size
    executor = ThreadPoolExecutor(max_workers=MIN_THREADS)
    logger.info("Starting indexer worker (threads=%d–%d)", MIN_THREADS, MAX_THREADS)

    while True:
        # Receive up to MSG_BATCH_SIZE messages (long-poll)
        resp = sqs.receive_message(
            QueueUrl=INDEX_QUEUE_URL,
            MaxNumberOfMessages=MSG_BATCH_SIZE,
            WaitTimeSeconds=POLL_WAIT_TIME
        )
        msgs = resp.get('Messages', [])
        for msg in msgs:
            executor.submit(index_task, msg)

        # Adjust thread pool size based on queue backlog
        adjust_threads(executor)

        # Throttle scaling checks
        time.sleep(SCALE_INTERVAL)


if __name__ == '__main__':
    main()
