# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# SQS
CRAWL_QUEUE_URL    = os.getenv("CRAWL_QUEUE_URL", "arn:aws:sqs:eu-north-1:935364008195:crawlTaskQueue")
INDEX_QUEUE_URL    = os.getenv("INDEX_QUEUE_URL", "arn:aws:sqs:eu-north-1:935364008195:indexTaskQueue")

# S3
S3_BUCKET          = os.getenv("S3_BUCKET", "crawler-bucket-group9")

# DynamoDB tables (make sure these match your AWS setup)
URL_TABLE          = os.getenv("URL_STATE", "UrlStateTable")
HEARTBEAT_TABLE    = os.getenv("HEARTBEAT", "CrawlerHeartbeatTable")

# Timing & thresholds
MAX_CRAWL_DELAY      = float(os.getenv("MAX_CRAWL_DELAY", "5"))        # secs between fetches
IN_PROGRESS_TIMEOUT  = int(os.getenv("IN_PROGRESS_TIMEOUT", "300"))    # secs before requeue
MASTER_POLL_INTERVAL = float(os.getenv("MASTER_POLL_INTERVAL", "10"))  # secs between master loops
