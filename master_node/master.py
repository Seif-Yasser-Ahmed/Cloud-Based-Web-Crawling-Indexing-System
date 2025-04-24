import time
import logging

from config import (
    CRAWL_QUEUE_URL,
    URL_TABLE,
    HEARTBEAT_TABLE,
    IN_PROGRESS_TIMEOUT,
    MASTER_POLL_INTERVAL
)
from aws_adapter import SqsQueue, DynamoDBAdapter

# Configure logger
title = "master"
logger = logging.getLogger(title)
logging.basicConfig(level=logging.INFO, format=f"%(asctime)s - {title.upper()} - %(levelname)s - %(message)s")

# Initialize AWS adapters
crawl_q = SqsQueue(CRAWL_QUEUE_URL)
url_db  = DynamoDBAdapter(URL_TABLE)
hb_db   = DynamoDBAdapter(HEARTBEAT_TABLE)

# Seed initial URLs into the crawl queue and mark them OPEN in DynamoDB
def seed(seeds):
    for url in seeds:
        crawl_q.send(url)
        url_db.set_state(url, "OPEN")
    logger.info(f"Seeded {len(seeds)} URLs into crawl queue")

# Re-queue any URLs stuck in IN_PROGRESS past the configured timeout
def requeue_stalled():
    stalled_urls = url_db.get_stalled(IN_PROGRESS_TIMEOUT)
    for url in stalled_urls:
        crawl_q.send(url)
        url_db.set_state(url, "OPEN")
        logger.info(f"Re-queued stalled URL: {url}")

# Main loop: seed once, then periodically re-queue and log heartbeat
def run_master(seeds):
    seed(seeds)
    while True:
        requeue_stalled()
        hb_db.log_heartbeat("master")
        logger.debug("Heartbeat logged")
        time.sleep(MASTER_POLL_INTERVAL)

if __name__ == "__main__":
    # Replace these with your actual seed URLs
    seeds = [
        "http://quotes.toscrape.com"
    ]
    run_master(seeds)
