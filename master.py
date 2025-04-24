# master.py
import time
from aws_adapter import SqsQueue, DynamoDBAdapter
from config import CRAWL_QUEUE_URL, IN_PROGRESS_TIMEOUT, MASTER_POLL_INTERVAL

crawl_q = SqsQueue(CRAWL_QUEUE_URL)
url_db  = DynamoDBAdapter("UrlStateTable")

def seed(seeds):
    for u in seeds:
        crawl_q.send(u)
        url_db.set_state(u, "OPEN")

def requeue_stalled():
    for url in url_db.get_stalled(IN_PROGRESS_TIMEOUT):
        crawl_q.send(url)
        url_db.set_state(url, "OPEN")

def run_master(seeds):
    seed(seeds)
    while True:
        requeue_stalled()
        time.sleep(MASTER_POLL_INTERVAL)

if __name__ == "__main__":
    seeds = ["https://example.com", "https://example.org"]
    run_master(seeds)
