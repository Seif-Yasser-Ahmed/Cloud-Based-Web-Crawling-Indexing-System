# master.py
import os
import time
import logging
from aws_adapter import SqsQueue, DynamoState, HeartbeatManager

logging.basicConfig(level=logging.INFO, format="%(asctime)s - MASTER - %(levelname)s - %(message)s")

def main():
    crawl_q = SqsQueue(os.environ["CRAWL_QUEUE_URL"])
    seeds = os.environ.get("SEED_URLS", "").split(",")
    depth = int(os.environ.get("MAX_DEPTH", "1"))
    state = DynamoState(os.environ["URL_TABLE"])
    hb_mgr = HeartbeatManager(os.environ["HEARTBEAT_TABLE"], timeout=int(os.environ.get("HEARTBEAT_TIMEOUT","60")))

    # Enqueue and mark seeds
    for url in filter(None, seeds):
        logging.info(f"Enqueue seed {url} (depth={depth})")
        crawl_q.send({"url": url, "depth": depth})
        state.update(url, crawl_status="OPEN")

    logging.info("All seeds enqueued; entering monitor loop.")

    while True:
        # (future) master heartbeat or requeue logic could go here
        time.sleep(60)

if __name__ == "__main__":
    main()
