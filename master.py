import os
import logging
from aws_adapter import SqsQueue

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - MASTER - %(levelname)s - %(message)s")

def main():
    # Load the crawl queue URL and seeds/depth from environment
    crawl_q = SqsQueue(os.environ["CRAWL_QUEUE_URL"])
    seeds   = os.environ.get("SEED_URLS", "").split(",")
    depth   = int(os.environ.get("MAX_DEPTH", "1"))

    # Enqueue each non-empty seed URL as a JSON payload
    for url in filter(None, seeds):
        logging.info(f"Enqueue seed {url} (depth={depth})")
        crawl_q.send({"url": url, "depth": depth})

    logging.info("All seeds enqueued; master exiting.")

if __name__ == "__main__":
    main()
