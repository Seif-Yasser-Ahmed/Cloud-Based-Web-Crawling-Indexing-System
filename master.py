from scripts.aws_adapter import SqsQueue
import logging
import os
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - MASTER - %(levelname)s - %(message)s"
)


def main():
    # Load AWS_REGION, CRAWL_QUEUE_URL, etc. from your .env
    load_dotenv()

    crawl_q = SqsQueue(os.environ["CRAWL_QUEUE_URL"])
    logging.info(
        "Interactive master started. Enter seed URLs; blank URL to exit.")

    try:
        while True:
            url = input("Seed URL (blank to quit): ").strip()
            if not url:
                break

            depth_str = input("Depth (integer): ").strip()
            try:
                depth = int(depth_str)
            except ValueError:
                print(" → Invalid depth; please enter an integer.")
                continue

            crawl_q.send({"url": url, "depth": depth})
            logging.info(f"Enqueued seed {url} with depth={depth}")

    except (KeyboardInterrupt, EOFError):
        print("\nShutting down master.")
    logging.info("Master exited.")


if __name__ == "__main__":
    main()
