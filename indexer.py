from dotenv import load_dotenv
load_dotenv()
import os
import json
import logging
import shutil
import time

from bs4 import BeautifulSoup
from whoosh.fields import Schema, ID, TEXT
from whoosh.index import create_in
from whoosh.writing import AsyncWriter

from aws_adapter import SqsQueue, S3Storage, DynamoState

# ——— Configuration & Logging ——————————————————————————————————————————————
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - INDEXER - %(levelname)s - %(message)s"
)

CRAWL_QUEUE_URL = os.environ["INDEX_QUEUE_URL"]
S3_BUCKET        = os.environ["S3_BUCKET"]
URL_TABLE        = os.environ["URL_TABLE"]
INDEX_DIR        = os.environ.get("INDEX_DIR", "indexdir")

# ——— (Re)build the Whoosh index so it has a `title` field —————————————————————————
if os.path.exists(INDEX_DIR):
    logging.info(f"Removing existing index directory `{INDEX_DIR}` to rebuild schema")
    shutil.rmtree(INDEX_DIR)
os.makedirs(INDEX_DIR, exist_ok=True)

schema = Schema(
    url=ID(stored=True, unique=True),
    title=TEXT(stored=True),
    content=TEXT
)
ix = create_in(INDEX_DIR, schema)
logging.info(f"Created new Whoosh index at `{INDEX_DIR}` with fields: {list(schema.names())}")

# ——— Worker Loop ——————————————————————————————————————————————————————
def indexer_worker():
    idx_q   = SqsQueue(CRAWL_QUEUE_URL)
    storage = S3Storage(S3_BUCKET)
    state   = DynamoState(URL_TABLE)

    logging.info(f"Indexer started; polling {idx_q.url}")

    while True:
        msgs = idx_q.receive(max_messages=1, wait=20)
        if not msgs:
            continue

        m = msgs[0]
        body = m["Body"]
        rh   = m["ReceiptHandle"]

        # 1) Parse JSON, or skip/delete bad messages
        try:
            task = json.loads(body)
        except json.JSONDecodeError:
            logging.error(f"Skipping non-JSON message body: {body!r}")
            idx_q.delete(rh)
            continue

        url    = task.get("url")
        s3_key = task.get("s3_key")
        if not url or not s3_key:
            logging.error(f"Malformed task, missing url or s3_key: {task}")
            idx_q.delete(rh)
            continue

        # 2) Claim in DynamoDB
        if not state.claim_index(url):
            idx_q.delete(rh)
            continue

        # 3) Download from S3, parse, and index
        try:
            html = storage.download(s3_key)
            soup = BeautifulSoup(html, "html.parser")
            title   = (soup.title.string or "No Title").strip() if soup.title else "No Title"
            content = soup.get_text(separator=" ").strip()

            with AsyncWriter(ix) as writer:
                writer.update_document(url=url, title=title, content=content)

            logging.info(f"Indexed {url}")
        except Exception as e:
            logging.error(f"Error indexing {url}: {e}", exc_info=True)
        finally:
            # 4) Delete the SQS message in all cases
            idx_q.delete(rh)

if __name__ == "__main__":
    indexer_worker()
