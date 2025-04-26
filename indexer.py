from scripts.aws_adapter import SqsQueue, S3Storage, DynamoState
from whoosh.writing import AsyncWriter
from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, ID, TEXT
from bs4 import BeautifulSoup
import boto3
import io
import tarfile
import shutil
import logging
import json
import os
from dotenv import load_dotenv
load_dotenv()


# ——— Configuration & Logging ——————————————————————————————————————————————
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - INDEXER - %(levelname)s - %(message)s"
)

# AWS S3 backup settings
S3_BUCKET = os.environ["S3_BUCKET"]             # indexer-bucket-group9
S3_PREFIX = os.environ.get("INDEX_S3_PREFIX", "whoosh-index")
INDEX_DIR = os.environ.get("INDEX_DIR", "indexdir")

CRAWL_QUEUE_URL = os.environ["INDEX_QUEUE_URL"]
URL_TABLE = os.environ["URL_TABLE"]

# Create S3 client using EC2 IAM role
s3 = boto3.client("s3")


def restore_index_from_s3():
    """Download and extract index archive from S3 if it exists."""
    key = f"{S3_PREFIX}/index.tar.gz"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    except s3.exceptions.NoSuchKey:
        logging.info("No existing index backup found in S3; starting fresh.")
        return
    buf = io.BytesIO(obj['Body'].read())
    with tarfile.open(fileobj=buf, mode="r:gz") as tar:
        tar.extractall(path=INDEX_DIR)
    logging.info(f"Restored index from s3://{S3_BUCKET}/{key}")


def backup_index_to_s3():
    """Archive INDEX_DIR and upload to S3 backup bucket."""
    key = f"{S3_PREFIX}/index.tar.gz"
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        tar.add(INDEX_DIR, arcname=os.path.basename(INDEX_DIR))
    buf.seek(0)
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    logging.info(f"Backed up index to s3://{S3_BUCKET}/{key}")


# ——— Prepare/Restore Whoosh Index ————————————————————————————————————————
# Ensure index directory exists
os.makedirs(INDEX_DIR, exist_ok=True)
# Restore from S3 if present
restore_index_from_s3()

# Open existing or create new Whoosh index with desired schema
schema = Schema(
    url=ID(stored=True, unique=True),
    title=TEXT(stored=True),
    content=TEXT
)
if exists_in(INDEX_DIR):
    ix = open_dir(INDEX_DIR)
    logging.info(f"Opened existing index at '{INDEX_DIR}'")
else:
    ix = create_in(INDEX_DIR, schema)
    logging.info(
        f"Created new Whoosh index at '{INDEX_DIR}' with fields: {list(schema.names())}")

# ——— Worker Loop ——————————————————————————————————————————————————————


def indexer_worker():
    idx_q = SqsQueue(CRAWL_QUEUE_URL)
    storage = S3Storage(S3_BUCKET)
    state = DynamoState(URL_TABLE)

    logging.info(f"Indexer started; polling {idx_q.url}")

    while True:
        msgs = idx_q.receive(max_messages=1, wait=20)
        if not msgs:
            continue

        m = msgs[0]
        body = m["Body"]
        rh = m["ReceiptHandle"]

        # 1) Parse JSON, or skip/delete bad messages
        try:
            task = json.loads(body)
        except json.JSONDecodeError:
            logging.error(f"Skipping non-JSON message body: {body!r}")
            idx_q.delete(rh)
            continue

        url = task.get("url")
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
            title = (soup.title.string or "No Title").strip(
            ) if soup.title else "No Title"
            content = soup.get_text(separator=" ").strip()

            with AsyncWriter(ix) as writer:
                writer.update_document(url=url, title=title, content=content)
            logging.info(f"Indexed {url}")
            # 4) After commit, back up entire index to S3
            backup_index_to_s3()

        except Exception as e:
            logging.error(f"Error indexing {url}: {e}", exc_info=True)
        finally:
            # 5) Delete the SQS message in all cases
            idx_q.delete(rh)


if __name__ == "__main__":
    indexer_worker()
