import os
import json
import logging
from whoosh.fields import Schema, ID, TEXT
from whoosh.index import create_in, open_dir
from whoosh.writing import AsyncWriter
from bs4 import BeautifulSoup

from aws_adapter import SqsQueue, S3Storage, DynamoState

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - INDEXER - %(levelname)s - %(message)s")

# Ensure index directory exists and open/create Whoosh index
INDEX_DIR = os.environ.get('INDEX_DIR', 'indexdir')
if not os.path.exists(INDEX_DIR):
    os.makedirs(INDEX_DIR)
    ix = create_in(
        INDEX_DIR,
        Schema(
            url=ID(stored=True, unique=True),
            title=TEXT(stored=True),
            content=TEXT
        )
    )
else:
    ix = open_dir(INDEX_DIR)


def indexer_worker():
    idx_q    = SqsQueue(os.environ['INDEX_QUEUE_URL'])
    storage  = S3Storage(os.environ['S3_BUCKET'])
    state    = DynamoState(os.environ['URL_TABLE'])

    logging.info(f"Indexer started; polling {idx_q.url}")
    while True:
        msgs = idx_q.receive(1, 20)
        if not msgs:
            continue

        m    = msgs[0]
        body = m['Body']
        try:
            task = json.loads(body)
        except json.JSONDecodeError:
            logging.warning(f"Skipping non-JSON message: {body}")
            idx_q.delete(m['ReceiptHandle'])
            continue

        url   = task.get('url')
        key   = task.get('s3_key')
        rh    = m['ReceiptHandle']

        # Claim for indexing
        if not state.claim_index(url):
            logging.info(f"Already indexed {url}; deleting message")
            idx_q.delete(rh)
            continue

        try:
            # Download HTML and extract text
            html = storage.download(key)
            soup = BeautifulSoup(html, 'html.parser')
            title = (soup.title.string or 'No Title').strip()
            content = soup.get_text(separator=' ')

            # Update Whoosh index
            with AsyncWriter(ix) as writer:
                writer.update_document(url=url, title=title, content=content)

            logging.info(f"Indexed {url}")
        except Exception as e:
            logging.error(f"Error indexing {url}: {e}", exc_info=True)
        finally:
            # Always delete the message to avoid retry storms
            idx_q.delete(rh)


if __name__ == '__main__':
    indexer_worker()
