import os, json, logging
from aws_adapter import SqsQueue, S3Storage, DynamoState
from whoosh.fields import Schema, ID, TEXT
from whoosh.index import create_in, open_dir
from whoosh.writing import AsyncWriter
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - INDEXER - %(levelname)s - %(message)s")

# Initialize Whoosh index
if not os.path.exists('indexdir'):
    os.mkdir('indexdir')
    ix = create_in('indexdir', Schema(
        url=ID(stored=True, unique=True),
        title=TEXT(stored=True),
        content=TEXT))
else:
    ix = open_dir('indexdir')

def indexer_worker():
    idx_q   = SqsQueue(os.environ['INDEX_QUEUE_URL'])
    storage = S3Storage(os.environ['S3_BUCKET'])
    state   = DynamoState(os.environ['URL_TABLE'])

    while True:
        msgs = idx_q.receive(1, 20)
        if not msgs:
            continue

        m = msgs[0]
        task = json.loads(m['Body'])
        url, key, rh = task['url'], task['s3_key'], m['ReceiptHandle']

        if not state.claim_index(url):
            idx_q.delete(rh)
            continue

        try:
            html = storage.download(key)
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text(separator=' ')
            title = (soup.title.string or 'No Title').strip()
            with AsyncWriter(ix) as writer:
                writer.update_document(url=url, title=title, content=text)
            logging.info(f"Indexed {url}")
        except Exception as e:
            logging.error(f"Error indexing {url}: {e}", exc_info=True)
        finally:
            idx_q.delete(rh)

if __name__ == "__main__":
    indexer_worker()
