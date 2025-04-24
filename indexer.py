# indexer.py
import time
from bs4 import BeautifulSoup
from aws_adapter import SqsQueue, S3Client, DynamoDBAdapter
from config import CRAWL_QUEUE_URL, INDEX_QUEUE_URL, S3_BUCKET, MAX_CRAWL_DELAY , URL_TABLE, HEARTBEAT_TABLE 
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID
import os

# Setup
indexdir = "indexdir"
schema   = Schema(url=ID(stored=True), content=TEXT)
if not os.path.exists(indexdir):
    os.makedirs(indexdir)
    ix = create_in(indexdir, schema)
else:
    ix = open_dir(indexdir)

index_q = SqsQueue(INDEX_QUEUE_URL)
s3      = S3Client(S3_BUCKET)
url_db  = DynamoDBAdapter(URL_TABLE)

def run_indexer():
    while True:
        msgs = index_q.receive()
        if not msgs:
            time.sleep(1)
            continue

        m  = msgs[0]
        url, rh = m["Body"], m["ReceiptHandle"]
        url_db.set_state(url, "INDEX_IN_PROGRESS")
        try:
            obj = s3.client.get_object(Bucket=S3_BUCKET, Key=f"pages/*/{url.split('//')[-1]}.html")
            html = obj["Body"].read().decode("utf-8", errors="ignore")
            text = BeautifulSoup(html, "html.parser").get_text()

            w = ix.writer()
            w.add_document(url=url, content=text)
            w.commit()

            url_db.set_state(url, "INDEXED")
            index_q.delete(rh)
        except Exception as e:
            url_db.set_state(url, "INDEX_FAILED", error=str(e))
        time.sleep(0.1)

if __name__ == "__main__":
    run_indexer()
