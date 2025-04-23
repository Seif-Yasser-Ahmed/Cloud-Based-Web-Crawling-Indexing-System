# crawler.py
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from robotexclusionrulesparser import RobotFileParserEx
from config import CRAWL_QUEUE_URL, INDEX_QUEUE_URL, S3_BUCKET, MAX_CRAWL_DELAY
from aws_adapter import SqsQueue, S3Client, DynamoDBAdapter

crawl_q = SqsQueue(CRAWL_QUEUE_URL)
index_q = SqsQueue(INDEX_QUEUE_URL)
s3     = S3Client(S3_BUCKET)
url_db = DynamoDBAdapter("UrlStateTable")        # or use config.URL_TABLE
hb_db  = DynamoDBAdapter("CrawlerHeartbeatTable")

def make_robot_checker(base_url):
    rp = RobotFileParserEx()
    try:
        rp.fetch(urljoin(base_url, "/robots.txt"))
    except:
        return lambda _: True
    return lambda path: rp.is_allowed("*", path)

def crawl(worker_id):
    robot_rules = {}
    success, failure = 0, 0

    while True:
        msgs = crawl_q.receive()
        if not msgs:
            time.sleep(MAX_CRAWL_DELAY)
            continue

        msg = msgs[0]
        url, rh = msg["Body"], msg["ReceiptHandle"]

        url_db.set_state(url, "IN_PROGRESS")
        hb_db.log_heartbeat(worker_id)

        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base not in robot_rules:
            robot_rules[base] = make_robot_checker(base)

        if not robot_rules[base](url):
            crawl_q.delete(rh)
            url_db.set_state(url, "SKIPPED")
            continue

        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()

            key = f"pages/{worker_id}/{int(time.time())}.html"
            s3.upload(key, r.content)

            crawl_q.delete(rh)   # delete right after successful fetch
            url_db.set_state(url, "DONE")

            soup = BeautifulSoup(r.text, "html.parser")
            for a in soup.find_all("a", href=True):
                child = urljoin(url, a["href"])
                index_q.send(child)

            success += 1
        except Exception as e:
            failure += 1
            url_db.set_state(url, "FAILED", error=str(e))
            # leave message for retry

        time.sleep(MAX_CRAWL_DELAY)
