# peek_queue.py
from dotenv import load_dotenv
import os
import boto3
import json

# load your .env
load_dotenv()

region = os.getenv("AWS_REGION")
crawl_url = os.getenv("CRAWL_QUEUE_URL")
index_url = os.getenv("INDEX_QUEUE_URL")

sqs = boto3.client("sqs", region_name=region)

def peek(queue_url):
    resp = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=0,
    )
    return resp.get("Messages", [])

print("⏳ Peeking crawl queue…")
msgs = peek(crawl_url)
print(json.dumps(msgs, indent=2))

print("\n⏳ Peeking index queue…")
msgs = peek(index_url)
print(json.dumps(msgs, indent=2))
