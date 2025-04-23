# aws_adapter.py
import time
import boto3
from botocore.config import Config as BotoConfig
from config import CRAWL_QUEUE_URL, INDEX_QUEUE_URL, S3_BUCKET, URL_TABLE, HEARTBEAT_TABLE

boto_config = BotoConfig(retries={"max_attempts": 5, "mode": "standard"})

class SqsQueue:
    def __init__(self, url):
        self.url = url
        self.client = boto3.client("sqs", config=boto_config)

    def send(self, body, attrs=None):
        self.client.send_message(QueueUrl=self.url, MessageBody=body, MessageAttributes=attrs or {})

    def receive(self, max_messages=1, wait_time=5):
        return self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=["All"],
        ).get("Messages", [])

    def delete(self, receipt_handle):
        self.client.delete_message(QueueUrl=self.url, ReceiptHandle=receipt_handle)


class S3Client:
    def __init__(self, bucket):
        self.bucket = bucket
        self.client = boto3.client("s3", config=boto_config)

    def upload(self, key, data, content_type="text/html"):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)


class DynamoDBAdapter:
    def __init__(self, table_name):
        self.table = boto3.resource("dynamodb").Table(table_name)

    def set_state(self, url, state, **attrs):
        item = {"url": url, "state": state, "timestamp": int(time.time())}
        item.update(attrs)
        self.table.put_item(Item=item)

    def get_stalled(self, timeout):
        cutoff = int(time.time()) - timeout
        resp = self.table.scan(
            FilterExpression="state = :ip AND timestamp < :cutoff",
            ExpressionAttributeValues={":ip": "IN_PROGRESS", ":cutoff": cutoff},
        )
        return [i["url"] for i in resp.get("Items", [])]

    def log_heartbeat(self, worker_id):
        hb_table = boto3.resource("dynamodb").Table(HEARTBEAT_TABLE)
        hb_table.put_item(Item={"worker_id": worker_id, "last_seen": int(time.time())})
