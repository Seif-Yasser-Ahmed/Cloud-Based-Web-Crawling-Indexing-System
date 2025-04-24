import time
import boto3
from botocore.config import Config as BotoConfig
from boto3.dynamodb.conditions import Attr
from config import (
    CRAWL_QUEUE_URL,
    INDEX_QUEUE_URL,
    S3_BUCKET,
    URL_TABLE,
    HEARTBEAT_TABLE
)

# Configure boto retries
boto_config = BotoConfig(retries={"max_attempts": 5, "mode": "standard"})

class SqsQueue:
    def __init__(self, identifier):
        # Initialize SQS client with explicit region
        self.client = boto3.client(
            "sqs",
            region_name="eu-north-1",
            config=boto_config
        )
        # If given an ARN, resolve to a queue URL
        if identifier.startswith("arn:aws:sqs"):
            parts = identifier.split(":")
            queue_name = parts[-1]
            account_id = parts[4]
            resp = self.client.get_queue_url(
                QueueName=queue_name,
                QueueOwnerAWSAccountId=account_id
            )
            self.url = resp["QueueUrl"]
        else:
            self.url = identifier

    def send(self, body, attrs=None):
        return self.client.send_message(
            QueueUrl=self.url,
            MessageBody=body,
            MessageAttributes=attrs or {}
        )

    def receive(self, max_messages=1, wait_time=10):
        resp = self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time,
            MessageAttributeNames=["All"]
        )
        return resp.get("Messages", [])

    def delete(self, receipt_handle):
        return self.client.delete_message(
            QueueUrl=self.url,
            ReceiptHandle=receipt_handle
        )

class S3Client:
    def __init__(self, bucket):
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            region_name="eu-north-1",
            config=boto_config
        )

    def upload(self, key, data, content_type="text/html"):
        return self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=data,
            ContentType=content_type
        )

class DynamoDBAdapter:
    def __init__(self, table_name):
        # Initialize DynamoDB table resource
        self.table = boto3.resource(
            "dynamodb",
            region_name="eu-north-1"
        ).Table(table_name)

    def set_state(self, url, state, **attrs):
        # Store or update URL processing state
        item = {"url": url, "state": state, "timestamp": int(time.time())}
        item.update(attrs)
        return self.table.put_item(Item=item)

    def get_stalled(self, timeout):
        # Retrieve URLs stuck in IN_PROGRESS past timeout
        cutoff = int(time.time()) - timeout
        resp = self.table.scan(
            FilterExpression=Attr("state").eq("IN_PROGRESS") &
                             Attr("timestamp").lt(cutoff)
        )
        return [i["url"] for i in resp.get("Items", [])]

    def log_heartbeat(self, worker_id):
        # Record worker heartbeat in a separate table
        hb_table = boto3.resource(
            "dynamodb",
            region_name="eu-north-1"
        ).Table(HEARTBEAT_TABLE)
        return hb_table.put_item(
            Item={"worker_id": worker_id, "last_seen": int(time.time())}
        )
