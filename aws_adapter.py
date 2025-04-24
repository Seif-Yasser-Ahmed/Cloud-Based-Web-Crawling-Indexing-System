import os
import json
import time
import boto3
from botocore.exceptions import ClientError

class SqsQueue:
    def __init__(self, url: str):
        self.url = url
        self.client = boto3.client('sqs', region_name=os.environ['AWS_REGION'])

    def send(self, payload: dict):
        self.client.send_message(QueueUrl=self.url, MessageBody=json.dumps(payload))

    def receive(self, max_messages=1, wait=20):
        resp = self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait
        )
        return resp.get('Messages', [])

    def delete(self, receipt_handle: str):
        self.client.delete_message(QueueUrl=self.url, ReceiptHandle=receipt_handle)


class S3Storage:
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.client = boto3.client('s3', region_name=os.environ['AWS_REGION'])

    def upload(self, key: str, content: str):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=content.encode('utf-8'))
        return key

    def download(self, key: str) -> str:
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        return obj['Body'].read().decode('utf-8')


class DynamoState:
    def __init__(self, table_name: str):
        self.table = boto3.resource('dynamodb', region_name=os.environ['AWS_REGION']) \
                         .Table(table_name)

    def get(self, url: str) -> dict:
        resp = self.table.get_item(Key={'url': url})
        return resp.get('Item', {})

    def update(self, url: str, **attrs):
        expr = 'SET ' + ', '.join(f"{k}=:{k}" for k in attrs)
        vals = {f":{k}": v for k, v in attrs.items()}
        self.table.update_item(
            Key={'url': url},
            UpdateExpression=expr,
            ExpressionAttributeValues=vals
        )

    def delete(self, url: str):
        self.table.delete_item(Key={'url': url})

    def claim_crawl(self, url: str) -> bool:
        now = int(time.time())
        try:
            self.table.update_item(
                Key={'url': url},
                UpdateExpression=(
                    "SET crawl_status = :inprog, ts = :now, "
                    "tries = if_not_exists(tries, :zero) + :one"
                ),
                ConditionExpression=(
                    "attribute_not_exists(crawl_status) OR crawl_status = :open"
                ),
                ExpressionAttributeValues={
                    ":inprog": "IN_PROGRESS",
                    ":open": "OPEN",
                    ":now": now,
                    ":zero": 0,
                    ":one": 1
                }
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return False
            raise

    def complete_crawl(self, url: str, s3_key: str):
        self.update(url, crawl_status="DONE", s3_key=s3_key, ts=int(time.time()))

    def claim_index(self, url: str) -> bool:
        now = int(time.time())
        try:
            self.table.update_item(
                Key={'url': url},
                UpdateExpression="SET #idx = :true, #ts = :now",
                ConditionExpression="attribute_not_exists(#idx) OR #idx = :false",
                ExpressionAttributeNames={
                    "#idx": "indexed",
                    "#ts": "idx_ts"
                },
                ExpressionAttributeValues={
                    ":true": True,
                    ":false": False,
                    ":now": now
                }
            )
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                return False
            raise


class HeartbeatManager:
    def __init__(self, table_name: str, timeout=10):
        self.table = boto3.resource('dynamodb', region_name=os.environ['AWS_REGION']) \
                         .Table(table_name)
        self.timeout = timeout

    def update(self, node_id: str):
        self.table.put_item(Item={'node_id': node_id, 'ts': int(time.time())})

    def get_all(self) -> dict:
        resp = self.table.scan()
        return {item['node_id']: item['ts'] for item in resp.get('Items', [])}

    def check_dead(self) -> list:
        now = int(time.time())
        return [n for n, ts in self.get_all().items() if now - ts > self.timeout]
