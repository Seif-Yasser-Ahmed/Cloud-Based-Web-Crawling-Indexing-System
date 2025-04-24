# aws_adapter.py
import boto3

class SqsQueue:
    def __init__(self, identifier):
        # Always tell boto your region:
        self.client = boto3.client("sqs", region_name="eu-north-1")
        if identifier.startswith("arn:aws:sqs"):
            # ARN format: arn:aws:sqs:<region>:<account>:<queueName>
            parts     = identifier.split(":")
            queueName = parts[-1]
            accountId = parts[4]
            resp = self.client.get_queue_url(
                QueueName=queueName,
                QueueOwnerAWSAccountId=accountId
            )
            self.url = resp["QueueUrl"]
        else:
            self.url = identifier

    def receive(self):
        return self.client.receive_message(
            QueueUrl=self.url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        ).get("Messages", [])

    def delete(self, receipt_handle):
        return self.client.delete_message(
            QueueUrl=self.url,
            ReceiptHandle=receipt_handle
        )

    def send(self, body):
        return self.client.send_message(
            QueueUrl=self.url,
            MessageBody=body
        )

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
