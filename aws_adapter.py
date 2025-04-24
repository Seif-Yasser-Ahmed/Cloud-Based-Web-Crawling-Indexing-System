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
