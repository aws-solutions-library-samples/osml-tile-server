import json
import logging
from typing import Dict

from botocore.exceptions import ClientError


class ViewpointRequestQueue:
    """
    The class to send messages via Amazon's Simple Queue Service (SQS)

    :param aws_sqs: AWS SQS client instance
    :param queue_name: SQS Queue name to send messages to
    """

    def __init__(self, aws_sqs, queue_name: str) -> None:
        self.sqs_client = aws_sqs
        self.queue = self.sqs_client.get_queue_by_name(QueueName=queue_name)
        self.logger = logging.getLogger("uvicorn")

    def send_request(self, request: Dict) -> None:
        """
        Send the message via SQS

        :param request: Dict = unique identifier of a message
        :return: None
        """
        try:
            self.queue.send_message(MessageBody=json.dumps(request))
        except ClientError as error:
            self.logger.error("Unable to send message visibility: {}".format(error))
