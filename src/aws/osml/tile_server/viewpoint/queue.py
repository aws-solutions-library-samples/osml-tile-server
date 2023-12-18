import json
import logging
from typing import Dict

from botocore.exceptions import ClientError


class ViewpointRequestQueue:
    """
    A class used to represent the ViewpointRequestQueue.
    """

    def __init__(self, aws_sqs, queue_name: str) -> None:
        """
        Initializes a new instance of the ViewpointRequestQueue class which sends messages via Amazon's Simple Queue
        Service (SQS)

        :param aws_sqs: AWS SQS client instance.
        :param queue_name: SQS Queue name to send messages to.

        :return: None
        """

        self.sqs_client = aws_sqs
        self.queue = self.sqs_client.get_queue_by_name(QueueName=queue_name)
        self.logger = logging.getLogger("uvicorn")

    def send_request(self, request: Dict) -> None:
        """
        Send the message to an associated SQS queue.

        :param request: A JSON request to assign the SQS message sent.
        :return: None
        :raises ClientError: if unable to send a message.
        """
        try:
            self.queue.send_message(MessageBody=json.dumps(request))
        except ClientError as error:
            self.logger.error("Unable to send message visibility: {}".format(error))
