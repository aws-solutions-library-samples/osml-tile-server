#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import json
from logging import Logger, getLogger
from typing import Dict

from botocore.exceptions import ClientError


class ViewpointRequestQueue:
    """
    A class used to represent the ViewpointRequestQueue.
    """

    def __init__(self, aws_sqs, queue_name: str, logger: Logger = getLogger(__name__)) -> None:
        """
        Initializes a new instance of the ViewpointRequestQueue class which sends messages via Amazon's Simple Queue
        Service (SQS).

        :param aws_sqs: AWS SQS client instance.
        :param logger: An optional logger to use.  If none provided it creates a new one.
        :param queue_name: SQS Queue name to send messages to.
        :return: None
        """

        self.sqs_client = aws_sqs
        self.queue = self.sqs_client.get_queue_by_name(QueueName=queue_name)
        self.logger = logger

    def send_request(self, request: Dict, attributes: Dict = None) -> None:
        """
        Send the message to an associated SQS queue.

        :param request: A JSON request to assign the SQS message sent.
        :param attributes: An optional set of attributes to attach to the message.
        :return: None
        """
        try:
            if attributes:
                self.queue.send_message(MessageBody=json.dumps(request), MessageAttributes=attributes)
            else:
                self.queue.send_message(MessageBody=json.dumps(request))
        except ClientError as error:
            self.logger.error("Unable to send message visibility: {}".format(error))
