#  Copyright 2023 Amazon.com, Inc. or its affiliates.

from unittest import TestCase
from unittest.mock import MagicMock

import boto3
from botocore.exceptions import ClientError
from moto import mock_sqs
from test_config import TestConfig


@mock_sqs
class TestViewpointRequestQueue(TestCase):
    def setUp(self):
        from aws.osml.tile_server.app_config import BotoConfig

        # create virtual queue
        self.sqs = boto3.resource("sqs", config=BotoConfig.default)
        self.queue = self.sqs.create_queue(QueueName=TestConfig.test_viewpoint_request_queue_name)

    def tearDown(self):
        self.sqs = None
        self.queue = None

    def test_viewpoint_request_queue(self):
        from aws.osml.tile_server.viewpoint import ViewpointRequestQueue

        mock_queue_name = TestConfig.test_viewpoint_request_queue_name
        viewpoint_request_queue = ViewpointRequestQueue(self.sqs, mock_queue_name)
        assert viewpoint_request_queue.sqs_client == self.sqs

    def test_send_request(self):
        from aws.osml.tile_server.viewpoint import ViewpointRequestQueue

        mock_queue_name = TestConfig.test_viewpoint_request_queue_name
        viewpoint_request_queue = ViewpointRequestQueue(self.sqs, mock_queue_name)

        mock_request = {"message": "very important tests message"}
        viewpoint_request_queue.send_request(mock_request)

    def test_send_request_bad_message(self):
        from aws.osml.tile_server.viewpoint import ViewpointRequestQueue

        mock_queue_name = TestConfig.test_viewpoint_request_queue_name
        viewpoint_request_queue = ViewpointRequestQueue(self.sqs, mock_queue_name)

        mock_request = {"message": "very important tests message"}
        mock_logger = MagicMock()
        viewpoint_request_queue.logger = mock_logger
        viewpoint_request_queue.queue = MagicMock()
        viewpoint_request_queue.queue.send_message.side_effect = ClientError(
            {"Error": {"Code": 500, "Message": "Mock Error"}}, "send_message"
        )

        viewpoint_request_queue.send_request(mock_request)
        assert mock_logger.error.has_been_called()
