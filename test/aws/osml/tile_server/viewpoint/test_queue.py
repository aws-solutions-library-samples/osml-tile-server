# Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import json
import unittest
from unittest import TestCase
from unittest.mock import MagicMock

import boto3
from botocore.exceptions import ClientError
from moto import mock_aws
from test_config import TestConfig


@mock_aws
class TestViewpointRequestQueue(TestCase):
    """Unit tests for the ViewpointRequestQueue class."""

    def setUp(self):
        """Set up the virtual SQS queue for testing."""
        from aws.osml.tile_server.app_config import BotoConfig

        # Create a virtual SQS queue
        self.sqs = boto3.resource("sqs", config=BotoConfig.default)
        self.queue = self.sqs.create_queue(QueueName=TestConfig.test_viewpoint_request_queue_name)

    def tearDown(self):
        """Clean up resources after each test."""
        self.sqs = None
        self.queue = None

    def test_viewpoint_request_queue_initialization(self):
        """Test the initialization of the ViewpointRequestQueue."""
        from aws.osml.tile_server.viewpoint import ViewpointRequestQueue

        mock_queue_name = TestConfig.test_viewpoint_request_queue_name
        viewpoint_request_queue = ViewpointRequestQueue(self.sqs, mock_queue_name)

        self.assertEqual(viewpoint_request_queue.sqs_client, self.sqs)
        self.assertEqual(viewpoint_request_queue.queue.url, self.queue.url)

    def test_send_request_success(self):
        """Test sending a request successfully to the SQS queue."""
        from aws.osml.tile_server.viewpoint import ViewpointRequestQueue

        mock_queue_name = TestConfig.test_viewpoint_request_queue_name
        viewpoint_request_queue = ViewpointRequestQueue(self.sqs, mock_queue_name)

        mock_request = {"message": "very important tests message"}
        viewpoint_request_queue.send_request(mock_request)

        # Verify the request was sent by checking the message count in the queue
        messages = self.queue.receive_messages(MaxNumberOfMessages=1)
        self.assertEqual(len(messages), 1)
        self.assertEqual(json.loads(messages[0].body), mock_request)

    def test_send_request_client_error(self):
        """Test handling a ClientError when sending a request."""
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

        mock_logger.error.assert_called_once_with(
            "Unable to send message visibility: An error occurred (500) when calling the send_message operation: Mock Error"
        )


if __name__ == "__main__":
    unittest.main()
