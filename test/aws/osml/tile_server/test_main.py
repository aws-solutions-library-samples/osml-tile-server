#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
import unittest
from unittest.mock import patch

import boto3
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient
from moto import mock_aws
from test_config import TestConfig


@mock_aws
class TestTileServer(unittest.TestCase):
    """Integration tests for the OSML Tile Server using mocked AWS services."""

    @patch("aws.osml.tile_server.services.initialize_token_key")
    @patch("aws.osml.tile_server.services.read_token_key", return_value=Fernet.generate_key())
    def setUp(self, mock_read_token, mock_init_token):
        """Set up the mock AWS services and the test client."""
        from aws.osml.tile_server.app_config import BotoConfig

        # Create virtual S3
        self.s3_resource = boto3.resource("s3", config=BotoConfig.default)
        self.s3_resource.create_bucket(
            Bucket=TestConfig.test_bucket, CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]}
        )
        self.s3_resource.meta.client.upload_file(
            TestConfig.test_file_path, TestConfig.test_bucket, TestConfig.test_object_key
        )

        # Create virtual DynamoDB
        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table = self.ddb.create_table(
            TableName=TestConfig.test_viewpoint_table_name,
            KeySchema=TestConfig.test_viewpoint_key_schema,
            AttributeDefinitions=TestConfig.test_viewpoint_attribute_def,
            BillingMode="PAY_PER_REQUEST",
        )

        # Create virtual SQS
        self.sqs = boto3.resource("sqs", config=BotoConfig.default)
        self.queue = self.sqs.create_queue(QueueName=TestConfig.test_viewpoint_request_queue_name)

        # Set up the FastAPI test client
        from aws.osml.tile_server.main import app

        self.client = TestClient(app)

    def tearDown(self):
        """Clean up after each test."""
        self.client = None

    def test_main(self):
        """Test the main page of the tile server."""
        response = self.client.get("/")
        self.assertEqual(response.status_code, 200)
        self.assertIn("OSML Tile Server", response.text)

    def test_ping(self):
        """Test the ping endpoint to ensure the server is running."""
        response = self.client.get("/ping")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "OK"})


if __name__ == "__main__":
    unittest.main()
