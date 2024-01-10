#  Copyright 2023-2024 Amazon.com, Inc or its affiliates.

import os
import unittest
from unittest.mock import patch

import boto3
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient
from moto import mock_dynamodb, mock_s3, mock_sqs
from test_config import TestConfig


@mock_s3
@mock_dynamodb
@mock_sqs
class TestTileServer(unittest.TestCase):
    @patch("aws.osml.tile_server.utils.initialize_token_key")
    @patch("aws.osml.tile_server.utils.read_token_key", return_value=Fernet.generate_key())
    def setUp(self, mock_read_token, mock_init_token):
        from aws.osml.tile_server.app_config import BotoConfig

        # create virtual s3
        self.s3_resource = boto3.resource("s3", config=BotoConfig.default)
        self.s3_resource.create_bucket(
            Bucket=TestConfig.test_bucket, CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]}
        )
        self.s3_resource.meta.client.upload_file(
            TestConfig.test_file_path, TestConfig.test_bucket, TestConfig.test_object_key
        )

        # create virtual ddb
        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table = self.ddb.create_table(
            TableName=TestConfig.test_viewpoint_table_name,
            KeySchema=TestConfig.test_viewpoint_key_schema,
            AttributeDefinitions=TestConfig.test_viewpoint_attribute_def,
            BillingMode="PAY_PER_REQUEST",
        )

        # create virtual queue
        self.sqs = boto3.resource("sqs", config=BotoConfig.default)
        self.queue = self.sqs.create_queue(QueueName=TestConfig.test_viewpoint_request_queue_name)

        from aws.osml.tile_server.main import app

        self.client = TestClient(app)

    def tearDown(self):
        self.client = None

    def test_main(self):
        response = self.client.get("/")
        assert response.status_code == 200
        self.assertIn("OSML Tile Server", response.text)

    def test_ping(self):
        response = self.client.get("/ping")
        assert response.status_code == 200
        assert response.json() == {"status": "OK"}


if __name__ == "__main__":
    unittest.main()
