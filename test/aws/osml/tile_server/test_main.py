# import json
import os
import unittest

import boto3
from fastapi.testclient import TestClient
from moto import mock_dynamodb, mock_s3

# from aws.osml.tile_server.viewpoint.models import ViewpointStatus

TEST_REGION = "us-west-2"

TEST_BUCKET = "test-bucket"
TEST_OBJECT_KEY = "test-sample.nitf"
TEST_FILE_PATH = "./test/data/test-sample.nitf"
TEST_VIEWPOINT_NAME = "test-name"

TEST_VIEWPOINT_TABLE_NAME = "ViewpointStatusTable"
TEST_VIEWPOINT_KEY_SCHEMA = [{"AttributeName": "viewpoint_id", "KeyType": "HASH"}]
TEST_VIEWPOINT_ATTRIBUTE_DEF = [{"AttributeName": "viewpoint_id", "AttributeType": "S"}]


@mock_s3
@mock_dynamodb
class TestTileServer(unittest.TestCase):
    def setUp(self):
        from aws.osml.tile_server.app_config import BotoConfig

        # create virtual s3
        self.s3_resource = boto3.resource("s3", config=BotoConfig.default)
        self.s3_resource.create_bucket(
            Bucket=TEST_BUCKET, CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]}
        )
        self.s3_resource.meta.client.upload_file(TEST_FILE_PATH, TEST_BUCKET, TEST_OBJECT_KEY)

        # create virtual ddb
        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table = self.ddb.create_table(
            TableName="ViewpointStatusTable",
            KeySchema=TEST_VIEWPOINT_KEY_SCHEMA,
            AttributeDefinitions=TEST_VIEWPOINT_ATTRIBUTE_DEF,
            BillingMode="PAY_PER_REQUEST",
        )

        from aws.osml.tile_server.main import app

        self.client = TestClient(app)

    def tearDown(self):
        self.client = None

    def test_main(self):
        response = self.client.get("/")
        assert response.status_code == 200
        self.assertIn("Hello! Welcome to OSML Tile Server - 0.1.0!", response.json())


if __name__ == "__main__":
    unittest.main()
