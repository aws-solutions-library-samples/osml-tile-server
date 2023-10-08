import json
import unittest
from fastapi.testclient import TestClient
from moto import mock_s3, mock_dynamodb
import boto3

TEST_REGION = "us-west-2"

TEST_BUCKET = "test-bucket"
TEST_OBJECT_KEY = "test-sample.nitf"
TEST_FILE_PATH = "./test/data/sample.nitf"

TEST_VIEWPOINT_TABLE_NAME = "ViewpointStatusTable"
TEST_VIEWPOINT_KEY_SCHEMA = [{"AttributeName": "viewpoint_id", "KeyType": "HASH"}]
TEST_VIEWPOINT_ATTRIBUTE_DEF = [{"AttributeName": "viewpoint_id", "AttributeType": "S"}]


@mock_s3
@mock_dynamodb
class TestTileServer(unittest.TestCase):
    def setUp(self):
        from aws.osml.tile_server import app

        # create virtual s3
        self.s3_resource = boto3.resource("s3")
        self.s3_resource.create_bucket(Bucket=TEST_BUCKET)
        self.s3_resource.meta.client.upload_file(TEST_FILE_PATH, TEST_BUCKET, TEST_OBJECT_KEY)

        # create virtual ddb
        self.ddb = boto3.resource("dynamodb", region_name=TEST_REGION)
        self.table = self.ddb.create_table(
            TableName=TEST_VIEWPOINT_TABLE_NAME,
            KeySchema=TEST_VIEWPOINT_KEY_SCHEMA,
            AttributeDefinitions=TEST_VIEWPOINT_ATTRIBUTE_DEF,
            BillingMode="PAY_PER_REQUEST",
        )

        self.client = TestClient(app)

    def tearDown(self):
        self.client = None
        self.table.delete()
        self.ddb = None
    
    # @fixture
    # def create_viewpoint() -> ViewpointModel:
    #     pass

    def test_get_viewpoint_list(self):
        response = self.client.get("/viewpoints/")
        # assert response == "{}"

    def test_create_viewpoint_valid(self):
        body = {
            "bucket_name": TEST_BUCKET,
            "object_key": TEST_OBJECT_KEY,
            "viewpoint_name": "test2",
            "tile_size": 512,
            "range_adjustment": "NONE"
        }

        response = self.client.post(
            "/viewpoints/",
            data=json.dumps(body)
        )

        assert response.status_code == 201

        # compare json file (data/sample.json)

    def test_delete_viewpoint_valid(self):
        pass
    def test_update_viewpoint_valid(self):
        pass
    def test_describe_viewpoint_valid(self):
        pass
    def test_metadata_viewpoint_valid(self):
        pass
    def test_bounds_viewpoint_valid(self):
        pass
    def test_info_viewpoint_valid(self):
        pass
    def test_statistics_viewpoint_valid(self):
        pass
    def test_tiles_viewpoint_valid(self):
        pass
    def test_preview_viewpoint_valid(self):
        pass
    def test_crop_viewpoint_valid(self):
        pass

    def test_delete_viewpoint_invalid(self):
        pass
    def test_update_viewpoint_invalid(self):
        pass
    def test_describe_viewpoint_invalid(self):
        pass
    def test_metadata_viewpoint_invalid(self):
        pass
    def test_bounds_viewpoint_invalid(self):
        pass
    def test_info_viewpoint_invalid(self):
        pass
    def test_statistics_viewpoint_invalid(self):
        pass
    def test_tiles_viewpoint_invalid(self):
        pass
    def test_preview_viewpoint_invalid(self):
        pass
    def test_crop_viewpoint_invalid(self):
        pass


if __name__ == "__main__":
    unittest.main()