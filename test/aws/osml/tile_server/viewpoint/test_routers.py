import json
import os
import unittest

import boto3
from fastapi.testclient import TestClient
from moto import mock_dynamodb, mock_s3, mock_sqs

from aws.osml.tile_server.viewpoint.models import ViewpointStatus

TEST_REGION = "us-west-2"

TEST_BUCKET = "test-bucket"
TEST_OBJECT_KEY = "test-sample.nitf"
TEST_FILE_PATH = "./test/data/test-sample.nitf"
TEST_VIEWPOINT_NAME = "test-name"

TEST_VIEWPOINT_TABLE_NAME = "ViewpointStatusTable"
TEST_VIEWPOINT_KEY_SCHEMA = [{"AttributeName": "viewpoint_id", "KeyType": "HASH"}]
TEST_VIEWPOINT_ATTRIBUTE_DEF = [{"AttributeName": "viewpoint_id", "AttributeType": "S"}]

TEST_INVALID_VIEWPOINT_ID = "invalid-viewpoint-id"

TEST_BODY = {
    "bucket_name": TEST_BUCKET,
    "object_key": TEST_OBJECT_KEY,
    "viewpoint_name": TEST_VIEWPOINT_NAME,
    "tile_size": 512,
    "range_adjustment": "NONE",
}

INVALID_TEST_BODY = {
    "bucket_name": None,
    "object_key": TEST_OBJECT_KEY,
    "viewpoint_name": TEST_VIEWPOINT_NAME,
    "tile_size": 512,
    "range_adjustment": "NONE",
}

VALID_UPDATE_TEST_BODY = {
    "viewpoint_id": "",
    "viewpoint_name": "New-Viewpoint-Name",
    "tile_size": 512,
    "range_adjustment": "NONE",
}


@mock_s3
@mock_dynamodb
@mock_sqs
class TestRouters(unittest.TestCase):
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
        # create virtual queue
        self.sqs = boto3.resource("sqs", config=BotoConfig.default)
        self.queue = self.sqs.create_queue(QueueName="ViewpointRequestQueue")

        from aws.osml.tile_server.main import app

        self.client = TestClient(app)

    def tearDown(self):
        self.client = None
        self.s3_resource = None
        self.ddb = None
        self.table = None
        self.queue = None

    def test_create_viewpoint_valid(self):
        response = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))

        assert response.status_code == 201

        response_data = response.json()

        with open("test/data/viewpoint_data_sample.json", "r") as output_json:
            # override viewpoint_id / local_path since its unique
            expected_json_result = json.loads(output_json.read())
            expected_json_result["viewpoint_id"] = response_data["viewpoint_id"]
            expected_json_result["local_object_path"] = response_data["local_object_path"]

            assert response_data == expected_json_result

    def test_create_viewpoint_invalid(self):
        with self.assertRaises(Exception):
            response = self.client.post("/viewpoints/", data=json.dumps(INVALID_TEST_BODY))
            assert response.status_code == 402

    def test_describe_viewpoint_valid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        # describe api
        response = self.client.get(f"/viewpoints/{viewpoint_data['viewpoint_id']}")

        assert response.status_code == 200
        assert response.json() == viewpoint_data

    def test_describe_viewpoint_invalid(self):
        self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))

        with self.assertRaises(Exception):
            response = self.client.get(f"/viewpoints/{TEST_INVALID_VIEWPOINT_ID}")
            assert response.status_code == 402

    def test_update_viewpoint_valid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        update_data_body = VALID_UPDATE_TEST_BODY
        update_data_body["viewpoint_id"] = viewpoint_data["viewpoint_id"]

        # update api
        response = self.client.put("/viewpoints/", data=json.dumps(update_data_body))

        assert response.status_code == 201
        assert response.json()["viewpoint_name"] is not viewpoint_data["viewpoint_name"]

    def test_delete_viewpoint_valid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        response = self.client.delete(f"/viewpoints/{viewpoint_data['viewpoint_id']}")

        assert response.status_code == 200
        assert response.json()["viewpoint_status"] == ViewpointStatus.DELETED
        assert response.json()["local_object_path"] is None

    def test_list_viewpoints_valid(self):
        self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))

        # get list of viewpoints api
        response = self.client.get("/viewpoints/")

        assert response.status_code == 200
        assert len(response.json()) == 3

    def test_get_metadata_valid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        # metadata api
        response = self.client.get(f"/viewpoints/{viewpoint_data['viewpoint_id']}/metadata")

        assert response.status_code == 200

        with open("test/data/viewpoint_metadata_sample.json", "r") as output_json:
            expected_json_result = json.loads(output_json.read())
            assert response.json() == expected_json_result

    def test_get_data_invalid(self):
        self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))

        with self.assertRaises(Exception):
            response = self.client.get(f"/viewpoints/{TEST_INVALID_VIEWPOINT_ID}/metadata")
            assert response.status_code == 402

    def test_get_bounds_valid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        # bounds api
        response = self.client.get(f"/viewpoints/{viewpoint_data['viewpoint_id']}/bounds")

        assert response.status_code == 200
        assert response.json()["bounds"] == ["325860N0846000E", "325859N0846000E", "325859N0850001E", "325859N0850000E"]

    def test_get_info_valid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        # info api
        response = self.client.get(f"/viewpoints/{viewpoint_data['viewpoint_id']}/info")

        assert response.status_code == 200
        assert response.json() is not None

    def test_get_statistics_valid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        # statistics api
        response = self.client.get(f"/viewpoints/{viewpoint_data['viewpoint_id']}/statistics")

        assert response.status_code == 200
        response_data = response.json()

        with open("test/data/viewpoint_statistics_sample.json", "r") as output_json:
            expected_json_result = json.loads(output_json.read())

            # update unique viewpoint_id
            expected_json_result["image_statistics"]["description"] = response_data["image_statistics"]["description"]
            expected_json_result["image_statistics"]["files"] = response_data["image_statistics"]["files"]
            assert response_data == expected_json_result

    def test_get_tile_valid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        # tiles api
        response = self.client.get(f"/viewpoints/{viewpoint_data['viewpoint_id']}/tiles/10/10/10.NITF")
        assert response.status_code == 200

    def test_get_tile_invalid(self):
        viewpoint_data = self.client.post("/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data.json()

        # tiles api
        response = self.client.get(f"/viewpoints/{viewpoint_data['viewpoint_id']}/tiles/10/10/10.bad")

        assert response.status_code == 422


if __name__ == "__main__":
    unittest.main()
