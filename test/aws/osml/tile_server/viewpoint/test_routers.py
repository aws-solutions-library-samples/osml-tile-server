#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import json
import os
import shutil
import unittest
from glob import glob
from unittest.mock import patch

import boto3
import mock
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient
from moto import mock_dynamodb, mock_s3, mock_sqs
from test_config import TestConfig

from aws.osml.tile_server.viewpoint.models import ViewpointStatus

TEST_INVALID_VIEWPOINT_ID = "invalid-viewpoint-id"

TEST_BODY = {
    "bucket_name": TestConfig.test_bucket,
    "object_key": TestConfig.test_object_key,
    "viewpoint_name": TestConfig.test_viewpoint_name,
    "tile_size": 512,
    "range_adjustment": "NONE",
}

INVALID_TEST_BODY = {
    "bucket_name": None,
    "object_key": TestConfig.test_object_key,
    "viewpoint_name": TestConfig.test_viewpoint_name,
    "tile_size": 512,
    "range_adjustment": "NONE",
}

VALID_UPDATE_TEST_BODY = {
    "viewpoint_id": "",
    "viewpoint_name": "New-Viewpoint-Name",
    "tile_size": 512,
    "range_adjustment": "NONE",
}


class TestRouters(unittest.TestCase):
    @patch("aws.osml.tile_server.utils.initialize_token_key")
    @patch("aws.osml.tile_server.utils.read_token_key", return_value=Fernet.generate_key())
    def setUp(self, mock_read_token, mock_init_token):
        from aws.osml.tile_server.main import app

        self.client = TestClient(app)

    def tearDown(self):
        self.client = None

    @pytest.mark.skip(reason="Test not implemented")
    def test_list_viewpoints(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_create_viewpoint(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_delete_viewpoint(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_update_viewpoint(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_describe_viewpoint(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_get_metadata(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_get_bounds(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_get_info(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_get_statistics(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_get_preview(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_get_tile(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_get_crop(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_validate_viewpoint_status(self):
        pass


@mock_s3
@mock_dynamodb
@mock_sqs
class TestRoutersE2E(unittest.TestCase):
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
        self.s3_resource = None
        self.ddb = None
        self.table = None
        self.sqs = None
        self.queue = None
        tmp_test_files = glob(os.path.join("test_tmp", "viewpoints", "*"))
        for tmp_test_file in tmp_test_files:
            os.remove(tmp_test_file)

    def mock_set_ready(self, viewpoint_id):
        return self.table.update_item(
            Key={
                "viewpoint_id": viewpoint_id,
            },
            UpdateExpression="SET viewpoint_status = :viewpoint_status",
            ExpressionAttributeValues={
                ":viewpoint_status": "READY",
            },
            ReturnValues="ALL_NEW",
        )

    def mock_set_local_path(self, viewpoint_id, path):
        return self.table.update_item(
            Key={
                "viewpoint_id": viewpoint_id,
            },
            UpdateExpression="SET local_object_path = :local_object_path",
            ExpressionAttributeValues={
                ":local_object_path": path,
            },
            ReturnValues="ALL_NEW",
        )

    def mock_download(self, viewpoint_id):
        local_dir = os.path.join("test_tmp", "viewpoints")
        local_path = os.path.join(local_dir, viewpoint_id)
        if not os.path.exists(local_dir):
            os.makedirs(local_dir)
        shutil.copy(TestConfig.test_file_path, local_path)
        self.mock_set_local_path(viewpoint_id, local_path)

    def mock_create_viewpoint(self) -> str:
        viewpoint_data_res = self.client.post("/latest/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data_res.json()
        self.mock_set_ready(viewpoint_data["viewpoint_id"])
        self.mock_download(viewpoint_data["viewpoint_id"])
        return viewpoint_data["viewpoint_id"]

    def test_e2e_list_viewpoints_valid(self):
        self.client.post("/latest/viewpoints/", data=json.dumps(TEST_BODY))
        self.client.post("/latest/viewpoints/", data=json.dumps(TEST_BODY))
        self.client.post("/latest/viewpoints/", data=json.dumps(TEST_BODY))

        # get list of viewpoints api
        response = self.client.get("/latest/viewpoints/")

        assert response.status_code == 200
        assert len(response.json()["items"]) == 3

    @mock.patch("aws.osml.tile_server.viewpoint.worker", autospec=True)
    def test_e2e_create_viewpoint_valid(self, mock_worker):
        response = self.client.post("/latest/viewpoints/", data=json.dumps(TEST_BODY))

        assert response.status_code == 201
        response_data = response.json()

        with open("test/data/viewpoint_data_sample.json", "r") as output_json:
            # override viewpoint_id / local_path since its unique
            expected_json_result = json.loads(output_json.read())
            expected_json_result["viewpoint_id"] = response_data["viewpoint_id"]
            expected_json_result["local_object_path"] = response_data["local_object_path"]

            assert response_data == expected_json_result

    def test_e2e_create_viewpoint_invalid(self):
        with self.assertRaises(Exception):
            response = self.client.post("/latest/viewpoints/", data=json.dumps(INVALID_TEST_BODY))
            assert response.status_code == 402

    def test_e2e_delete_viewpoint_valid(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.delete(f"/latest/viewpoints/{viewpoint_id}")

        assert response.status_code == 200
        assert response.json()["viewpoint_status"] == ViewpointStatus.DELETED
        assert response.json()["local_object_path"] is None

    def test_e2e_update_viewpoint_valid(self):
        viewpoint_data_res = self.client.post("/latest/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data_res.json()
        self.mock_set_ready(viewpoint_data["viewpoint_id"])
        update_data_body = VALID_UPDATE_TEST_BODY
        update_data_body["viewpoint_id"] = viewpoint_data["viewpoint_id"]

        response = self.client.put("/latest/viewpoints/", data=json.dumps(update_data_body))

        assert response.status_code == 201
        assert response.json()["viewpoint_name"] is not viewpoint_data["viewpoint_name"]

    def test_e2e_describe_viewpoint_valid(self):
        viewpoint_data_res = self.client.post("/latest/viewpoints/", data=json.dumps(TEST_BODY))
        viewpoint_data = viewpoint_data_res.json()

        response = self.client.get(f"/latest/viewpoints/{viewpoint_data['viewpoint_id']}")

        assert response.status_code == 200
        assert response.json() == viewpoint_data

    def test_e2e_describe_viewpoint_invalid(self):
        self.client.post("/latest/viewpoints/", data=json.dumps(TEST_BODY))

        with self.assertRaises(Exception):
            response = self.client.get(f"/latest/viewpoints/{TEST_INVALID_VIEWPOINT_ID}")
            assert response.status_code == 402

    def test_e2e_get_metadata_valid(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/metadata")

        assert response.status_code == 200

        with open("test/data/viewpoint_metadata_sample.json", "r") as output_json:
            expected_json_result = json.loads(output_json.read())
            assert response.json() == expected_json_result

    def test_e2e_get_metadata_invalid(self):
        self.mock_create_viewpoint()
        with self.assertRaises(Exception):
            response = self.client.get(f"/latest/viewpoints/{TEST_INVALID_VIEWPOINT_ID}/metadata")
            assert response.status_code == 402

    def test_e2e_get_bounds_valid(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/bounds")

        assert response.status_code == 200
        assert response.json()["bounds"] == ["325860N0846000E", "325859N0846000E", "325859N0850001E", "325859N0850000E"]

    def test_e2e_get_info_valid(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/info")

        assert response.status_code == 200
        assert response.json() is not None

    def test_e2e_get_statistics_valid(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/statistics")

        assert response.status_code == 200
        response_data = response.json()

        with open("test/data/viewpoint_statistics_sample.json", "r") as output_json:
            expected_json_result = json.loads(output_json.read())

            # update unique viewpoint_id
            expected_json_result["image_statistics"]["description"] = response_data["image_statistics"]["description"]
            expected_json_result["image_statistics"]["files"] = response_data["image_statistics"]["files"]
            assert response_data == expected_json_result

    def test_e2e_get_preview(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/preview.JPEG")
        assert response.status_code == 200
        assert response.headers.get("content-type") == "image/jpeg"
        assert len(response.content) == 250282

    def test_e2e_get_tile_valid(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/tiles/10/10/10.NITF")
        assert response.status_code == 200

    def test_e2e_get_tile_invalid(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/tiles/10/10/10.bad")

        assert response.status_code == 422

    def test_e2e_get_crop_min_max(self):
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/crop/32,32,64,64.PNG")
        assert response.status_code == 200
        assert response.headers.get("content-type") == "image/png"
        assert len(response.content) == 748

    def test_e2e_get_crop_height_width(self):
        viewpoint_id = self.mock_create_viewpoint()
        query_params = {"width": 32, "height": 32}
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/crop/32,32,128,128.PNG", params=query_params)
        assert response.status_code == 200
        assert response.headers.get("content-type") == "image/png"
        assert len(response.content) == 748


if __name__ == "__main__":
    unittest.main()
