#  Copyright 2023-2024 Amazon.com, Inc or its affiliates.

import json
import os
import shutil
import unittest
from unittest.mock import patch

import boto3
import pytest
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient
from moto import mock_aws
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
    """Unit tests for API endpoints in tile_server."""

    @patch("aws.osml.tile_server.utils.initialize_token_key")
    @patch("aws.osml.tile_server.utils.read_token_key", return_value=Fernet.generate_key())
    def setUp(self, mock_read_token, mock_init_token):
        """Set up the test client."""
        from aws.osml.tile_server.main import app

        self.client = TestClient(app)

    def tearDown(self):
        """Clean up the test client."""
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


@mock_aws
class TestRoutersE2E(unittest.TestCase):
    """End-to-end tests for the tile_server API."""

    @patch("aws.osml.tile_server.utils.initialize_token_key")
    @patch("aws.osml.tile_server.utils.read_token_key", return_value=Fernet.generate_key())
    def setUp(self, mock_read_token, mock_init_token):
        """Set up virtual AWS resources and the test client."""
        from aws.osml.tile_server.app_config import BotoConfig

        # Set up virtual S3
        self.s3_resource = boto3.resource("s3", config=BotoConfig.default)
        self.s3_resource.create_bucket(
            Bucket=TestConfig.test_bucket, CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_DEFAULT_REGION"]}
        )
        self.s3_resource.meta.client.upload_file(
            TestConfig.test_file_path, TestConfig.test_bucket, TestConfig.test_object_key
        )

        # Set up virtual DynamoDB
        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table = self.ddb.create_table(
            TableName=TestConfig.test_viewpoint_table_name,
            KeySchema=TestConfig.test_viewpoint_key_schema,
            AttributeDefinitions=TestConfig.test_viewpoint_attribute_def,
            BillingMode="PAY_PER_REQUEST",
        )

        # Set up virtual SQS
        self.sqs = boto3.resource("sqs", config=BotoConfig.default)
        self.queue = self.sqs.create_queue(QueueName=TestConfig.test_viewpoint_request_queue_name)

        # Set up the test client
        from aws.osml.tile_server.main import app

        self.client = TestClient(app)

    def tearDown(self):
        """Clean up virtual AWS resources and test client."""
        self.client = None
        self.s3_resource = None
        self.ddb = None
        self.table = None
        self.sqs = None
        self.queue = None
        tmp_viewpoints_path = os.path.join("test_tmp", "viewpoints")
        if os.path.isdir(tmp_viewpoints_path):
            shutil.rmtree(tmp_viewpoints_path)

    def mock_set_ready(self, viewpoint_id):
        """Mock the update of a viewpoint's status to READY."""
        return self.table.update_item(
            Key={"viewpoint_id": viewpoint_id},
            UpdateExpression="SET viewpoint_status = :viewpoint_status",
            ExpressionAttributeValues={":viewpoint_status": "READY"},
            ReturnValues="ALL_NEW",
        )

    def mock_set_local_path(self, viewpoint_id, path):
        """Mock the update of a viewpoint's local object path."""
        return self.table.update_item(
            Key={"viewpoint_id": viewpoint_id},
            UpdateExpression="SET local_object_path = :local_object_path",
            ExpressionAttributeValues={":local_object_path": path},
            ReturnValues="ALL_NEW",
        )

    def mock_download(self, viewpoint_id):
        """Mock the download of an object to a local path."""
        local_dir = os.path.join("test_tmp", "viewpoints", viewpoint_id)
        local_path = os.path.join(local_dir, TestConfig.test_object_key)
        os.makedirs(local_dir, exist_ok=True)
        shutil.copy(TestConfig.test_file_path, local_path)
        self.mock_set_local_path(viewpoint_id, local_path)

    def mock_extract_metadata(self, viewpoint_id):
        """Mock the extraction of metadata files."""
        local_dir = os.path.join("test_tmp", "viewpoints", viewpoint_id)
        shutil.copy(TestConfig.test_metadata_path, local_dir)
        shutil.copy(TestConfig.test_stats_path, local_dir)
        shutil.copy(TestConfig.test_info_path, local_dir)
        shutil.copy(TestConfig.test_bounds_path, local_dir)

    def mock_create_viewpoint(self) -> str:
        """Create a viewpoint and set its status to READY."""
        viewpoint_data_res = self.client.post("/latest/viewpoints/", json=TEST_BODY)
        viewpoint_data = viewpoint_data_res.json()
        self.mock_set_ready(viewpoint_data["viewpoint_id"])
        self.mock_download(viewpoint_data["viewpoint_id"])
        self.mock_extract_metadata(viewpoint_data["viewpoint_id"])
        return viewpoint_data["viewpoint_id"]

    def test_e2e_list_viewpoints_valid(self):
        """Test listing multiple valid viewpoints."""
        self.client.post("/latest/viewpoints/", json=TEST_BODY)
        self.client.post("/latest/viewpoints/", json=TEST_BODY)
        self.client.post("/latest/viewpoints/", json=TEST_BODY)

        response = self.client.get("/latest/viewpoints/")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["items"]), 3)

    @patch("aws.osml.tile_server.viewpoint.worker", autospec=True)
    def test_e2e_create_viewpoint_valid(self, mock_worker):
        """Test creating a valid viewpoint."""
        response = self.client.post("/latest/viewpoints/", json=TEST_BODY)

        self.assertEqual(response.status_code, 201)
        response_data = response.json()

        with open("test/data/viewpoint_data_sample.json", "r") as output_json:
            expected_json_result = json.load(output_json)
            expected_json_result["viewpoint_id"] = response_data["viewpoint_id"]
            expected_json_result["local_object_path"] = response_data["local_object_path"]

            response_data["expire_time"] = None
            self.assertEqual(response_data, expected_json_result)

    def test_e2e_create_viewpoint_invalid(self):
        """Test creating a viewpoint with invalid data."""
        response = self.client.post("/latest/viewpoints/", json=INVALID_TEST_BODY)
        self.assertEqual(response.status_code, 422)

    def test_e2e_delete_viewpoint_valid(self):
        """Test deleting a valid viewpoint."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.delete(f"/latest/viewpoints/{viewpoint_id}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["viewpoint_status"], ViewpointStatus.DELETED)
        self.assertIsNone(response.json()["local_object_path"])

    def test_e2e_update_viewpoint_valid(self):
        """Test updating a valid viewpoint."""
        viewpoint_data_res = self.client.post("/latest/viewpoints/", json=TEST_BODY)
        viewpoint_data = viewpoint_data_res.json()
        self.mock_set_ready(viewpoint_data["viewpoint_id"])

        update_data_body = VALID_UPDATE_TEST_BODY
        update_data_body["viewpoint_id"] = viewpoint_data["viewpoint_id"]

        response = self.client.put("/latest/viewpoints/", json=update_data_body)

        self.assertEqual(response.status_code, 201)
        self.assertNotEqual(response.json()["viewpoint_name"], viewpoint_data["viewpoint_name"])

    def test_e2e_describe_viewpoint_valid(self):
        """Test describing a valid viewpoint."""
        viewpoint_data_res = self.client.post("/latest/viewpoints/", json=TEST_BODY)
        viewpoint_data = viewpoint_data_res.json()

        response = self.client.get(f"/latest/viewpoints/{viewpoint_data['viewpoint_id']}")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), viewpoint_data)

    def test_e2e_describe_viewpoint_invalid(self):
        """Test describing a viewpoint with an invalid ID."""
        self.client.post("/latest/viewpoints/", json=TEST_BODY)

        with self.assertRaises(Exception):
            response = self.client.get(f"/latest/viewpoints/{TEST_INVALID_VIEWPOINT_ID}")
            self.assertEqual(response.status_code, 422)

    def test_e2e_get_metadata_valid(self):
        """Test retrieving metadata for a valid viewpoint."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/metadata")

        self.assertEqual(response.status_code, 200)

        with open(TestConfig.test_metadata_path, "r") as output_json:
            expected_json_result = json.load(output_json)
            self.assertEqual(response.json(), expected_json_result)

    def test_e2e_get_metadata_invalid(self):
        """Test retrieving metadata for an invalid viewpoint ID."""
        self.mock_create_viewpoint()

        with self.assertRaises(Exception):
            response = self.client.get(f"/latest/viewpoints/{TEST_INVALID_VIEWPOINT_ID}/image/metadata")
            self.assertEqual(response.status_code, 422)

    def test_e2e_get_bounds_valid(self):
        """Test retrieving bounds for a valid viewpoint."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/bounds")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["bounds"], [0, 0, 1024, 1024])

    def test_e2e_get_info_valid(self):
        """Test retrieving info for a valid viewpoint."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/info")

        self.assertEqual(response.status_code, 200)
        response_data = response.json()

        with open(TestConfig.test_info_path, "r") as output_json:
            expected_json_result = json.load(output_json)
            self.assertEqual(response_data, expected_json_result)

    def test_e2e_get_statistics_valid(self):
        """Test retrieving statistics for a valid viewpoint."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/statistics")

        self.assertEqual(response.status_code, 200)
        response_data = response.json()

        with open(TestConfig.test_stats_path, "r") as output_json:
            expected_json_result = json.load(output_json)
            self.assertEqual(response_data, expected_json_result)

    def test_e2e_get_preview(self):
        """Test retrieving a preview image for a valid viewpoint."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/preview.JPEG")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-type"), "image/jpeg")
        self.assertEqual(len(response.content), 250282)

    def test_e2e_get_tile_valid(self):
        """Test retrieving a valid tile."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/tiles/10/10/10.NITF")

        self.assertEqual(response.status_code, 200)

    def test_e2e_get_tile_invalid(self):
        """Test retrieving a tile with an invalid format."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/tiles/10/10/10.bad")

        self.assertEqual(response.status_code, 422)

    def test_e2e_get_crop_min_max(self):
        """Test retrieving a cropped image using min/max coordinates."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/crop/32,32,64,64.PNG")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-type"), "image/png")
        self.assertEqual(len(response.content), 748)

    def test_e2e_get_crop_height_width(self):
        """Test retrieving a cropped image using height and width."""
        viewpoint_id = self.mock_create_viewpoint()
        query_params = {"width": 32, "height": 32}
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/image/crop/32,32,128,128.PNG", params=query_params)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-type"), "image/png")
        self.assertEqual(len(response.content), 748)

    def test_e2e_get_map_tilesets(self):
        """Test retrieving map tilesets for a valid viewpoint."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/map/tiles")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-type"), "application/json")
        json_response = response.json()
        self.assertIn("tilesets", json_response)
        self.assertGreater(len(json_response["tilesets"]), 0)

    def test_e2e_get_map_tileset_metadata(self):
        """Test retrieving metadata for a specific map tileset."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/map/tiles/WebMercatorQuad")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers.get("content-type"), "application/json")
        json_response = response.json()
        self.assertIn("dataType", json_response)
        self.assertIn("tileMatrixSetLimits", json_response)
        self.assertGreater(len(json_response["tileMatrixSetLimits"]), 0)

    def test_e2e_get_map_tile_empty(self):
        """Test retrieving an empty map tile."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/map/tiles/WebMercatorQuad/10/10/10.PNG")

        self.assertEqual(response.status_code, 204)

    def test_e2e_get_map_tile_valid(self):
        """Test retrieving a valid map tile."""
        viewpoint_id = self.mock_create_viewpoint()
        response = self.client.get(f"/latest/viewpoints/{viewpoint_id}/map/tiles/WebMercatorQuad/0/0/0.PNG")
        self.assertEqual(response.status_code, 200)


if __name__ == "__main__":
    unittest.main()
