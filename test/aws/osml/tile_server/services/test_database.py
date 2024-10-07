#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

from unittest import TestCase
from unittest.mock import MagicMock, PropertyMock

import boto3
import pytest
from botocore.exceptions import ClientError
from fastapi import HTTPException
from moto import mock_aws
from test_config import TestConfig

from aws.osml.gdal import RangeAdjustmentType
from aws.osml.tile_server.models import ViewpointModel, ViewpointStatus

# Mock Viewpoint Models
MOCK_VIEWPOINT_1 = ViewpointModel(
    viewpoint_id="1",
    viewpoint_name="test1",
    bucket_name="no bucket",
    object_key="no key",
    tile_size=512,
    range_adjustment=RangeAdjustmentType.NONE,
    viewpoint_status=ViewpointStatus.READY,
    local_object_path=None,
    error_message=None,
    expire_time=None,
)
MOCK_VIEWPOINT_2 = ViewpointModel(
    viewpoint_id="2",
    viewpoint_name="test2",
    bucket_name="no bucket",
    object_key="no key",
    tile_size=512,
    range_adjustment=RangeAdjustmentType.NONE,
    viewpoint_status=ViewpointStatus.READY,
    local_object_path=None,
    error_message=None,
    expire_time=None,
)
MOCK_VIEWPOINT_3 = ViewpointModel(
    viewpoint_id="3",
    viewpoint_name="test3",
    bucket_name="no bucket",
    object_key="no key",
    tile_size=512,
    range_adjustment=RangeAdjustmentType.NONE,
    viewpoint_status=ViewpointStatus.READY,
    local_object_path=None,
    error_message=None,
    expire_time=None,
)
MOCK_VIEWPOINT_4 = ViewpointModel(
    viewpoint_id="4",
    viewpoint_name="test4",
    bucket_name="no bucket",
    object_key="no key",
    tile_size=512,
    range_adjustment=RangeAdjustmentType.NONE,
    viewpoint_status=ViewpointStatus.READY,
    local_object_path=None,
    error_message=None,
    expire_time=None,
)


@mock_aws
class TestViewpointStatusTable(TestCase):
    """Unit tests for the ViewpointStatusTable class."""

    def setUp(self):
        """Set up the DynamoDB mock environment."""
        from aws.osml.tile_server.app_config import BotoConfig

        self.ddb = boto3.resource("dynamodb", config=BotoConfig.default)
        self.table = self.ddb.create_table(
            TableName=TestConfig.test_viewpoint_table_name,
            KeySchema=TestConfig.test_viewpoint_key_schema,
            AttributeDefinitions=TestConfig.test_viewpoint_attribute_def,
            BillingMode="PAY_PER_REQUEST",
        )

    def tearDown(self):
        """Clean up the mock environment."""
        self.ddb = None
        self.table = None

    def test_viewpoint_status_table_initialization(self):
        """Test the initialization of ViewpointStatusTable."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)
        self.assertEqual(viewpoint_status_table.ddb, self.ddb)

    def test_viewpoint_status_table_bad_table(self):
        """Test initialization of ViewpointStatusTable with a bad table."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        mock_ddb = MagicMock()
        mock_table = MagicMock()
        type(mock_table).table_status = PropertyMock(
            side_effect=ClientError({"Error": {"Code": 500, "Message": "Mock Error"}}, "table_status")
        )
        mock_ddb.Table.return_value = mock_table

        with pytest.raises(ClientError):
            ViewpointStatusTable(mock_ddb)

    def test_get_viewpoints_no_params(self):
        """Test retrieval of all viewpoints with no parameters."""
        from aws.osml.tile_server.models import ViewpointListResponse
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        expected_viewpoints = ViewpointListResponse(
            items=[MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4], next_token=None
        )
        viewpoints = viewpoint_status_table.get_viewpoints()
        self.assertEqual(viewpoints, expected_viewpoints)

    def test_get_viewpoints_with_limit(self):
        """Test retrieval of viewpoints with a limit."""
        from aws.osml.tile_server.models import ViewpointListResponse
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        expected_viewpoints = ViewpointListResponse(
            items=[MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3], next_token="3"
        )
        viewpoints = viewpoint_status_table.get_viewpoints(limit=3)
        self.assertEqual(viewpoints, expected_viewpoints)

    def test_get_viewpoints_with_next_token(self):
        """Test retrieval of viewpoints with a next_token."""
        from aws.osml.tile_server.models import ViewpointListResponse
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        expected_viewpoints = ViewpointListResponse(items=[MOCK_VIEWPOINT_4], next_token=None)
        viewpoints = viewpoint_status_table.get_viewpoints(next_token="3")
        self.assertEqual(viewpoints, expected_viewpoints)

    def test_get_viewpoints_with_limit_and_next_token(self):
        """Test retrieval of viewpoints with both limit and next_token."""
        from aws.osml.tile_server.models import ViewpointListResponse
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        expected_viewpoints = ViewpointListResponse(items=[MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3], next_token="3")
        viewpoints = viewpoint_status_table.get_viewpoints(limit=2, next_token="1")
        self.assertEqual(viewpoints, expected_viewpoints)

    def test_get_viewpoints_client_error(self):
        """Test handling of ClientError during viewpoint retrieval."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        viewpoint_status_table.get_all_viewpoints = MagicMock(
            side_effect=ClientError({"Error": {"Code": 500, "Message": "Mock Error"}}, "scan")
        )
        with pytest.raises(HTTPException):
            viewpoint_status_table.get_viewpoints()

    def test_get_viewpoints_key_error(self):
        """Test handling of KeyError during viewpoint retrieval."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        viewpoint_status_table.get_all_viewpoints = MagicMock(side_effect=KeyError())
        with pytest.raises(HTTPException):
            viewpoint_status_table.get_viewpoints()

    def test_get_viewpoints_other_exception(self):
        """Test handling of a general exception during viewpoint retrieval."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        viewpoint_status_table.get_all_viewpoints = MagicMock(side_effect=ValueError())
        with pytest.raises(HTTPException):
            viewpoint_status_table.get_viewpoints()

    def test_get_all_viewpoints(self):
        """Test retrieving all viewpoints with no pagination."""
        from aws.osml.tile_server.models import ViewpointListResponse
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        expected_viewpoints = ViewpointListResponse(
            items=[MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4], next_token=None
        )
        viewpoints = viewpoint_status_table.get_all_viewpoints({"Limit": 2})
        self.assertEqual(viewpoints, expected_viewpoints)

    def test_get_paged_viewpoints(self):
        """Test retrieving paged viewpoints."""
        from aws.osml.tile_server.models import ViewpointListResponse
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        expected_viewpoints = ViewpointListResponse(items=[MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2], next_token="2")
        viewpoints = viewpoint_status_table.get_paged_viewpoints({"Limit": 2})
        self.assertEqual(viewpoints, expected_viewpoints)

    def test_get_paged_viewpoints_last_page(self):
        """Test retrieving the last page of paged viewpoints."""
        from aws.osml.tile_server.models import ViewpointListResponse
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        for viewpoint in [MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4]:
            viewpoint_status_table.create_viewpoint(viewpoint)

        expected_viewpoints = ViewpointListResponse(
            items=[MOCK_VIEWPOINT_1, MOCK_VIEWPOINT_2, MOCK_VIEWPOINT_3, MOCK_VIEWPOINT_4], next_token=None
        )
        viewpoints = viewpoint_status_table.get_paged_viewpoints({"Limit": 5})
        self.assertEqual(viewpoints, expected_viewpoints)

    def test_get_single_viewpoint(self):
        """Test retrieving a single viewpoint by ID."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

        result = viewpoint_status_table.get_viewpoint("1")
        self.assertEqual(result, MOCK_VIEWPOINT_1)

    def test_get_viewpoint_client_error(self):
        """Test handling of ClientError when retrieving a single viewpoint."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)
        viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

        mock_table = MagicMock()
        mock_table.get_item.side_effect = ClientError({"Error": {"Code": 500, "Message": "Mock Error"}}, "get_item")
        viewpoint_status_table.table = mock_table

        with pytest.raises(HTTPException):
            viewpoint_status_table.get_viewpoint("1")

    def test_get_viewpoint_key_error(self):
        """Test handling of KeyError when retrieving a single viewpoint."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)
        viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

        with pytest.raises(HTTPException):
            viewpoint_status_table.get_viewpoint("123")

    def test_get_viewpoint_other_exception(self):
        """Test handling of a general exception when retrieving a single viewpoint."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)
        viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

        mock_table = MagicMock()
        mock_table.get_item.side_effect = ValueError()
        viewpoint_status_table.table = mock_table

        with pytest.raises(HTTPException):
            viewpoint_status_table.get_viewpoint("1")

    def test_create_viewpoint(self):
        """Test creation of a new viewpoint."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        result = viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)
        expected_result = {
            "bucket_name": "no bucket",
            "error_message": None,
            "expire_time": None,
            "local_object_path": None,
            "object_key": "no key",
            "range_adjustment": RangeAdjustmentType.NONE,
            "tile_size": 512,
            "viewpoint_id": "1",
            "viewpoint_name": "test1",
            "viewpoint_status": ViewpointStatus.READY,
        }
        self.assertEqual(result, expected_result)

    def test_create_viewpoint_client_error(self):
        """Test handling of ClientError during viewpoint creation."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        mock_table = MagicMock()
        mock_table.put_item.side_effect = ClientError({"Error": {"Code": 500, "Message": "Mock Error"}}, "put_item")
        viewpoint_status_table.table = mock_table

        with pytest.raises(HTTPException):
            viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

    def test_update_viewpoint(self):
        """Test updating an existing viewpoint."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

        updated_viewpoint = ViewpointModel(
            viewpoint_id="1",
            viewpoint_name="test1",
            bucket_name="not this bucket",
            object_key="no key",
            tile_size=1024,
            range_adjustment=RangeAdjustmentType.NONE,
            viewpoint_status=ViewpointStatus.READY,
            local_object_path=None,
            error_message=None,
            expire_time=None,
        )
        update_result = viewpoint_status_table.update_viewpoint(updated_viewpoint)

        self.assertEqual(update_result, updated_viewpoint)

    def test_update_viewpoint_client_error(self):
        """Test handling of ClientError during viewpoint update."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

        updated_viewpoint = ViewpointModel(
            viewpoint_id="1",
            viewpoint_name="test1",
            bucket_name="not this bucket",
            object_key="no key",
            tile_size=1024,
            range_adjustment=RangeAdjustmentType.NONE,
            viewpoint_status=ViewpointStatus.READY,
            local_object_path=None,
            error_message=None,
            expire_time=None,
        )
        mock_table = MagicMock()
        mock_table.update_item.side_effect = ClientError({"Error": {"Code": 500, "Message": "Mock Error"}}, "update_item")
        viewpoint_status_table.table = mock_table

        with pytest.raises(HTTPException):
            viewpoint_status_table.update_viewpoint(updated_viewpoint)

    def test_update_viewpoint_other_exception(self):
        """Test handling of a general exception during viewpoint update."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

        updated_viewpoint = ViewpointModel(
            viewpoint_id="1",
            viewpoint_name="test1",
            bucket_name="not this bucket",
            object_key="no key",
            tile_size=1024,
            range_adjustment=RangeAdjustmentType.NONE,
            viewpoint_status=ViewpointStatus.READY,
            local_object_path=None,
            error_message=None,
            expire_time=None,
        )
        mock_table = MagicMock()
        mock_table.update_item.side_effect = ValueError()
        viewpoint_status_table.table = mock_table

        with pytest.raises(HTTPException):
            viewpoint_status_table.update_viewpoint(updated_viewpoint)

    def test_update_params(self):
        """Test generation of update parameters for a viewpoint."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        mock_update_params = {"viewpoint_id": "12345", "name": "John Doe", "age": 42, "aws_employee": True}
        update_expression, update_attr = viewpoint_status_table.get_update_params(mock_update_params)

        self.assertEqual(update_expression, "SET name = :name, age = :age, aws_employee = :aws_employee")
        self.assertEqual(update_attr, {":name": "John Doe", ":age": 42, ":aws_employee": True})

    def test_delete_viewpoint(self):
        """Test deleting a viewpoint from the DynamoDB table."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        # Create a viewpoint to be deleted
        viewpoint_status_table.create_viewpoint(MOCK_VIEWPOINT_1)

        # Delete the viewpoint
        viewpoint_status_table.delete_viewpoint(MOCK_VIEWPOINT_1.viewpoint_id)

        # Try to retrieve the deleted viewpoint
        with pytest.raises(HTTPException):
            viewpoint_status_table.get_viewpoint(MOCK_VIEWPOINT_1.viewpoint_id)

    def test_delete_viewpoint_client_error(self):
        """Test handling of ClientError during viewpoint deletion."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        mock_table = MagicMock()
        mock_table.delete_item.side_effect = ClientError({"Error": {"Code": 500, "Message": "Mock Error"}}, "delete_item")
        viewpoint_status_table.table = mock_table

        with pytest.raises(HTTPException):
            viewpoint_status_table.delete_viewpoint(MOCK_VIEWPOINT_1.viewpoint_id)

    def test_delete_viewpoint_other_exception(self):
        """Test handling of a general exception during viewpoint deletion."""
        from aws.osml.tile_server.services import ViewpointStatusTable

        viewpoint_status_table = ViewpointStatusTable(self.ddb)

        mock_table = MagicMock()
        mock_table.delete_item.side_effect = ValueError()
        viewpoint_status_table.table = mock_table

        with pytest.raises(HTTPException):
            viewpoint_status_table.delete_viewpoint(MOCK_VIEWPOINT_1.viewpoint_id)
