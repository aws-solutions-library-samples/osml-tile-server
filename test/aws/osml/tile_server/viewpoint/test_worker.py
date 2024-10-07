#  Copyright 2023-2024 Amazon.com, Inc or its affiliates.

import copy
from threading import Event
from unittest import TestCase
from unittest.mock import MagicMock, mock_open, patch

import pytest
from botocore.exceptions import ClientError

from aws.osml.gdal import RangeAdjustmentType
from aws.osml.tile_server.models import ViewpointModel, ViewpointStatus
from aws.osml.tile_server.viewpoint import SupplementaryFileType


class TestViewpointWorker(TestCase):
    """Unit tests for the ViewpointWorker class."""

    def setUp(self):
        """Set up the mock environment for each test."""
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        self.mock_queue = MagicMock(name="queue")
        self.mock_s3 = MagicMock(name="S3")
        self.mock_ddb = MagicMock(name="ddb")
        self.worker = ViewpointWorker(self.mock_queue, self.mock_s3, self.mock_ddb)

    def tearDown(self):
        """Clean up the environment after each test."""
        self.worker = None

    def test_viewpoint_worker_initialization(self):
        """Test the initialization of the ViewpointWorker."""
        self.assertTrue(self.worker.daemon)
        self.assertIsInstance(self.worker.stop_event, Event)

    @pytest.mark.skip(reason="Test not implemented")
    def test_join(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_run(self):
        pass

    def test_download_image_successful(self):
        """Test successful image download."""
        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)
        self.worker._create_local_tmp_directory = MagicMock(return_value="/tmp/1/no_key")
        self.worker._download_s3_file_to_local_tmp = MagicMock(return_value=(None, None))
        self.worker._download_supplementary_file = MagicMock()

        self.worker.download_image(mock_viewpoint)

        self.worker._download_supplementary_file.assert_called_with(mock_viewpoint, SupplementaryFileType.AUX)

    def test_download_image_failed(self):
        """Test image download failure handling."""
        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)
        self.worker._create_local_tmp_directory = MagicMock(return_value="/tmp/1/no_key")
        self.worker._download_s3_file_to_local_tmp = MagicMock(return_value=(ViewpointStatus.FAILED, "Failed"))
        self.worker._download_supplementary_file = MagicMock()

        self.worker.download_image(mock_viewpoint)

        self.assertEqual(mock_viewpoint.viewpoint_status, ViewpointStatus.FAILED)
        self.assertEqual(mock_viewpoint.error_message, "Failed")
        self.worker._download_supplementary_file.assert_not_called()

    @pytest.mark.skip(reason="Test not implemented")
    def test_create_tile_pyramid(self):
        pass

    def test_create_tile_pyramid_exception(self):
        """Test handling exceptions during tile pyramid creation."""
        self.worker.get_default_tile_factory_pool_for_viewpoint = MagicMock(side_effect=ValueError("Mock Error"))
        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)

        self.worker.create_tile_pyramid(mock_viewpoint)

        self.assertEqual(mock_viewpoint.viewpoint_status, ViewpointStatus.FAILED)
        self.assertEqual(mock_viewpoint.error_message, "Unable to create sample tile for viewpoint: 1! Error=Mock Error")

    def test_extract_metadata_successful(self):
        """Test successful metadata extraction."""
        self.worker.get_default_tile_factory_pool_for_viewpoint = MagicMock()
        self.worker._write_metadata = MagicMock()
        self.worker._write_bounds = MagicMock()
        self.worker._write_info = MagicMock()
        self.worker._write_statistics = MagicMock()

        self.worker.extract_metadata(MOCK_VIEWPOINT_ITEM)

        self.worker._write_metadata.assert_called_once()
        self.worker._write_bounds.assert_called_once()
        self.worker._write_info.assert_called_once()
        self.worker._write_statistics.assert_called_once()

    def test_extract_metadata_exception(self):
        """Test handling exceptions during metadata extraction."""
        self.worker.get_default_tile_factory_pool_for_viewpoint = MagicMock(side_effect=ValueError("Mock Error"))
        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)

        self.worker.extract_metadata(mock_viewpoint)

        self.assertEqual(mock_viewpoint.viewpoint_status, ViewpointStatus.FAILED)
        self.assertEqual(mock_viewpoint.error_message, "Unable to extract metadata for viewpoint: 1! Error=Mock Error")

    def test_process_message_requested(self):
        """Test processing a message with status REQUESTED."""
        mock_message = MagicMock()
        mock_message.body = (
            '{"viewpoint_id": "1", "viewpoint_name": "mock_name", "viewpoint_status": "REQUESTED", '
            '"bucket_name": "mock_bucket", "object_key": "mock_object", "tile_size": 512, '
            '"range_adjustment": "NONE", "local_object_path": null, "error_message": null, "expire_time": null}'
        )
        mock_message.delete = MagicMock()

        self.worker.download_image = MagicMock()
        self.worker.create_tile_pyramid = MagicMock()
        self.worker.extract_metadata = MagicMock()
        self.worker._update_status = MagicMock()

        self.worker._process_message(mock_message)

        self.worker.download_image.assert_called_once()
        self.worker.create_tile_pyramid.assert_called_once()
        self.worker.extract_metadata.assert_called_once()
        self.worker._update_status.assert_called_once()
        mock_message.delete.assert_called_once()

    def test_process_message_not_requested(self):
        """Test processing a message with a status other than REQUESTED."""
        mock_message = MagicMock()
        mock_message.body = (
            '{"viewpoint_id": "1", "viewpoint_name": "mock_name", "viewpoint_status": "READY", '
            '"bucket_name": "mock_bucket", "object_key": "mock_object", "tile_size": 512, '
            '"range_adjustment": "NONE", "local_object_path": null, "error_message": null, "expire_time": null}'
        )
        mock_message.delete = MagicMock()

        self.worker.download_image = MagicMock()
        self.worker.create_tile_pyramid = MagicMock()
        self.worker.extract_metadata = MagicMock()
        self.worker._update_status = MagicMock()

        self.worker._process_message(mock_message)

        self.worker.download_image.assert_not_called()
        self.worker.create_tile_pyramid.assert_not_called()
        self.worker.extract_metadata.assert_not_called()
        self.worker._update_status.assert_not_called()
        mock_message.delete.assert_not_called()

    def test_update_status_ready(self):
        """Test updating the viewpoint status to READY."""
        self.worker.viewpoint_request_queue = self.mock_queue
        self.worker.viewpoint_database = self.mock_ddb

        self.worker._update_status(MOCK_VIEWPOINT_ITEM)

        expected_viewpoint_item = copy.deepcopy(MOCK_VIEWPOINT_ITEM)
        expected_viewpoint_item.viewpoint_status = ViewpointStatus.READY

        self.mock_ddb.update_viewpoint.assert_called_with(expected_viewpoint_item)

    @patch("aws.osml.tile_server.viewpoint.worker.ServerConfig")
    def test_create_local_tmp_directory(self, mock_server_config):
        """Test creating a local temporary directory."""
        mock_server_config.efs_mount_name = "tmp"
        path = self.worker._create_local_tmp_directory(MOCK_VIEWPOINT_ITEM)

        self.assertEqual(path, "/tmp/1/no_key")

    def test_download_s3_file_to_local_tmp_successful(self):
        """Test successful download of an S3 file to a local temporary directory."""
        viewpoint_status, error_message = self.worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)
        self.assertIsNone(viewpoint_status)
        self.assertIsNone(error_message)

    def test_download_s3_file_to_local_tmp_client_error_404(self):
        """Test handling a 404 ClientError during S3 file download."""
        self.mock_s3.meta.client.download_file = MagicMock(
            side_effect=ClientError({"Error": {"Code": "404", "Message": "Mock Error"}}, "download_file")
        )

        worker = self.worker
        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)

        self.assertEqual(viewpoint_status, ViewpointStatus.FAILED)
        self.assertIn("An error occurred (404)", error_message)
        self.assertIn("The no_bucket bucket does not exist!", error_message)

    def test_download_s3_file_to_local_tmp_client_error_403(self):
        """Test handling a 403 ClientError during S3 file download."""
        self.mock_s3.meta.client.download_file = MagicMock(
            side_effect=ClientError({"Error": {"Code": "403", "Message": "Mock Error"}}, "download_file")
        )

        worker = self.worker
        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)

        self.assertEqual(viewpoint_status, ViewpointStatus.FAILED)
        self.assertIn("An error occurred (403)", error_message)
        self.assertIn("You do not have permission to access no_bucket bucket!", error_message)

    def test_download_s3_file_to_local_tmp_client_error_400(self):
        """Test handling a 400 ClientError during S3 file download."""
        self.mock_s3.meta.client.download_file = MagicMock(
            side_effect=ClientError({"Error": {"Code": "400", "Message": "Mock Error"}}, "download_file")
        )

        worker = self.worker
        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)

        self.assertEqual(viewpoint_status, ViewpointStatus.FAILED)
        self.assertIn("An error occurred (400)", error_message)

    def test_download_s3_file_to_local_tmp_other_exception(self):
        """Test handling a generic exception during S3 file download."""
        self.mock_s3.meta.client.download_file = MagicMock(side_effect=ValueError("Mock error"))

        worker = self.worker
        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)

        self.assertEqual(viewpoint_status, ViewpointStatus.FAILED)
        self.assertIn("Something went wrong!", error_message)
        self.assertIn("Mock error", error_message)

    def test_download_supplementary_file_overview(self):
        """Test downloading a supplementary OVERVIEW file."""
        self.worker._download_supplementary_file(MOCK_VIEWPOINT_ITEM_2, SupplementaryFileType.OVERVIEW)

        self.mock_s3.meta.client.download_file.assert_called_with("no_bucket", "no_key.ovr", "/tmp/1/no_key.ovr")

    def test_download_supplementary_file_aux(self):
        """Test downloading a supplementary AUX file."""
        self.worker._download_supplementary_file(MOCK_VIEWPOINT_ITEM_2, SupplementaryFileType.AUX)

        self.mock_s3.meta.client.download_file.assert_called_with("no_bucket", "no_key.aux.xml", "/tmp/1/no_key.aux.xml")

    def test_download_supplementary_file_client_error(self):
        """Test handling a ClientError during supplementary file download."""
        self.mock_s3.meta.client.download_file = MagicMock(
            side_effect=ClientError({"Error": {"Code": 500, "Message": "Mock Error"}}, "download_file")
        )
        mock_logger = MagicMock()
        self.worker.logger = mock_logger

        self.worker._download_supplementary_file(MOCK_VIEWPOINT_ITEM_2, SupplementaryFileType.AUX)

        self.mock_s3.meta.client.download_file.assert_called_with("no_bucket", "no_key.aux.xml", "/tmp/1/no_key.aux.xml")
        mock_logger.info.assert_called_with("No aux file available for 1")

    @patch("aws.osml.tile_server.viewpoint.worker.get_tile_factory_pool")
    def test_get_default_tile_factory_pool_for_viewpoint_no_range_adjustment(self, mock_get_tile_factory_pool):
        """Test getting the default tile factory pool with no range adjustment."""
        from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats

        self.worker.get_default_tile_factory_pool_for_viewpoint(MOCK_VIEWPOINT_ITEM)

        mock_get_tile_factory_pool.assert_called_with(
            GDALImageFormats.PNG, GDALCompressionOptions.NONE, None, None, RangeAdjustmentType.NONE
        )

    @patch("aws.osml.tile_server.viewpoint.worker.get_tile_factory_pool")
    def test_get_default_tile_factory_pool_for_viewpoint_with_range_adjustment(self, mock_get_tile_factory_pool):
        """Test getting the default tile factory pool with range adjustment."""
        from osgeo import gdalconst

        from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats

        self.worker.get_default_tile_factory_pool_for_viewpoint(MOCK_VIEWPOINT_ITEM_4)

        mock_get_tile_factory_pool.assert_called_with(
            GDALImageFormats.PNG, GDALCompressionOptions.NONE, None, gdalconst.GDT_Byte, RangeAdjustmentType.DRA
        )

    @patch("aws.osml.tile_server.viewpoint.worker.gdal")
    def test_calculate_image_statistics(self, mock_gdal):
        """Test calculating image statistics."""
        mock_gdal_open = MagicMock()
        mock_gdal_info = MagicMock()
        mock_gdal.Open = mock_gdal_open
        mock_gdal.Info = mock_gdal_info

        self.worker._calculate_image_statistics(MOCK_VIEWPOINT_ITEM_2)

        mock_gdal_open.assert_called_once_with("/tmp/1/no_key")
        mock_gdal_info.assert_called_once()

    @patch("aws.osml.tile_server.viewpoint.worker.get_standard_overviews")
    def test_create_image_pyramid(self, mock_get_overviews):
        """Test creating an image pyramid."""
        mock_tile_factory = MagicMock()
        mock_tile_factory.raster_dataset.BuildOverviews = MagicMock()
        mock_get_overviews.return_value = "mock overviews"

        self.worker._create_image_pyramid(mock_tile_factory, MOCK_VIEWPOINT_ITEM_2)

        mock_tile_factory.raster_dataset.BuildOverviews.assert_called_once_with("CUBIC", "mock overviews")

    def test_verify_tile_creation(self):
        """Test verifying tile creation."""
        mock_image_bytes = b"mock image bytes"
        mock_tile_factory = MagicMock()
        mock_tile_factory.create_encoded_tile.return_value = mock_image_bytes

        image_bytes = self.worker._verify_tile_creation(mock_tile_factory, MOCK_VIEWPOINT_ITEM)

        self.assertEqual(image_bytes, mock_image_bytes)

    def test_write_metadata(self):
        """Test writing metadata to a file."""
        mock_tile_factory = MagicMock()
        mock_tile_factory.raster_dataset.GetMetadata.return_value = {"data": "mock"}
        open_mock = mock_open()
        with patch("aws.osml.tile_server.viewpoint.worker.open", open_mock, create=True):
            self.worker._write_metadata(mock_tile_factory, MOCK_VIEWPOINT_ITEM_2)

        open_mock.assert_called_with("/tmp/1/no_key.metadata", "w")
        open_mock.return_value.write.assert_called_once_with('{"metadata": {"data": "mock"}}')

    def test_write_bounds(self):
        """Test writing bounds to a file."""
        mock_tile_factory = MagicMock()
        mock_tile_factory.raster_dataset.RasterXSize = 512
        mock_tile_factory.raster_dataset.RasterYSize = 256
        open_mock = mock_open()
        with patch("aws.osml.tile_server.viewpoint.worker.open", open_mock, create=True):
            self.worker._write_bounds(mock_tile_factory, MOCK_VIEWPOINT_ITEM_2)

        open_mock.assert_called_with("/tmp/1/no_key.bounds", "w")
        open_mock.return_value.write.assert_called_once_with('{"bounds": [0, 0, 512, 256]}')

    def test_write_info(self):
        """Test writing info to a GeoJSON file."""
        mock_world_coordinate = MagicMock()
        mock_world_coordinate.latitude = 0.6
        mock_world_coordinate.longitude = 0.35
        mock_tile_factory = MagicMock()
        mock_tile_factory.raster_dataset.RasterXSize = 512
        mock_tile_factory.raster_dataset.RasterYSize = 256
        mock_tile_factory.sensor_model.image_to_world.return_value = mock_world_coordinate
        open_mock = mock_open()
        with patch("aws.osml.tile_server.viewpoint.worker.open", open_mock, create=True):
            self.worker._write_info(mock_tile_factory, MOCK_VIEWPOINT_ITEM_2)

        open_mock.assert_called_with("/tmp/1/no_key.geojson", "w")
        expected_file_contents = (
            '{"type": "FeatureCollection", "features": [{"type": "Feature", "id": "test1", "geometry": {"type": '
            '"Polygon", "coordinates": [[[20.053523, 34.377468], [20.053523, 34.377468], [20.053523, 34.377468], '
            '[20.053523, 34.377468], [20.053523, 34.377468]]]}, "properties": {}}]}'
        )
        open_mock.return_value.write.assert_called_once_with(expected_file_contents)

    @patch("aws.osml.tile_server.viewpoint.worker.gdal")
    def test_write_statistics(self, mock_gdal):
        """Test writing image statistics to a file."""
        mock_gdal.Info.return_value = {"mock": "gdal info"}

        open_mock = mock_open()
        with patch("aws.osml.tile_server.viewpoint.worker.open", open_mock, create=True):
            self.worker._write_statistics(MOCK_VIEWPOINT_ITEM_2)

        open_mock.assert_called_with("/tmp/1/no_key.stats", "w")
        open_mock.return_value.write.assert_called_once_with('{"image_statistics": {"mock": "gdal info"}}')


MOCK_VIEWPOINT_ITEM = ViewpointModel(
    viewpoint_id="1",
    viewpoint_name="test1",
    bucket_name="no_bucket",
    object_key="no_key",
    tile_size=512,
    range_adjustment=RangeAdjustmentType.NONE,
    viewpoint_status=ViewpointStatus.REQUESTED,
    local_object_path=None,
    error_message=None,
    expire_time=None,
)

MOCK_VIEWPOINT_ITEM_2 = ViewpointModel(
    viewpoint_id="1",
    viewpoint_name="test1",
    bucket_name="no_bucket",
    object_key="no_key",
    tile_size=512,
    range_adjustment=RangeAdjustmentType.NONE,
    viewpoint_status=ViewpointStatus.REQUESTED,
    local_object_path="/tmp/1/no_key",
    error_message=None,
    expire_time=None,
)

MOCK_VIEWPOINT_ITEM_4 = ViewpointModel(
    viewpoint_id="1",
    viewpoint_name="test1",
    bucket_name="no_bucket",
    object_key="no_key",
    tile_size=512,
    range_adjustment=RangeAdjustmentType.DRA,
    viewpoint_status=ViewpointStatus.REQUESTED,
    local_object_path=None,
    error_message=None,
    expire_time=None,
)
