#  Copyright 2023-2024 Amazon.com, Inc or its affiliates.

import copy
from datetime import UTC, datetime, timedelta
from threading import Event
from unittest import TestCase
from unittest.mock import MagicMock, mock_open, patch

import pytest
from botocore.exceptions import ClientError

from aws.osml.gdal import RangeAdjustmentType
from aws.osml.tile_server.viewpoint import ViewpointModel, ViewpointStatus

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

MOCK_VIEWPOINT_ITEM_3 = ViewpointModel(
    viewpoint_id="1",
    viewpoint_name="test1",
    bucket_name="no_bucket",
    object_key="no_key",
    tile_size=512,
    range_adjustment=RangeAdjustmentType.NONE,
    viewpoint_status=ViewpointStatus.FAILED,
    local_object_path=None,
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


class TestViewpointWorker(TestCase):
    def setUp(self):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        self.worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

    def tearDown(self):
        self.worker = None

    def test_viewpoint_worker(self):
        assert self.worker.daemon
        assert isinstance(self.worker.stop_event, Event)

    @pytest.mark.skip(reason="Test not implemented")
    def test_join(self):
        pass

    @pytest.mark.skip(reason="Test not implemented")
    def test_run(self):
        pass

    def test_download_image(self):
        from aws.osml.tile_server.viewpoint import SupplementaryFileType

        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)
        mock_create_local_tmp_directory = MagicMock(return_value="/tmp/1/no_key")
        mock_download_s3_file = MagicMock(return_value=(None, None))
        mock_supplemental_download = MagicMock()
        self.worker._create_local_tmp_directory = mock_create_local_tmp_directory
        self.worker._download_s3_file_to_local_tmp = mock_download_s3_file
        self.worker._download_supplementary_file = mock_supplemental_download
        self.worker.download_image(mock_viewpoint)
        mock_supplemental_download.assert_called_with(mock_viewpoint, SupplementaryFileType.AUX)

    def test_download_image_failed(self):
        from aws.osml.tile_server.viewpoint import ViewpointStatus

        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)
        mock_create_local_tmp_directory = MagicMock(return_value="/tmp/1/no_key")
        mock_download_s3_file = MagicMock(return_value=(ViewpointStatus.FAILED, "Failed"))
        mock_supplemental_download = MagicMock()
        self.worker._create_local_tmp_directory = mock_create_local_tmp_directory
        self.worker._download_s3_file_to_local_tmp = mock_download_s3_file
        self.worker._download_supplementary_file = mock_supplemental_download
        self.worker.download_image(mock_viewpoint)
        assert mock_viewpoint.viewpoint_status == ViewpointStatus.FAILED
        assert mock_viewpoint.error_message == "Failed"
        mock_supplemental_download.assert_not_called()

    @pytest.mark.skip(reason="Test not implemented")
    def test_create_tile_pyramid(self):
        pass

    def test_create_tile_pyramid_exception(self):
        self.worker.get_default_tile_factory_pool_for_viewpoint = MagicMock(side_effect=ValueError("Mock Error"))
        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)

        self.worker.create_tile_pyramid(mock_viewpoint)

        assert mock_viewpoint.viewpoint_status == ViewpointStatus.FAILED
        assert mock_viewpoint.error_message == "Unable to create sample tile for viewpoint: 1! Error=Mock Error"

    def test_extract_metadata(self):
        mock_metadata = MagicMock()
        mock_bounds = MagicMock()
        mock_info = MagicMock()
        mock_statistics = MagicMock()
        self.worker.get_default_tile_factory_pool_for_viewpoint = MagicMock()
        self.worker._write_metadata = mock_metadata
        self.worker._write_bounds = mock_bounds
        self.worker._write_info = mock_info
        self.worker._write_statistics = mock_statistics

        self.worker.extract_metadata(MOCK_VIEWPOINT_ITEM)

        mock_metadata.assert_called_once()
        mock_bounds.assert_called_once()
        mock_info.assert_called_once()
        mock_statistics.assert_called_once()

    def test_extract_metadata_exception(self):
        self.worker.get_default_tile_factory_pool_for_viewpoint = MagicMock(side_effect=ValueError("Mock Error"))

        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)

        self.worker.extract_metadata(mock_viewpoint)

        assert mock_viewpoint.viewpoint_status == ViewpointStatus.FAILED
        assert mock_viewpoint.error_message == "Unable to extract metadata for viewpoint: 1! Error=Mock Error"

    def test_private_process_message(self):
        mock_message = MagicMock()
        mock_delete = MagicMock()
        mock_message.body = (
            '{"viewpoint_id": "1", "viewpoint_name": "mock_name", "viewpoint_status": "REQUESTED", '
            '"bucket_name": "mock_bucket", "object_key": "mock_object", "tile_size": 512, '
            '"range_adjustment": "NONE", "local_object_path": null, "error_message": null, "expire_time": null}'
        )
        mock_message.delete = mock_delete

        mock_download_image = MagicMock()
        mock_create_tile_pyramid = MagicMock()
        mock_extract_metadata = MagicMock()
        mock_update_status = MagicMock()
        self.worker.download_image = mock_download_image
        self.worker.create_tile_pyramid = mock_create_tile_pyramid
        self.worker.extract_metadata = mock_extract_metadata
        self.worker._update_status = mock_update_status

        self.worker._process_message(mock_message)

        mock_download_image.assert_called_once()
        mock_create_tile_pyramid.assert_called_once()
        mock_extract_metadata.assert_called_once()
        mock_update_status.assert_called_once()
        mock_delete.assert_called_once()

    def test_private_process_message_not_requested(self):
        mock_message = MagicMock()
        mock_delete = MagicMock()
        mock_message.body = (
            '{"viewpoint_id": "1", "viewpoint_name": "mock_name", "viewpoint_status": "READY", '
            '"bucket_name": "mock_bucket", "object_key": "mock_object", "tile_size": 512, '
            '"range_adjustment": "NONE", "local_object_path": null, "error_message": null, "expire_time": null}'
        )
        mock_message.delete = mock_delete

        mock_download_image = MagicMock()
        mock_create_tile_pyramid = MagicMock()
        mock_extract_metadata = MagicMock()
        mock_update_status = MagicMock()
        self.worker.download_image = mock_download_image
        self.worker.create_tile_pyramid = mock_create_tile_pyramid
        self.worker.extract_metadata = mock_extract_metadata
        self.worker._update_status = mock_update_status

        self.worker._process_message(mock_message)

        mock_download_image.assert_not_called()
        mock_create_tile_pyramid.assert_not_called()
        mock_extract_metadata.assert_not_called()
        mock_update_status.assert_not_called()
        mock_delete.assert_not_called()

    def test_private_update_status_ready(self):
        from aws.osml.gdal import RangeAdjustmentType
        from aws.osml.tile_server.viewpoint import ViewpointModel, ViewpointStatus, ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        worker._update_status(MOCK_VIEWPOINT_ITEM)
        updated_viewpoint_item = ViewpointModel(
            viewpoint_id="1",
            viewpoint_name="test1",
            bucket_name="no_bucket",
            object_key="no_key",
            tile_size=512,
            range_adjustment=RangeAdjustmentType.NONE,
            viewpoint_status=ViewpointStatus.READY,
            local_object_path=None,
            error_message=None,
            expire_time=None,
        )
        mock_ddb.update_viewpoint.assert_called_with(updated_viewpoint_item)

    @patch("datetime.datetime")
    def test_private_update_status_failed(self, mock_datetime):
        from aws.osml.gdal import RangeAdjustmentType
        from aws.osml.tile_server.viewpoint import ViewpointModel, ViewpointStatus, ViewpointWorker

        mock_now = datetime.now(UTC)
        mock_datetime.now.return_value = mock_now
        mock_tomorrow = mock_now + timedelta(days=1)
        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        worker._update_status(MOCK_VIEWPOINT_ITEM_3)
        updated_viewpoint_item = ViewpointModel(
            viewpoint_id="1",
            viewpoint_name="test1",
            bucket_name="no_bucket",
            object_key="no_key",
            tile_size=512,
            range_adjustment=RangeAdjustmentType.NONE,
            viewpoint_status=ViewpointStatus.FAILED,
            local_object_path=None,
            error_message=None,
            expire_time=int(mock_tomorrow.timestamp()),
        )
        mock_ddb.update_viewpoint.assert_called_with(updated_viewpoint_item)

    @patch("aws.osml.tile_server.viewpoint.worker.ServerConfig")
    def test_private_create_local_tmp_directory(self, mock_server_config):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        mock_server_config.efs_mount_name = "tmp"

        path = worker._create_local_tmp_directory(MOCK_VIEWPOINT_ITEM)
        assert path == "/tmp/1/no_key"

    def test_private_download_s3_file_to_local_tmp(self):
        viewpoint_status, error_message = self.worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)
        assert viewpoint_status is None
        assert error_message is None

    def test_private_download_s3_file_to_local_tmp_client_error_404(self):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")

        mock_s3.meta.client.download_file = MagicMock(
            side_effect=ClientError({"Error": {"Code": "404", "Message": "Mock Error"}}, "download_file")
        )

        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)
        assert viewpoint_status == ViewpointStatus.FAILED
        assert error_message == (
            "Image Tile Server cannot process your S3 request! Error=An error occurred (404) "
            "when calling the download_file operation: Mock Error The no_bucket bucket does not exist!"
        )

    def test_private_download_s3_file_to_local_tmp_client_error_403(self):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")

        mock_s3.meta.client.download_file = MagicMock(
            side_effect=ClientError({"Error": {"Code": "403", "Message": "Mock Error"}}, "download_file")
        )

        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)
        assert viewpoint_status == ViewpointStatus.FAILED
        assert error_message == (
            "Image Tile Server cannot process your S3 request! Error=An error occurred (403) when calling the"
            " download_file operation: Mock Error You do not have permission to access no_bucket bucket!"
        )

    def test_private_download_s3_file_to_local_tmp_client_error_400(self):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")

        mock_s3.meta.client.download_file = MagicMock(
            side_effect=ClientError({"Error": {"Code": "400", "Message": "Mock Error"}}, "download_file")
        )

        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)
        assert viewpoint_status == ViewpointStatus.FAILED
        assert error_message == (
            "Image Tile Server cannot process your S3 request! Error=An error occurred (400) "
            "when calling the download_file operation: Mock Error"
        )

    def test_private_download_s3_file_to_local_tmp_other_exception(self):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")

        mock_s3.meta.client.download_file = MagicMock(side_effect=ValueError("Mock error"))

        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)
        assert viewpoint_status == ViewpointStatus.FAILED
        assert error_message == "Attempt 3/3: Something went wrong! Viewpoint_id: 1 | Error=Mock error"

    def test_private_download_supplementary_file_overview(self):
        from aws.osml.tile_server.viewpoint import SupplementaryFileType, ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        worker._download_supplementary_file(MOCK_VIEWPOINT_ITEM_2, SupplementaryFileType.OVERVIEW)

        mock_s3.meta.client.download_file.assert_called_with("no_bucket", "no_key.ovr", "/tmp/1/no_key.ovr")

    def test_private_download_supplementary_file_aux(self):
        from aws.osml.tile_server.viewpoint import SupplementaryFileType, ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        worker._download_supplementary_file(MOCK_VIEWPOINT_ITEM_2, SupplementaryFileType.AUX)

        mock_s3.meta.client.download_file.assert_called_with("no_bucket", "no_key.aux.xml", "/tmp/1/no_key.aux.xml")

    def test_private_download_supplementary_file_client_error(self):
        from aws.osml.tile_server.viewpoint import SupplementaryFileType, ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_s3.meta.client.download_file = MagicMock(
            side_effect=ClientError({"Error": {"Code": 500, "Message": "Mock Error"}}, "download_file")
        )
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)
        mock_logger = MagicMock()
        worker.logger = mock_logger

        worker._download_supplementary_file(MOCK_VIEWPOINT_ITEM_2, SupplementaryFileType.AUX)

        mock_s3.meta.client.download_file.assert_called_with("no_bucket", "no_key.aux.xml", "/tmp/1/no_key.aux.xml")
        mock_logger.info.assert_called_with("No aux file available for 1")

    @patch("aws.osml.tile_server.viewpoint.worker.get_tile_factory_pool")
    def test_get_default_tile_factory_pool_for_viewpoint_no_range_adjustment(self, mock_get_tile_factory_pool):
        from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        worker.get_default_tile_factory_pool_for_viewpoint(MOCK_VIEWPOINT_ITEM)

        mock_get_tile_factory_pool.assert_called_with(
            GDALImageFormats.PNG, GDALCompressionOptions.NONE, None, None, RangeAdjustmentType.NONE
        )

    @patch("aws.osml.tile_server.viewpoint.worker.get_tile_factory_pool")
    def test_get_default_tile_factory_pool_for_viewpoint_with_range_adjustment(self, mock_get_tile_factory_pool):
        from osgeo import gdalconst

        from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        worker.get_default_tile_factory_pool_for_viewpoint(MOCK_VIEWPOINT_ITEM_4)

        mock_get_tile_factory_pool.assert_called_with(
            GDALImageFormats.PNG, GDALCompressionOptions.NONE, None, gdalconst.GDT_Byte, RangeAdjustmentType.DRA
        )

    @patch("aws.osml.tile_server.viewpoint.worker.gdal")
    def test_private_calculate_image_statistics(self, mock_gdal):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        mock_gdal_open = MagicMock()
        mock_gdal_info = MagicMock()
        mock_gdal.Open = mock_gdal_open
        mock_gdal.Info = mock_gdal_info

        worker._calculate_image_statistics(MOCK_VIEWPOINT_ITEM_2)

        mock_gdal_open.assert_called_once_with("/tmp/1/no_key")
        mock_gdal_info.assert_called_once()

    @patch("aws.osml.tile_server.viewpoint.worker.get_standard_overviews")
    def test_private_create_image_pyramid(self, mock_get_overviews):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        mock_tile_factory = MagicMock()
        mock_ds = MagicMock()
        mock_build_overviews = MagicMock()
        mock_tile_factory.raster_dataset = mock_ds
        mock_ds.BuildOverviews = mock_build_overviews
        mock_get_overviews.return_value = "mock overviews"

        worker._create_image_pyramid(mock_tile_factory, MOCK_VIEWPOINT_ITEM_2)

        mock_build_overviews.assert_called_once_with("CUBIC", "mock overviews")

    def test_private_verify_tile_creation(self):
        mock_image_bytes = b"mock image bytes"
        mock_tile_factory = MagicMock()
        mock_tile_factory.create_encoded_tile.return_value = mock_image_bytes
        image_bytes = self.worker._verify_tile_creation(mock_tile_factory, MOCK_VIEWPOINT_ITEM)

        assert image_bytes == mock_image_bytes

    def test_private_write_metadata(self):
        mock_tile_factory = MagicMock()
        mock_tile_factory.raster_dataset.GetMetadata.return_value = {"data": "mock"}
        open_mock = mock_open()
        with patch("aws.osml.tile_server.viewpoint.worker.open", open_mock, create=True):
            self.worker._write_metadata(mock_tile_factory, MOCK_VIEWPOINT_ITEM_2)

        open_mock.assert_called_with("/tmp/1/no_key.metadata", "w")
        open_mock.return_value.write.assert_called_once_with('{"metadata": {"data": "mock"}}')

    def test_private_write_bounds(self):
        mock_tile_factory = MagicMock()
        mock_tile_factory.raster_dataset.RasterXSize = 512
        mock_tile_factory.raster_dataset.RasterYSize = 256
        open_mock = mock_open()
        with patch("aws.osml.tile_server.viewpoint.worker.open", open_mock, create=True):
            self.worker._write_bounds(mock_tile_factory, MOCK_VIEWPOINT_ITEM_2)

        open_mock.assert_called_with("/tmp/1/no_key.bounds", "w")
        open_mock.return_value.write.assert_called_once_with('{"bounds": [0, 0, 512, 256]}')

    def test_private_write_info(self):
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
    def test_private_write_statistics(self, mock_gdal):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        mock_gdal.Info.return_value = {"mock": "gdal info"}

        open_mock = mock_open()
        with patch("aws.osml.tile_server.viewpoint.worker.open", open_mock, create=True):
            worker._write_statistics(MOCK_VIEWPOINT_ITEM_2)

        open_mock.assert_called_with("/tmp/1/no_key.stats", "w")
        open_mock.return_value.write.assert_called_once_with('{"image_statistics": {"mock": "gdal info"}}')
