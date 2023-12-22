#  Copyright 2023 Amazon.com, Inc. or its affiliates.
import copy
from datetime import UTC, datetime, timedelta
from threading import Event
from unittest import TestCase
from unittest.mock import MagicMock, patch

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


class TestViewpointWorker(TestCase):
    def test_viewpoint_worker(self):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        assert worker.daemon
        assert isinstance(worker.stop_event, Event)

    @pytest.mark.skip(reason="Test not implemented")
    def test_join(self):
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        worker.join()

    @pytest.mark.skip(reason="Test not implemented")
    def test_run(self):
        pass

    def test_download_image(self):
        from aws.osml.tile_server.viewpoint import SupplementaryFileType, ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)
        mock_create_local_tmp_directory = MagicMock(return_value="/tmp/1/no_key")
        mock_download_s3_file = MagicMock(return_value=(None, None))
        mock_supplemental_download = MagicMock()
        worker._create_local_tmp_directory = mock_create_local_tmp_directory
        worker._download_s3_file_to_local_tmp = mock_download_s3_file
        worker._download_supplementary_file = mock_supplemental_download
        worker.download_image(mock_viewpoint)
        mock_supplemental_download.assert_called_with(mock_viewpoint, SupplementaryFileType.AUX)

    def test_download_image_failed(self):
        from aws.osml.tile_server.viewpoint import ViewpointStatus, ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")
        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        mock_viewpoint = copy.deepcopy(MOCK_VIEWPOINT_ITEM)
        mock_create_local_tmp_directory = MagicMock(return_value="/tmp/1/no_key")
        mock_download_s3_file = MagicMock(return_value=(ViewpointStatus.FAILED, "Failed"))
        mock_supplemental_download = MagicMock()
        worker._create_local_tmp_directory = mock_create_local_tmp_directory
        worker._download_s3_file_to_local_tmp = mock_download_s3_file
        worker._download_supplementary_file = mock_supplemental_download
        worker.download_image(mock_viewpoint)
        assert mock_viewpoint.viewpoint_status == ViewpointStatus.FAILED
        assert mock_viewpoint.error_message == "Failed"
        mock_supplemental_download.assert_not_called()

    @pytest.mark.skip(reason="Test not implemented")
    def test_create_tile_pyramid(self):
        pass

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
        from aws.osml.tile_server.viewpoint import ViewpointWorker

        mock_queue = MagicMock(name="queue")
        mock_s3 = MagicMock(name="S3")
        mock_ddb = MagicMock(name="ddb")

        worker = ViewpointWorker(mock_queue, mock_s3, mock_ddb)

        viewpoint_status, error_message = worker._download_s3_file_to_local_tmp(MOCK_VIEWPOINT_ITEM)
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
