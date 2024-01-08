import json
import logging
import time
import traceback
from datetime import UTC, datetime, timedelta
from enum import auto
from logging import Logger
from math import degrees
from pathlib import Path
from threading import Event, Thread
from typing import Tuple

import geojson
from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError
from osgeo import gdal, gdalconst

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType
from aws.osml.photogrammetry import ImageCoordinate
from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.utils import AutoLowerStringEnum, TileFactoryPool, get_standard_overviews, get_tile_factory_pool

from .common import (
    AUXXML_FILE_EXTENSION,
    BOUNDS_FILE_EXTENSION,
    INFO_FILE_EXTENSION,
    METADATA_FILE_EXTENSION,
    OVERVIEW_FILE_EXTENSION,
    STATISTICS_FILE_EXTENSION,
)
from .database import DecimalEncoder, ViewpointStatusTable
from .models import ViewpointModel, ViewpointStatus
from .queue import ViewpointRequestQueue


class SupplementaryFileType(str, AutoLowerStringEnum):
    """
    Provides supplementary file types to download from S3.

    :cvar AUX: Aux file.
    :cvar OVERVIEW: Overview file.
    """

    AUX = auto()
    OVERVIEW = auto()


class ViewpointWorker(Thread):
    def __init__(
        self,
        viewpoint_request_queue: ViewpointRequestQueue,
        aws_s3: ServiceResource,
        viewpoint_database: ViewpointStatusTable,
        logger: Logger = logging.getLogger(__name__),
    ):
        """
        The `__init__` method of the `ViewpointWorker` class initializes a new instance of the `ViewpointWorker`.

        :param viewpoint_request_queue: `ViewpointRequestQueue` class representing the queue for viewpoint requests.
        :param aws_s3: An instance of the `ServiceResource` class representing the AWS S3 service.
        :param viewpoint_database: `ViewpointStatusTable` class representing the database for viewpoint status.
        :param logger: 'Logger' class representing the logger.  Defaults to the default python logger.

        :return: None
        """
        super().__init__()
        self.daemon = True
        self.viewpoint_request_queue = viewpoint_request_queue
        self.s3 = aws_s3
        self.viewpoint_database = viewpoint_database
        self.logger = logger
        self.stop_event = Event()

    def join(self, timeout: float | None = ...) -> None:
        """
        Join the ViewpointWorker threads together.

        :param timeout: The maximum number of seconds to wait for the thread to finish execution. If `None`, the method
            will block until the thread is finished. (Default is `None`)
        :return: None
        """
        self.logger.info("ViewpointWorker Background Thread Stopping.")
        self.stop_event.set()
        Thread.join(self, timeout)

    def run(self) -> None:
        """
        Monitors SQS queues for ViewpointRequest and be able to process it. First, it will
        pick up a message from ViewpointRequest SQS. Then, it will download an image from S3
        and save it to the local temp directory. Once that's completed, it will update the DynamoDB
        to reflect that this Viewpoint is READY to review. This function will run in the background.

        :return: None
        """
        self.logger.info("ViewpointWorker Background Thread Started.")
        while not self.stop_event.is_set():
            self.logger.debug("Scanning for SQS messages")
            try:
                messages = self.viewpoint_request_queue.queue.receive_messages(WaitTimeSeconds=5)
                for message in messages:
                    self._process_message(message)

            except ClientError as err:
                self.logger.error(f"[Worker Background Thread] {err} / {traceback.format_exc()}")
            except KeyError as err:
                self.logger.error(f"[Worker Background Thread] {err} / {traceback.format_exc()}")
            except Exception as err:
                self.logger.error(f"[Worker Background Thread] {err} / {traceback.format_exc()}")

    def download_image(self, viewpoint_item: ViewpointModel) -> None:
        """
        This method downloads an image file from an S3 bucket using the bucket_name and object_key attributes of a
        ViewpointModel instance. The downloaded image is saved under specific server configuration, and its path is
        stored. The method also attempts to download optional files, logging their unavailability. Note: The method
        assumes the existence of the following constants: OVERVIEW_FILE_EXTENSION and AUXXML_FILE_EXTENSION,
        which represent the file extensions for the optional overview and aux files respectively.

        :param viewpoint_item: Instance of ViewpointModel representing the viewpoint item to be downloaded.
        :return: None

        """
        viewpoint_item.local_object_path = self._create_local_tmp_directory(viewpoint_item)

        failed, error_message = self._download_s3_file_to_local_tmp(viewpoint_item)
        if isinstance(failed, ViewpointStatus):
            viewpoint_item.viewpoint_status = failed
            viewpoint_item.error_message = error_message
        else:
            self._download_supplementary_file(viewpoint_item, SupplementaryFileType.OVERVIEW)
            self._download_supplementary_file(viewpoint_item, SupplementaryFileType.AUX)

    def create_tile_pyramid(self, viewpoint_item: ViewpointModel):
        """
        Main tile pyramid creation function. It creates a tile pyramid for a specific viewpoint item
        by considering its range adjustment and local object path.

        :param: The viewpoint item object for which to create the tile pyramid.
        :return: None
        """
        try:
            tile_factory_pool = self.get_default_tile_factory_pool_for_viewpoint(viewpoint_item)

            aux_file_path = Path(viewpoint_item.local_object_path + AUXXML_FILE_EXTENSION)
            overview_file_path = Path(viewpoint_item.local_object_path + OVERVIEW_FILE_EXTENSION)

            if not self.stop_event.is_set() and not aux_file_path.is_file():
                self._calculate_image_statistics(viewpoint_item)

            start_time = time.perf_counter()
            with tile_factory_pool.checkout_in_context() as tile_factory:
                end_time = time.perf_counter()
                self.logger.info(
                    f"METRIC: TileFactory Create Time: {end_time - start_time} for {viewpoint_item.local_object_path}"
                )

                image_bytes = None
                if not self.stop_event.is_set() and not overview_file_path.is_file():
                    self._create_image_pyramid(tile_factory, viewpoint_item)
                    image_bytes = self._verify_tile_creation(tile_factory, viewpoint_item)

                if not image_bytes:
                    raise ValueError("Image is empty.")

        except Exception as err:
            error_message = f"Unable to create sample tile for viewpoint: {viewpoint_item.viewpoint_id}! Error={err}"
            self.logger.error(error_message)
            viewpoint_item.viewpoint_status = ViewpointStatus.FAILED
            viewpoint_item.error_message = error_message

    def extract_metadata(self, viewpoint_item: ViewpointModel) -> None:
        """
        Extracts the metadata from an image and creates various JSON files for the metadata, bounds, info, etc. so
        that they can be easily returned by the web API without need to reparse the information from the image itself.
        This reduces the pressure on the tile factory resource pool making those resources more available for
        operations that need to return pixels.

        :param: the description of the viewpoint item
        """

        try:
            tile_factory_pool = self.get_default_tile_factory_pool_for_viewpoint(viewpoint_item)
            with tile_factory_pool.checkout_in_context() as tile_factory:
                self._write_metadata(tile_factory, viewpoint_item)
                self._write_bounds(tile_factory, viewpoint_item)
                self._write_info(tile_factory, viewpoint_item)
                self._write_statistics(viewpoint_item)

        except Exception as err:
            error_message = f"Unable to extract metadata for viewpoint: {viewpoint_item.viewpoint_id}! Error={err}"
            self.logger.error(error_message)
            viewpoint_item.viewpoint_status = ViewpointStatus.FAILED
            viewpoint_item.error_message = error_message

            start_time = time.perf_counter()
            end_time = time.perf_counter()
            self.logger.info(
                f"METRIC: TileFactory Create Time: {end_time - start_time} for {viewpoint_item.local_object_path}"
            )

    def _process_message(self, message) -> None:
        self.logger.info(f"MESSAGE: {message.body}")
        message_attributes = json.loads(message.body)
        viewpoint_item = ViewpointModel.model_validate_json(json.dumps(message_attributes, cls=DecimalEncoder))

        if viewpoint_item.viewpoint_status != ViewpointStatus.REQUESTED:
            self.logger.error(
                f"Cannot process {viewpoint_item.viewpoint_id} due to the incorrect "
                f"Viewpoint Status {viewpoint_item.viewpoint_status}!"
            )
            return

        self.download_image(viewpoint_item)
        self.create_tile_pyramid(viewpoint_item)
        self.extract_metadata(viewpoint_item)

        self._update_status(viewpoint_item)

        # Remove message from the queue since it has been processed
        message.delete()

    def _update_status(self, viewpoint_item: ViewpointModel) -> None:
        """
        Update ddb table to reflect status change after processed by the worker.

        :param viewpoint_item: Item being processed by the worker.

        :return: None.
        """
        if viewpoint_item.viewpoint_status == ViewpointStatus.FAILED:
            time_now = datetime.now(UTC)
            expire_time = time_now + timedelta(days=1)
            viewpoint_item.expire_time = int(expire_time.timestamp())
        else:
            viewpoint_item.viewpoint_status = ViewpointStatus.READY

        self.viewpoint_database.update_viewpoint(viewpoint_item)

    def _create_local_tmp_directory(self, viewpoint_item: ViewpointModel) -> str:
        """
        Create a tmp directory locally to store the downloaded files

        :param viewpoint_item: Item being processed by the worker.

        :return: The path to the temp directory.
        """
        message_viewpoint_id = viewpoint_item.viewpoint_id
        message_object_key = viewpoint_item.object_key

        self.logger.info(f"Creating local directory for {message_viewpoint_id} in /{ServerConfig.efs_mount_name}")
        local_viewpoint_folder = Path("/" + ServerConfig.efs_mount_name, message_viewpoint_id)
        local_viewpoint_folder.mkdir(parents=True, exist_ok=True)
        local_object_path = Path(local_viewpoint_folder, Path(message_object_key).name)
        return str(local_object_path.absolute())

    def _download_s3_file_to_local_tmp(
        self, viewpoint_item: ViewpointModel, max_retries: int = 3
    ) -> Tuple[ViewpointStatus | None, str | None]:
        """
        Download the object from S3 to the local tmp dorectory.

        :param viewpoint_item: Item being processed by the worker.

        :param max_retries: The number of times to retry the download if a non-fatal error is encountered.

        :return: The viewpoint status and error message as a tuple. Returns None, None if the operation was successful.
        """
        message_viewpoint_id = viewpoint_item.viewpoint_id
        message_object_key = viewpoint_item.object_key
        message_bucket_name = viewpoint_item.bucket_name
        local_object_path_str = viewpoint_item.local_object_path

        viewpoint_status = None
        error_message = None

        retry_count = 0
        while retry_count < max_retries:
            try:
                self.logger.info(f"Beginning download of {message_viewpoint_id}")
                self.s3.meta.client.download_file(message_bucket_name, message_object_key, local_object_path_str)
                self.logger.info(f"Successfully download to {local_object_path_str}.")
                viewpoint_status = None
                error_message = None
                break

            except ClientError as err:
                detailed_error = ""
                if err.response["Error"]["Code"] == "404":
                    detailed_error = f"The {message_bucket_name} bucket does not exist!"
                    self.logger.error(detailed_error)

                elif err.response["Error"]["Code"] == "403":
                    detailed_error = f"You do not have permission to access {message_bucket_name} bucket!"
                    self.logger.error(detailed_error)

                error_message = f"Image Tile Server cannot process your S3 request! Error={err} {detailed_error}".strip()
                self.logger.error(error_message)
                viewpoint_status = ViewpointStatus.FAILED
                break

            except Exception as err:
                error_message = (
                    f"Attempt {retry_count + 1}/{max_retries}: Something went wrong!"
                    f" Viewpoint_id: {message_viewpoint_id} | Error={err}"
                )
                self.logger.error(error_message)
                viewpoint_status = ViewpointStatus.FAILED

            retry_count += 1
        return viewpoint_status, error_message

    def _download_supplementary_file(self, viewpoint_item: ViewpointModel, file_type: SupplementaryFileType) -> None:
        """
        Attempts to download associated supplementary file from S3, if present

        :param viewpoint_item: Item being processed by the worker.

        :param file_type: The type of supplementary file to download from S3.

        :return: None.
        """
        message_viewpoint_id = viewpoint_item.viewpoint_id
        message_object_key = viewpoint_item.object_key
        message_bucket_name = viewpoint_item.bucket_name
        local_object_path = viewpoint_item.local_object_path
        extension_lookup = {
            SupplementaryFileType.AUX: AUXXML_FILE_EXTENSION,
            SupplementaryFileType.OVERVIEW: OVERVIEW_FILE_EXTENSION,
        }
        try:
            self.logger.info(f"Attempting to download optional {file_type.value} file for {message_viewpoint_id}")
            self.s3.meta.client.download_file(
                message_bucket_name,
                message_object_key + extension_lookup[file_type],
                local_object_path + extension_lookup[file_type],
            )
            self.logger.info(
                f"Successfully downloaded {file_type.value} file to {local_object_path + extension_lookup[file_type]}."
            )
        except ClientError:
            self.logger.info(f"No {file_type.value} file available for {message_viewpoint_id}")

    @staticmethod
    def get_default_tile_factory_pool_for_viewpoint(viewpoint_item: ViewpointModel) -> TileFactoryPool:
        """
        Helper method that creates a default tile factory pool for the image. This resource pool will not necessarily
        be sufficient to build all tiles, but it is sufficient to build previews and handle metadata.

        :param viewpoint_item: the description of the viewpoint item
        :return: a default tile factory pool
        """
        tile_format = GDALImageFormats.PNG
        compression = GDALCompressionOptions.NONE
        output_type = None
        if viewpoint_item.range_adjustment is not RangeAdjustmentType.NONE:
            output_type = gdalconst.GDT_Byte
        tile_factory_pool = get_tile_factory_pool(
            tile_format,
            compression,
            viewpoint_item.local_object_path,
            output_type,
            viewpoint_item.range_adjustment,
        )
        return tile_factory_pool

    def _calculate_image_statistics(self, viewpoint_item: ViewpointModel):
        """NOTE: This code forces GDAL to and compute the statistics / histograms for each band. This can be a
        time-consuming operation, so we want to only do this once. GDAL will write those statistics into a
        .aux.xml file associated with the dataset, but it only appears to do so when the dataset object is
        cleaned up. So this code creates a temporary dataset, forces it to generate the statistics using the
        gdal.Info() command and then closes the dataset to ensure that the auxiliary file is created. This is
        somewhat of a hack but all future calls to gdal.Open (i.e. in the tile factory pool) should be able to
        read the .aux.xml file and skip the expensive work of generating the statistics."""
        self.logger.info(f"Calculating Image Statistics for {viewpoint_item.local_object_path}")
        start_time = time.perf_counter()
        temp_ds = gdal.Open(viewpoint_item.local_object_path)
        gdal.Info(temp_ds, stats=True, approxStats=True, computeMinMax=True, reportHistograms=True)
        del temp_ds
        end_time = time.perf_counter()
        self.logger.info(f"METRIC: GDAL Info Time: {end_time - start_time} for {viewpoint_item.local_object_path}")

    def _create_image_pyramid(self, tile_factory: TileFactoryPool, viewpoint_item: ViewpointModel) -> None:
        self.logger.info(f"Creating Image Pyramid for {viewpoint_item.local_object_path}")
        start_time = time.perf_counter()
        ds = tile_factory.raster_dataset
        overviews = get_standard_overviews(ds.RasterXSize, ds.RasterYSize, 1024)
        ds.BuildOverviews("CUBIC", overviews)
        end_time = time.perf_counter()
        self.logger.info(f"METRIC: BuildOverviews Time: {end_time - start_time}" f" for {viewpoint_item.local_object_path}")

    def _verify_tile_creation(self, tile_factory: TileFactoryPool, viewpoint_item: ViewpointModel) -> bytes:
        self.logger.info(f"Verifying tile creation for {viewpoint_item.local_object_path}.")
        start_time = time.perf_counter()
        tile_size = viewpoint_item.tile_size
        image_bytes = tile_factory.create_encoded_tile([0, 0, tile_size, tile_size])
        end_time = time.perf_counter()
        self.logger.info(f"METRIC: Sample TileCreate Time: {end_time - start_time} for {viewpoint_item.local_object_path}")
        return image_bytes

    @staticmethod
    def _write_metadata(tile_factory: TileFactoryPool, viewpoint_item: ViewpointModel) -> None:
        metadata = tile_factory.raster_dataset.GetMetadata()
        with open(viewpoint_item.local_object_path + METADATA_FILE_EXTENSION, "w") as md_file:
            md_file.write(json.dumps({"metadata": metadata}))

    @staticmethod
    def _write_bounds(tile_factory: TileFactoryPool, viewpoint_item: ViewpointModel) -> None:
        width = tile_factory.raster_dataset.RasterXSize
        height = tile_factory.raster_dataset.RasterYSize
        image_coordinates = [0, 0, width, height]
        with open(viewpoint_item.local_object_path + BOUNDS_FILE_EXTENSION, "w") as bounds_file:
            bounds_file.write(json.dumps({"bounds": image_coordinates}))

    @staticmethod
    def _write_info(tile_factory: TileFactoryPool, viewpoint_item: ViewpointModel) -> None:
        width = tile_factory.raster_dataset.RasterXSize
        height = tile_factory.raster_dataset.RasterYSize
        coordinates = []
        for corner in [(0, 0), (0, height), (width, height), (width, 0)]:
            world_coordinate = tile_factory.sensor_model.image_to_world(ImageCoordinate(corner))
            coordinates.append((degrees(world_coordinate.longitude), degrees(world_coordinate.latitude)))
        coordinates.append(coordinates[0])

        feature = geojson.Feature(
            id=viewpoint_item.viewpoint_name, geometry=geojson.geometry.Polygon([coordinates]), properties={}
        )
        feature_collection = geojson.FeatureCollection(features=[feature])
        with open(viewpoint_item.local_object_path + INFO_FILE_EXTENSION, "w") as info_file:
            info_file.write(geojson.dumps(feature_collection))

    @staticmethod
    def _write_statistics(viewpoint_item: ViewpointModel) -> None:
        gdal_options = gdal.InfoOptions(format="json", showMetadata=False)
        gdal_info = gdal.Info(viewpoint_item.local_object_path, options=gdal_options)
        with open(viewpoint_item.local_object_path + STATISTICS_FILE_EXTENSION, "w") as stats_file:
            stats_file.write(json.dumps({"image_statistics": gdal_info}))
