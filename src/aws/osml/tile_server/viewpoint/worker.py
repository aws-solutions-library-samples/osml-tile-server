import json
import logging
import threading
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError
from osgeo import gdal, gdalconst

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType
from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.utils import get_standard_overviews, get_tile_factory_pool

from .database import DecimalEncoder, ViewpointStatusTable
from .models import ViewpointModel, ViewpointStatus
from .queue import ViewpointRequestQueue

OVERVIEW_FILE_EXTENSION = ".ovr"
AUXXML_FILE_EXTENSION = ".aux.xml"


class ViewpointWorker(threading.Thread):
    def __init__(
        self,
        viewpoint_request_queue: ViewpointRequestQueue,
        aws_s3: ServiceResource,
        viewpoint_database: ViewpointStatusTable,
    ):
        """
        The `__init__` method of the `ViewpointWorker` class initializes a new instance of the `ViewpointWorker`.

        :param viewpoint_request_queue: `ViewpointRequestQueue` class representing the queue for viewpoint requests.
        :param aws_s3: An instance of the `ServiceResource` class representing the AWS S3 service.
        :param viewpoint_database: `ViewpointStatusTable` class representing the database for viewpoint status.

        :return: None
        """
        super().__init__()
        self.daemon = True
        self.viewpoint_request_queue = viewpoint_request_queue
        self.s3 = aws_s3
        self.viewpoint_database = viewpoint_database
        self.logger = logging.getLogger("uvicorn")
        self.stop_event = threading.Event()

    def join(self, timeout: float | None = ...) -> None:
        """
        Join the ViewpointWorker threads together.

        :param timeout: The maximum number of seconds to wait for the thread to finish execution. If `None`, the method
            will block until the thread is finished. (Default is `None`)
        :return: None
        """
        self.logger.info("ViewpointWorker Background Thread Stopping.")
        self.stop_event.set()
        threading.Thread.join(self, timeout)

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
                    self.logger.info(f"MESSAGE: {message.body}")
                    message_attributes = json.loads(message.body)
                    viewpoint_item = ViewpointModel.model_validate_json(json.dumps(message_attributes, cls=DecimalEncoder))

                    if viewpoint_item.viewpoint_status != ViewpointStatus.REQUESTED:
                        self.logger.error(
                            f"Cannot process {viewpoint_item.viewpoint_id} due to the incorrect "
                            f"Viewpoint Status {viewpoint_item.viewpoint_status}!"
                        )
                        continue

                    if viewpoint_item.viewpoint_status != ViewpointStatus.FAILED:
                        self.download_image(viewpoint_item)

                    if viewpoint_item.viewpoint_status != ViewpointStatus.FAILED:
                        self.create_tile_pyramid(viewpoint_item)

                    # Update ddb table to reflect status change
                    if viewpoint_item.viewpoint_status == ViewpointStatus.FAILED:
                        time_now = datetime.utcnow()
                        expire_time = time_now + timedelta(1)
                        viewpoint_item.expire_time = int(expire_time.timestamp())
                    else:
                        viewpoint_item.viewpoint_status = ViewpointStatus.READY

                    self.viewpoint_database.update_viewpoint(viewpoint_item)

                    # Remove message from the queue since it has been processed
                    message.delete()

            except ClientError as err:
                self.logger.error(f"[Worker Background Thread] {err} / {traceback.format_exc()}")
            except KeyError as err:
                self.logger.error(f"[Worker Background Thread] {err} / {traceback.format_exc()}")
            except Exception as err:
                self.logger.error(f"[Worker Background Thread] {err} / {traceback.format_exc()}")

    def download_image(self, viewpoint_item: ViewpointModel):
        """
        This method downloads an image file from an S3 bucket using the bucket_name and object_key attributes of a
        ViewpointModel instance. The downloaded image is saved under specific server configuration, and its path is
        stored. The method also attempts to download optional files, logging their unavailability. Note: The method
        assumes the existence of the following constants: OVERVIEW_FILE_EXTENSION and AUXXML_FILE_EXTENSION,
        which represent the file extensions for the optional overview and aux files respectively.

        :param viewpoint_item: Instance of ViewpointModel representing the viewpoint item to be downloaded.
        :return: None

        """
        message_viewpoint_id = viewpoint_item.viewpoint_id
        message_object_key = viewpoint_item.object_key
        message_bucket_name = viewpoint_item.bucket_name

        # Create a temp file
        self.logger.info(f"Creating local directory for {message_viewpoint_id} in /{ServerConfig.efs_mount_name}")
        local_viewpoint_folder = Path("/" + ServerConfig.efs_mount_name, message_viewpoint_id)
        local_viewpoint_folder.mkdir(parents=True, exist_ok=True)
        local_object_path = Path(local_viewpoint_folder, Path(message_object_key).name)
        local_object_path_str = str(local_object_path.absolute())
        viewpoint_item.local_object_path = local_object_path_str

        # Download file to temp local (TODO update when efs is implemented)
        retry_count = 0
        while retry_count < 3:
            try:
                self.logger.info(f"Beginning download of {message_viewpoint_id}")
                self.s3.meta.client.download_file(message_bucket_name, message_object_key, local_object_path_str)
                self.logger.info(f"Successfully download to {local_object_path_str}.")
                break

            except ClientError as err:
                if err.response["Error"]["Code"] == "404":
                    error_message = f"The {message_bucket_name} bucket does not exist! Error={err}"
                    self.logger.error(error_message)

                elif err.response["Error"]["Code"] == "403":
                    error_message = f"You do not have permission to access {message_bucket_name} bucket! Error={err}"
                    self.logger.error(error_message)

                error_message = f"Image Tile Server cannot process your S3 request! Error={err}"
                self.logger.error(error_message)
                viewpoint_item.viewpoint_status = ViewpointStatus.FAILED
                viewpoint_item.error_message = error_message
                break
            except Exception as err:
                error_message = f"Something went wrong! Viewpoint_id: {message_viewpoint_id}! Error={err}"
                self.logger.error(error_message)
                viewpoint_item.viewpoint_status = ViewpointStatus.FAILED
                viewpoint_item.error_message = error_message

            retry_count += 1

        if viewpoint_item.viewpoint_status != ViewpointStatus.FAILED:
            try:
                self.logger.info(f"Attempting to download optional overviews for {message_viewpoint_id}")
                self.s3.meta.client.download_file(
                    message_bucket_name,
                    message_object_key + OVERVIEW_FILE_EXTENSION,
                    local_object_path_str + OVERVIEW_FILE_EXTENSION,
                )
                self.logger.info(f"Successfully downloaded overviews to {local_object_path_str + OVERVIEW_FILE_EXTENSION}.")
            except ClientError:
                self.logger.info(f"No overviews available for {message_viewpoint_id}")

            try:
                self.logger.info(f"Attempting to download optional aux file for {message_viewpoint_id}")
                self.s3.meta.client.download_file(
                    message_bucket_name,
                    message_object_key + AUXXML_FILE_EXTENSION,
                    local_object_path_str + AUXXML_FILE_EXTENSION,
                )
                self.logger.info(f"Successfully download aux file to {local_object_path_str + AUXXML_FILE_EXTENSION}.")
            except ClientError:
                self.logger.info(f"No aux file available for {message_viewpoint_id}")

    def create_tile_pyramid(self, viewpoint_item: ViewpointModel):
        """
        Main tile pyramid creation function. It creates a tile pyramid for a specific viewpoint item
        by considering its range adjustment and local object path.

        :param: The viewpoint item object for which to create the tile pyramid.
        :return: None
        """
        try:
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

            aux_file_path = Path(viewpoint_item.local_object_path + AUXXML_FILE_EXTENSION)
            overview_file_path = Path(viewpoint_item.local_object_path + OVERVIEW_FILE_EXTENSION)

            """NOTE: This code forces GDAL to and compute the statistics / histograms for each band. This can be a
            time-consuming operation, so we want to only do this once. GDAL will write those statistics into a
            .aux.xml file associated with the dataset, but it only appears to do so when the dataset object is
            cleaned up. So this code creates a temporary dataset, forces it to generate the statistics using the
            gdal.Info() command and then closes the dataset to ensure that the auxiliary file is created. This is
            somewhat of a hack but all future calls to gdal.Open (i.e. in the tile factory pool) should be able to
            read the .aux.xml file and skip the expensive work of generating the statistics."""
            if not self.stop_event.is_set() and not aux_file_path.is_file():
                self.logger.info(f"Calculating Image Statistics for {viewpoint_item.local_object_path}")
                start_time = time.perf_counter()
                temp_ds = gdal.Open(viewpoint_item.local_object_path)
                gdal.Info(temp_ds, stats=True, approxStats=True, computeMinMax=True, reportHistograms=True)
                del temp_ds
                end_time = time.perf_counter()
                self.logger.info(
                    f"METRIC: GDAL Info Time: {end_time - start_time}" f" for {viewpoint_item.local_object_path}"
                )

            start_time = time.perf_counter()
            with tile_factory_pool.checkout_in_context() as tile_factory:
                end_time = time.perf_counter()
                self.logger.info(
                    f"METRIC: TileFactory Create Time: {end_time - start_time}" f" for {viewpoint_item.local_object_path}"
                )

                if not self.stop_event.is_set() and not overview_file_path.is_file():
                    self.logger.info(f"Creating Image Pyramid for {viewpoint_item.local_object_path}")
                    start_time = time.perf_counter()
                    ds = tile_factory.raster_dataset
                    overviews = get_standard_overviews(ds.RasterXSize, ds.RasterYSize, 1024)
                    ds.BuildOverviews("CUBIC", overviews)
                    end_time = time.perf_counter()
                    self.logger.info(
                        f"METRIC: BuildOverviews Time: {end_time - start_time}" f" for {viewpoint_item.local_object_path}"
                    )

                self.logger.info(f"Verifying tile creation for {viewpoint_item.local_object_path}.")
                start_time = time.perf_counter()
                tile_size = viewpoint_item.tile_size
                image_bytes = tile_factory.create_encoded_tile([0, 0, tile_size, tile_size])
                end_time = time.perf_counter()
                self.logger.info(
                    f"METRIC: Sample TileCreate Time: {end_time - start_time}" f" for {viewpoint_item.local_object_path}"
                )

                if not image_bytes:
                    error_message = f"Unable to create valid tile for viewpoint: {viewpoint_item.viewpoint_id}"
                    self.logger.error(error_message)
                    viewpoint_item.viewpoint_status = ViewpointStatus.FAILED
                    viewpoint_item.error_message = error_message

        except Exception as err:
            error_message = f"Unable to create sample tile for! Viewpoint_id: {viewpoint_item.viewpoint_id}! Error={err}"
            self.logger.error(error_message)
            viewpoint_item.viewpoint_status = ViewpointStatus.FAILED
            viewpoint_item.error_message = error_message
