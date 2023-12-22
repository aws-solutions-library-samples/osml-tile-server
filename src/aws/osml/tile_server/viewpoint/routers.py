#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import inspect
import io
import logging
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from secrets import token_hex
from typing import Any, Dict

import dateutil.parser
from boto3.resources.base import ServiceResource
from cryptography.fernet import Fernet
from fastapi import APIRouter, HTTPException, Query, Response
from fastapi_versioning import version
from osgeo import gdalconst
from starlette.responses import FileResponse, StreamingResponse

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType
from aws.osml.tile_server.utils import get_media_type, get_tile_factory_pool, perform_gdal_translation

from .common import BOUNDS_FILE_EXTENSION, INFO_FILE_EXTENSION, METADATA_FILE_EXTENSION, STATISTICS_FILE_EXTENSION
from .database import ViewpointStatusTable
from .models import (
    ViewpointApiNames,
    ViewpointListResponse,
    ViewpointModel,
    ViewpointRequest,
    ViewpointStatus,
    ViewpointUpdate,
)
from .queue import ViewpointRequestQueue


class ViewpointRouter:
    """
    A class used to represent the ViewpointRouter.
    """

    def __init__(
        self,
        viewpoint_database: ViewpointStatusTable,
        viewpoint_queue: ViewpointRequestQueue,
        aws_s3: ServiceResource,
        encryptor: Fernet,
    ) -> None:
        """
        The `ViewpointRouter` class is responsible for handling API endpoints related to viewpoints.

        :param viewpoint_database: Instance of the ViewpointStatusTable class representing the viewpoint database.

        :param viewpoint_queue: Instance of the ViewpointRequestQueue class representing the viewpoint request queue.

        :param aws_s3: Instance of the ServiceResource class representing the AWS S3 service.

        :return: None
        """
        self.viewpoint_database = viewpoint_database
        self.viewpoint_queue = viewpoint_queue
        self.s3 = aws_s3
        self.encryptor = encryptor
        self.logger = logging.getLogger("uvicorn")

    @property
    def router(self):
        """
        Initializes a new instance of the ViewpointRouter class.

        :return: None
        """
        api_router = APIRouter(
            prefix="/viewpoints",
            tags=["viewpoints"],
            dependencies=[],
            responses={404: {"description": "Not found!"}},
        )

        @api_router.get("/")
        @version(1, 0)
        def list_viewpoints(max_results: int | None = None, next_token: str | None = None) -> ViewpointListResponse:
            """
            Get a list of viewpoints in the database.

            :param max_results: Optional max number of viewpoints requested

            :param next_token: Optional token to begin a query from provided by the previous query response.

            :return: List of viewpoints with details from the table.
            """
            current_function_name = inspect.stack()[0].function
            current_route = [route for route in api_router.routes if route.name == current_function_name][0]
            current_endpoint = current_route.endpoint
            endpoint_major_version, endpoint_minor_version = getattr(current_endpoint, "_api_version", (0, 0))
            endpoint_version = f"{endpoint_major_version}.{endpoint_minor_version}"

            if next_token:
                try:
                    decrypted_token = self.encryptor.decrypt(next_token.encode("utf-8")).decode("utf-8")
                    decrypted_next_token, expiration_iso, decrypted_endpoint_version = decrypted_token.split("|")
                    expiration_dt = dateutil.parser.isoparse(expiration_iso)
                    now = datetime.now(UTC)
                except Exception as err:
                    raise HTTPException(status_code=400, detail=f"Invalid next_token. {err}")
                if expiration_dt < now:
                    raise HTTPException(status_code=400, detail="next_token expired. Please submit a new request.")
                if decrypted_endpoint_version != endpoint_version:
                    raise HTTPException(
                        status_code=400,
                        detail="next_token is not compatible with this endpoint version. Please submit a new request.",
                    )
            else:
                decrypted_next_token = None

            results = self.viewpoint_database.get_viewpoints(max_results, decrypted_next_token)
            if results.next_token:
                expiration = datetime.now(UTC) + timedelta(days=1)
                token_string = f"{results.next_token}|{expiration.isoformat()}|{endpoint_version}"
                encrypted_token = self.encryptor.encrypt(token_string.encode("utf-8")).decode("utf-8")
                results.next_token = encrypted_token
            return results

        @api_router.post("/", status_code=201)
        def create_viewpoint(viewpoint_request: ViewpointRequest) -> Dict[str, Any]:
            """
            Create a viewpoint item, then copy the imagery file from S3 to EFS, then create an item into the database.

            :param viewpoint_request: client's request which contains name, file source, and range type.

            :return: Status associated with the request to create the viewpoint in the table.
            """
            # Create unique viewpoint_id
            viewpoint_id = token_hex(16)

            # These will be stored in ddb
            new_viewpoint_request = ViewpointModel(
                viewpoint_id=viewpoint_id,
                viewpoint_name=viewpoint_request.viewpoint_name,
                bucket_name=viewpoint_request.bucket_name,
                object_key=viewpoint_request.object_key,
                tile_size=viewpoint_request.tile_size,
                range_adjustment=viewpoint_request.range_adjustment,
                viewpoint_status=ViewpointStatus.REQUESTED,
                local_object_path=None,
                error_message=None,
                expire_time=None,
            )

            # Place this request into SQS, then the worker will pick up in order to
            # download the image from S3 to Local (TODO rename to EFS once implemented)
            self.viewpoint_queue.send_request(new_viewpoint_request.model_dump())

            return self.viewpoint_database.create_viewpoint(new_viewpoint_request)

        @api_router.delete("/{viewpoint_id}")
        def delete_viewpoint(viewpoint_id: str) -> ViewpointModel:
            """
            Remove the file from the EFS and update the database to indicate that it has been deleted.

            :param viewpoint_id: Unique viewpoint id to get from the table.

            :return ViewpointModel: Updated viewpoint item details from the table.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.UPDATE)

            if viewpoint_item:
                shutil.rmtree(Path(viewpoint_item.local_object_path).parent, ignore_errors=True)

            viewpoint_item.viewpoint_status = ViewpointStatus.DELETED
            viewpoint_item.local_object_path = None
            time_now = datetime.utcnow()
            expire_time = time_now + timedelta(1)
            viewpoint_item.expire_time = int(expire_time.timestamp())

            return self.viewpoint_database.update_viewpoint(viewpoint_item)

        @api_router.put("/", status_code=201)
        def update_viewpoint(viewpoint_request: ViewpointUpdate) -> ViewpointModel:
            """
            Update the viewpoint item in DynamoDB based on the given viewpoint_id.

            :param viewpoint_request: Client's request, which contains name, file source, and range type.

            :return: Updated viewpoint item details from the table.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_request.viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.UPDATE)

            viewpoint_item.viewpoint_name = viewpoint_request.viewpoint_name
            viewpoint_item.tile_size = viewpoint_request.tile_size
            viewpoint_item.range_adjustment = viewpoint_request.range_adjustment

            return self.viewpoint_database.update_viewpoint(viewpoint_item)

        @api_router.get("/{viewpoint_id}")
        def describe_viewpoint(viewpoint_id: str) -> ViewpointModel:
            """
            Get viewpoint details based on provided viewpoint id.

            :param viewpoint_id: Unique viewpoint id to get from the table.

            :return: Details from the viewpoint item in the table.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
            return viewpoint_item

        @api_router.get("/{viewpoint_id}/metadata")
        def get_metadata(viewpoint_id: str) -> FileResponse:
            """
            Get viewpoint metadata based on provided viewpoint id.

            :param viewpoint_id: Unique viewpoint id to get from the table.

            :return: Viewpoint metadata associated with the viewpoint item from the table.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.METADATA)

            return FileResponse(viewpoint_item.local_object_path + METADATA_FILE_EXTENSION, media_type="application/json")

        @api_router.get("/{viewpoint_id}/bounds")
        def get_bounds(viewpoint_id: str) -> FileResponse:
            """
            Get viewpoint bounds based on provided viewpoint id.

            :param viewpoint_id: Unique viewpoint id to get from the table.

            :return: Viewpoint bounds for the given table item.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.BOUNDS)

            return FileResponse(viewpoint_item.local_object_path + BOUNDS_FILE_EXTENSION, media_type="application/json")

        @api_router.get("/{viewpoint_id}/info")
        def get_info(viewpoint_id: str) -> FileResponse:
            """
            Get viewpoint info based on provided viewpoint id.

            :param viewpoint_id: Unique viewpoint id to get from the table.

            :return: Viewpoint info associated with the given id.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.INFO)

            return FileResponse(viewpoint_item.local_object_path + INFO_FILE_EXTENSION, media_type="application/json")

        @api_router.get("/{viewpoint_id}/statistics")
        def get_statistics(viewpoint_id: str) -> FileResponse:
            """
            Get viewpoint statistics based on provided viewpoint id.

            :param viewpoint_id: Unique viewpoint id to get from the table.

            :return: Viewpoint statistics associated with the id.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.STATISTICS)

            return FileResponse(viewpoint_item.local_object_path + STATISTICS_FILE_EXTENSION, media_type="application/json")

        @api_router.get("/{viewpoint_id}/preview.{img_format}")
        def get_preview(
            viewpoint_id: str,
            img_format: GDALImageFormats = Path(GDALImageFormats.PNG, description="Output image type."),
            max_size: int = 1024,
            width: int = 0,
            height: int = 0,
            compression: GDALCompressionOptions = Query(
                GDALCompressionOptions.NONE, description="Compression Algorithm for image."
            ),
        ) -> Response:
            """
            Get preview of viewpoint in the requested format

            :param viewpoint_id: Unique viewpoint id to get from the table.

            :param img_format: The Desired format for preview output, valid options are defined by GDALImageFormats.

            :param max_size: Max size of the preview image, defaults to 1024 pixels.

            :param width: Preview width in pixels that supersedes scale if > 0.

            :param height: Preview height in pixels that supersedes scale if > 0.

            :param compression: GDAL image compression format to use.

            :return: StreamingResponse of preview binary with the appropriate mime type based on the img_format
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.PREVIEW)

            output_type = None
            if viewpoint_item.range_adjustment is not RangeAdjustmentType.NONE:
                output_type = gdalconst.GDT_Byte

            tile_factory_pool = get_tile_factory_pool(
                img_format, compression, viewpoint_item.local_object_path, output_type, viewpoint_item.range_adjustment
            )
            with tile_factory_pool.checkout_in_context() as tile_factory:
                preview_options = tile_factory.default_gdal_translate_kwargs.copy()

                if tile_factory.raster_dataset.RasterXSize >= tile_factory.raster_dataset.RasterYSize:
                    preview_options["width"] = max_size
                else:
                    preview_options["height"] = max_size

                if width > 0:
                    preview_options["width"] = width
                if height > 0:
                    preview_options["height"] = height

                preview_bytes = perform_gdal_translation(tile_factory.raster_dataset, preview_options)
                return StreamingResponse(io.BytesIO(preview_bytes), media_type=get_media_type(img_format), status_code=200)

        @api_router.get("/{viewpoint_id}/tiles/{z}/{x}/{y}.{tile_format}")
        def get_tile(
            viewpoint_id: str,
            z: int,
            x: int,
            y: int,
            tile_format: GDALImageFormats = Path(GDALImageFormats.PNG, description="Output image type. Defaults to PNG."),
            compression: GDALCompressionOptions = Query(
                GDALCompressionOptions.NONE, description="Compression Algorithm for image."
            ),
        ) -> Response:
            """

            :param viewpoint_id: Unique viewpoint id to get from the table.

            :param z: Resolution-level in the image pyramid 0 = full resolution, 1 = full/2, 2 = full/4, ...

            :param x: Tile row location in pixels for the given tile.

            :param y: Tile column location in pixels for the given tile.

            :param tile_format: Desired format for tile output, valid options are defined by GDALImageFormats.

            :param compression: GDAL tile compression format.

            :return: StreamingResponse of tile image binary payload.
            """
            if z < 0:
                raise HTTPException(
                    status_code=400, detail=f"Resolution Level for get tile request must be >= 0. Requested z={z}"
                )

            try:
                viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
                self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.TILE)

                output_type = None
                if viewpoint_item.range_adjustment is not RangeAdjustmentType.NONE:
                    output_type = gdalconst.GDT_Byte

                tile_factory_pool = get_tile_factory_pool(
                    tile_format, compression, viewpoint_item.local_object_path, output_type, viewpoint_item.range_adjustment
                )
                with tile_factory_pool.checkout_in_context() as tile_factory:
                    if tile_factory is None:
                        raise HTTPException(
                            status_code=500, detail=f"Unable to read tiles from viewpoint {viewpoint_item.viewpoint_id}"
                        )

                    tile_size = viewpoint_item.tile_size
                    src_tile_size = 2**z * tile_size
                    image_bytes = tile_factory.create_encoded_tile(
                        src_window=[x * src_tile_size, y * src_tile_size, src_tile_size, src_tile_size],
                        output_size=(tile_size, tile_size),
                    )

                return StreamingResponse(io.BytesIO(image_bytes), media_type=get_media_type(tile_format), status_code=200)
            except Exception as err:
                raise HTTPException(status_code=500, detail=f"Failed to fetch tile for image. {err}")

        @api_router.get("/{viewpoint_id}/crop/{min_x},{min_y},{max_x},{max_y}.{img_format}")
        def get_crop(
            viewpoint_id: str,
            min_x: int = Path(description="Unique viewpoint id"),
            min_y: int = Path(description="The left pixel coordinate of the desired crop."),
            max_x: int = Path(description="The right pixel coordinate of the desired crop."),
            max_y: int = Path(description="The lower pixel coordinate of the pixel crop."),
            img_format: GDALImageFormats = Path(
                default=GDALImageFormats.PNG,
                description="Desired format for cropped output. Valid options are defined by GDALImageFormats.",
            ),
            compression: GDALCompressionOptions = Query(
                default=GDALCompressionOptions.NONE, description="GDAL compression algorithm for image."
            ),
            width: int = Query(
                default=None,
                description="Optional. Width in px of the desired crop.  If provided, max_x will be " "ignored.",
            ),
            height: int = Query(
                default=None,
                description="Optional. Height in px of the desired crop.  If provided, max_y will be " "ignored.",
            ),
        ) -> Response:
            """
            Crop a portion of the viewpoint.

            :param viewpoint_id: Unique viewpoint id to get from the table as a crop.

            :param min_x: The left pixel coordinate of the desired crop.

            :param min_y: The upper pixel coordinate of the desired crop.

            :param max_x: The right pixel coordinate of the desired crop.

            :param max_y: The lower pixel coordinate of the pixel crop.

            :param img_format: Desired format for cropped output. Valid options are defined by GDALImageFormats.

            :param compression: GDAL compression algorithm for image.

            :param width: Optional width in px of the desired crop, if provided, max_x will be ignored.

            :param height: Optional height in px of the desired crop, if provided, max_y will be ignored.

            :return: StreamingResponse of cropped image binary with the appropriate mime type based on the img_format
            """

            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.PREVIEW)

            tile_factory_pool = get_tile_factory_pool(
                img_format,
                compression,
                viewpoint_item.local_object_path,
                output_type=gdalconst.GDT_Byte,
                range_adjustment=viewpoint_item.range_adjustment,
            )
            with tile_factory_pool.checkout_in_context() as tile_factory:
                crop_width = width if width is not None else max_x - min_x
                crop_height = height if height is not None else max_y - min_y

                crop_bytes = tile_factory.create_encoded_tile([min_x, min_y, crop_width, crop_height])
                return StreamingResponse(io.BytesIO(crop_bytes), media_type=get_media_type(img_format), status_code=200)

        return api_router

    @staticmethod
    def _validate_viewpoint_status(current_status: ViewpointStatus, api_operation: ViewpointApiNames) -> None:
        """
        This is a helper function that is to validate if we can execute an operation based on the
        given status

        :param current_status: Current status of a viewpoint in the table.

        :param api_operation: The associated API operation being used on the viewpoint.

        :return: Viewpoint detail
        """
        if current_status == ViewpointStatus.DELETED:
            raise HTTPException(
                status_code=404,
                detail=f"Cannot view {api_operation} for this image since this has already been " f"deleted.",
            )
        elif current_status == ViewpointStatus.REQUESTED:
            raise HTTPException(
                status_code=400,
                detail="This viewpoint has been requested and not in READY state. Please try again " "later.",
            )
