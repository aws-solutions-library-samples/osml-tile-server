#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import io
import logging
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from secrets import token_hex
from typing import Annotated, Any, Dict

from boto3.resources.base import ServiceResource
from fastapi import APIRouter, HTTPException, Query, Response
from osgeo import gdal, gdalconst
from starlette.responses import StreamingResponse

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType, load_gdal_dataset
from aws.osml.image_processing import GDALTileFactory
from aws.osml.photogrammetry.coordinates import ImageCoordinate
from aws.osml.tile_server.utils import get_media_type, get_tile_factory_pool, perform_gdal_translation

from .database import ViewpointStatusTable
from .models import ViewpointApiNames, ViewpointModel, ViewpointRequest, ViewpointStatus, ViewpointUpdate
from .queue import ViewpointRequestQueue


class ViewpointRouter:
    def __init__(
        self, viewpoint_database: ViewpointStatusTable, viewpoint_queue: ViewpointRequestQueue, aws_s3: ServiceResource
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
        def list_viewpoints(limit: int | None = None, next_token: str | None = None) -> Dict[str, Any]:
            """
            Get a list of viewpoints in the database

            :param limit: Optional. max number of viewpoints requested
            :param next_token: Optional. the token to begin a query from.  provided by the previous query response that
                had more records available
            :return: a list of viewpoints with details
            """
            return self.viewpoint_database.get_viewpoints(limit, next_token)

        @api_router.post("/", status_code=201)
        def create_viewpoint(viewpoint_request: ViewpointRequest) -> Dict[str, Any]:
            """
            Create a viewpoint item, then copy the imagery file from S3 to EFS, then create a item into the database

            :param viewpoint_request: client's request which contains name, file source, and range type
            TODO
                - utilize efs service

            :return: success creation of viewpoint
            """
            # Create unique viewpoint_id
            viewpoint_id = token_hex(16)

            # these will be stored in ddb
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
            Remove the file from the EFS and update the database to indicate that it has been deleted

            :param viewpoint_id: unique viewpoint id

            :return ViewpointModel: updated viewpoint item
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.UPDATE)

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
            Update the viewpoint item in DynamoDB based on the given viewpoint_id

            :param viewpoint_request: client's request which contains name, file source, and range type

            :return: updated viewpoint details
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_request.viewpoint_id)

            self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.UPDATE)

            viewpoint_item.viewpoint_name = viewpoint_request.viewpoint_name
            viewpoint_item.tile_size = viewpoint_request.tile_size
            viewpoint_item.range_adjustment = viewpoint_request.range_adjustment

            return self.viewpoint_database.update_viewpoint(viewpoint_item)

        @api_router.get("/{viewpoint_id}")
        def describe_viewpoint(viewpoint_id: str) -> ViewpointModel:
            """
            Get viewpoint details based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return ViewpointModel: viewpoint detail
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
            return viewpoint_item

        @api_router.get("/{viewpoint_id}/metadata")
        def get_metadata(viewpoint_id: str) -> Dict[str, Any]:
            """
            Get viewpoint metadata based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return Dict[str, Any]: viewpoint metadata
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.METADATA)

            viewpoint_path = viewpoint_item.local_object_path

            ds, sm = load_gdal_dataset(viewpoint_path)
            metadata = ds.GetMetadata()

            if not metadata:
                raise HTTPException(status_code=200, detail="The metadata is empty!")

            return {"metadata": metadata}

        @api_router.get("/{viewpoint_id}/bounds")
        def get_bounds(viewpoint_id: str) -> Dict[str, Any]:
            """
            Get viewpoint bounds based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return Dict[str, Any]: viewpoint bounds
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.BOUNDS)

            viewpoint_path = viewpoint_item.local_object_path

            ds, sm = load_gdal_dataset(viewpoint_path)
            width = ds.RasterXSize
            height = ds.RasterYSize

            world_coordinates = []
            for coordinates in [[0, 0], [0, height], [width, height], [width / 2, height / 2]]:
                world_coordinates.append(sm.image_to_world(ImageCoordinate(coordinates)).to_dms_string())

            return {"bounds": world_coordinates}

        @api_router.get("/{viewpoint_id}/info")
        def get_info(viewpoint_id: str) -> Dict[str, Any]:
            """
            Get viewpoint info based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return Dict[str, Any]: viewpoint info
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.INFO)

            viewpoint_path = viewpoint_item.local_object_path

            ds, sm = load_gdal_dataset(viewpoint_path)
            width = ds.RasterXSize
            height = ds.RasterYSize

            world_coordinates = []
            for coordinates in [[0, 0], [0, height], [width, height], [width / 2, height / 2]]:
                world_coordinates.append(sm.image_to_world(ImageCoordinate(coordinates)).to_dms_string())

            # TODO get geoJson feature that has a polygon geometry using the world coordinates above

            return {"features": None}

        @api_router.get("/{viewpoint_id}/statistics")
        def get_statistics(viewpoint_id: str) -> Dict[str, Any]:
            """
            Get viewpoint statistics based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return Dict[str, Any]: viewpoint statistics
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.STATISTICS)

            viewpoint_path = viewpoint_item.local_object_path

            try:
                gdal_options = gdal.InfoOptions(format="json", showMetadata=False)
                gdal_info = gdal.Info(viewpoint_path, options=gdal_options)
            except Exception as err:
                raise HTTPException(status_code=400, detail=f"Failed to fetch statistics of an image. {err}")

            return {"image_statistics": gdal_info}

        @api_router.get("/{viewpoint_id}/preview.{img_format}/")
        def get_preview(
            viewpoint_id: str,
            img_format: GDALImageFormats = Path(GDALImageFormats.PNG, description="Output image type."),
            scale: Annotated[int, Query(gt=0, le=100)] = None,
            x_px: int = 0,
            y_px: int = 0,
            compression: GDALCompressionOptions = Query(
                GDALCompressionOptions.NONE, description="Compression Algorithm for image."
            ),
        ) -> Response:
            """
            Get preview of viewpoint in the requested format

            :param viewpoint_id: Unique viewpoint id
            :param img_format: Desired format for preview output. Valid options are defined by GDALImageFormats
            :param scale: Preview scale in percentage or original image. (0-100%)
            :param x_px: Preview width (px).  Supercedes scale if > 0.
            :param y_px: Preview height (px).  Supercedes scale if > 0.
            :param compression: GDAL image compression

            :return: StreamingResponse of preview binary with the appropriate mime type based on the img_format
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
            self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.PREVIEW)

            ds, sensor_model = load_gdal_dataset(viewpoint_item.local_object_path)
            tile_factory = GDALTileFactory(
                ds, sensor_model, img_format, compression, gdalconst.GDT_Byte, viewpoint_item.range_adjustment
            )
            preview_options = tile_factory.default_gdal_translate_kwargs.copy()

            if x_px > 0 or y_px > 0:
                preview_options["width"] = x_px
                preview_options["height"] = y_px
            elif scale:
                preview_options["widthPct"] = scale
                preview_options["heightPct"] = scale

            preview_bytes = perform_gdal_translation(ds, preview_options)
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

            :param viewpoint_id: Unique viewpoint id
            :param z: r-level
            :param x: tile row (px)
            :param y: tile column(px)
            :param tile_format: Desired format for tile output. Valid options are defined by GDALImageFormats
            :param compression: GDAL tile compression

            :return: StreamingResponse of tile image binary
            """
            try:
                viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
                self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.TILE)

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
                    image_bytes = tile_factory.create_encoded_tile([x * tile_size, y * tile_size, tile_size, tile_size])

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
            \f
            Crop a portion of the viewpoint.

            :param viewpoint_id: Unique viewpoint id
            :param min_x: The left pixel coordinate of the desired crop.
            :param min_y: The upper pixel coordinate of the desired crop.
            :param max_x: The right pixel coordinate of the desired crop.
            :param max_y: The lower pixel coordinate of the pixel crop.
            :param img_format: Desired format for cropped output. Valid options are defined by GDALImageFormats.
            :param compression: GDAL compression algorithm for image.
            :param width: Optional. Width in px of the desired crop.  If provided, max_x will be ignored.
            :param height: Optional. Height in px of the desired crop.  If provided, max_y will be ignored.

            :return: StreamingResponse of cropped image binary with the appropriate mime type based on the img_format
            """

            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
            self.validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.PREVIEW)

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

    def validate_viewpoint_status(self, current_status: ViewpointStatus, api_operation: ViewpointApiNames) -> None:
        """
        This is a helper function which is to validate if we can execute an operation based on the
        given status

        :param current_status: current status of a viewpoint
        :param api_operation: api operation

        :return: viewpoint detail
        """
        if current_status == ViewpointStatus.DELETED:
            raise HTTPException(
                status_code=400,
                detail=f"Cannot view {api_operation} for this image since this has already been " f"deleted.",
            )
        elif current_status == ViewpointStatus.REQUESTED:
            raise HTTPException(
                status_code=400,
                detail="This viewpoint has been requested and not in READY state. Please try again " "later.",
            )
