import io
import logging
import os
import shutil
from pathlib import Path
from secrets import token_hex
from typing import Annotated, Any, Dict, List

from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException, Query, Response
from osgeo import gdal, gdalconst
from starlette.responses import StreamingResponse

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType, load_gdal_dataset
from aws.osml.image_processing import GDALTileFactory
from aws.osml.photogrammetry.coordinates import ImageCoordinate
from aws.osml.tile_server.utils import get_media_type, get_tile_factory, perform_gdal_translation, validate_viewpoint_status
from aws.osml.tile_server.viewpoint.database import ViewpointStatusTable
from aws.osml.tile_server.viewpoint.models import (
    ViewpointApiNames,
    ViewpointModel,
    ViewpointRequest,
    ViewpointStatus,
    ViewpointUpdate,
)

FILESYSTEM_CACHE_ROOT = os.getenv("VIEWPOINT_FILESYSTEM_CACHE", "/tmp/viewpoint")


class ViewpointRouter:
    def __init__(self, viewpoint_database: ViewpointStatusTable, aws_s3: ServiceResource) -> None:
        self.viewpoint_database = viewpoint_database
        self.s3 = aws_s3
        self.logger = logging.getLogger("uvicorn")

    @property
    def router(self):
        api_router = APIRouter(
            prefix="/viewpoints",
            tags=["viewpoints"],
            dependencies=[],
            responses={404: {"description": "Not found!"}},
        )

        @api_router.get("/")
        def list_viewpoints() -> List[Dict[str, Any]]:
            """
            Get a list of viewpoints in the database

            :param viewpoint_status_table: Viewpoint Status Table

            :return: a list of viewpoints with details
            """
            return self.viewpoint_database.get_all_viewpoints()

        @api_router.post("/", status_code=201)
        async def create_viewpoint(viewpoint_request: ViewpointRequest) -> Dict[str, Any]:
            """
            Create a viewpoint item, then it copies the imagery file from S3 to EFS, then create a item into the database

            :param request: client's request which contains name, file source, and range type
            :param viewpoint_status_table: Viewpoint Status Table

            TODO
                - utilize efs service
                - utilize background tasks

            :return: success creation of viewpoint
            """
            # Create unique viewpoint_id
            viewpoint_id = token_hex(16)

            file_name = viewpoint_request.object_key.split("/")[-1]

            # TODO rename local to EFS (CDK changes required)
            local_viewpoint_folder = Path(FILESYSTEM_CACHE_ROOT, viewpoint_id)
            local_viewpoint_folder.mkdir(parents=True, exist_ok=True)
            local_object_path = Path(local_viewpoint_folder, file_name)

            try:
                self.s3.meta.client.download_file(
                    viewpoint_request.bucket_name, viewpoint_request.object_key, str(local_object_path.absolute())
                )
            except ClientError as err:
                if err.response["Error"]["Code"] == "404":
                    raise HTTPException(
                        status_code=404, detail=f"The {viewpoint_request.bucket_name} bucket does not exist! Error={err}"
                    )
                elif err.response["Error"]["Code"] == "403":
                    raise HTTPException(
                        status_code=403,
                        detail=f"You do not have permission to access {viewpoint_request.bucket_name} bucket! Error={err}",
                    )

                raise HTTPException(status_code=400, detail=f"Image Tile Server cannot process your S3 request! Error={err}")

            # these will be stored in ddb
            new_viewpoint_request = ViewpointModel(
                viewpoint_id=viewpoint_id,
                viewpoint_name=viewpoint_request.viewpoint_name,
                bucket_name=viewpoint_request.bucket_name,
                object_key=viewpoint_request.object_key,
                tile_size=viewpoint_request.tile_size,
                range_adjustment=viewpoint_request.range_adjustment,
                viewpoint_status=ViewpointStatus.REQUESTED,
                local_object_path=str(local_object_path.absolute()),
            )

            # TODO we need to integrate background task here and update the status to READY when its completed

            return self.viewpoint_database.create_viewpoint(new_viewpoint_request)

        @api_router.delete("/{viewpoint_id}")
        async def delete_viewpoint(viewpoint_id: str) -> ViewpointModel:
            """
            Remove the file from the EFS and update the database to indicate that it has been deleted

            :param viewpoint_id: unique viewpoint id

            :return ViewpointModel: updated viewpoint item
            """
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)

            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.UPDATE)

            if viewpoint_item:
                shutil.rmtree(Path(viewpoint_item.local_object_path).parent, ignore_errors=True)

            viewpoint_item.viewpoint_status = ViewpointStatus.DELETED
            viewpoint_item.local_object_path = None

            return self.viewpoint_database.update_viewpoint(viewpoint_item)

        @api_router.put("/", status_code=201)
        async def update_viewpoint(viewpoint_request: ViewpointUpdate) -> ViewpointModel:
            """
            Update the viewpoint item in DynamoDB based on the given viewpoint_id

            :param request: client's request which contains name, file source, and range type

            :return: updated viewpoint details
            """
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_request.viewpoint_id)

            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.UPDATE)

            viewpoint_item.viewpoint_name = viewpoint_request.viewpoint_name
            viewpoint_item.tile_size = viewpoint_request.tile_size
            viewpoint_item.range_adjustment = viewpoint_request.range_adjustment

            return self.viewpoint_database.update_viewpoint(viewpoint_item)

        @api_router.get("/{viewpoint_id}")
        async def describe_viewpoint(viewpoint_id: str) -> ViewpointModel:
            """
            Get viewpoint details based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return ViewpointModel: viewpoint detail
            """
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)
            return viewpoint_item

        @api_router.get("/{viewpoint_id}/metadata")
        async def get_metadata(viewpoint_id: str) -> Dict[str, Any]:
            """
            Get viewpoint metadata based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return Dict[str, Any]: viewpoint metadata
            """
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)

            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.METADATA)

            viewpoint_path = viewpoint_item.local_object_path

            ds, sm = load_gdal_dataset(viewpoint_path)
            metadata = ds.GetMetadata()

            if not metadata:
                raise HTTPException(status_code=200, detail="The metadata is empty!")

            return {"metadata": metadata}

        @api_router.get("/{viewpoint_id}/bounds")
        async def get_bounds(viewpoint_id: str) -> Dict[str, Any]:
            """
            Get viewpoint bounds based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return Dict[str, Any]: viewpoint bounds
            """
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)

            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.BOUNDS)

            viewpoint_path = viewpoint_item.local_object_path

            ds, sm = load_gdal_dataset(viewpoint_path)
            width = ds.RasterXSize
            height = ds.RasterYSize

            world_coordinates = []
            for coordinates in [[0, 0], [0, height], [width, height], [width / 2, height / 2]]:
                world_coordinates.append(sm.image_to_world(ImageCoordinate(coordinates)).to_dms_string())

            return {"bounds": world_coordinates}

        @api_router.get("/{viewpoint_id}/info")
        async def get_info(viewpoint_id: str) -> Dict[str, Any]:
            """
            Get viewpoint info based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return Dict[str, Any]: viewpoint info
            """
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)

            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.INFO)

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
        async def get_statistics(viewpoint_id: str) -> Dict[str, Any]:
            """
            Get viewpoint statistics based on provided viewpoint id

            :param viewpoint_id: Unique viewpoint id

            :return Dict[str, Any]: viewpoint statistics
            """
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)

            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.STATISTICS)

            viewpoint_path = viewpoint_item.local_object_path

            try:
                gdalOptions = gdal.InfoOptions(format="json", showMetadata=False)
                gdalInfo = gdal.Info(viewpoint_path, options=gdalOptions)
            except Exception as err:
                raise HTTPException(status_code=400, detail=f"Failed to fetch statistics of an image. {err}")

            return {"image_statistics": gdalInfo}

        @api_router.get("/{viewpoint_id}/preview.{img_format}/")
        async def get_preview(
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
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)
            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.PREVIEW)

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
        async def get_tile(
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
            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)

            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.TILE)

            if viewpoint_item.range_adjustment is not RangeAdjustmentType.NONE:
                tile_factory = get_tile_factory(
                    tile_format,
                    compression,
                    viewpoint_item.local_object_path,
                    output_type=gdalconst.GDT_Byte,
                    range_adjustment=viewpoint_item.range_adjustment,
                )
            else:
                tile_factory = get_tile_factory(tile_format, compression, viewpoint_item.local_object_path)

            if tile_factory is None:
                raise HTTPException(
                    status_code=500, detail=f"Unable to read tiles from viewpoint {viewpoint_item.viewpoint_id}"
                )

            tile_size = viewpoint_item.tile_size
            image_bytes = tile_factory.create_encoded_tile([x * tile_size, y * tile_size, tile_size, tile_size])

            return StreamingResponse(io.BytesIO(image_bytes), media_type=get_media_type(tile_format), status_code=200)

        @api_router.get("/{viewpoint_id}/crop/{min_x},{min_y},{max_x},{max_y}.{img_format}")
        async def get_crop(
            viewpoint_id: str,
            min_x: int,
            min_y: int,
            max_x: int,
            max_y: int,
            img_format: GDALImageFormats = Path(GDALImageFormats.PNG, description="Output image type."),
            compression: GDALCompressionOptions = Query(
                GDALCompressionOptions.NONE, description="Compression Algorithm for image."
            ),
            width: int = Query(None, description="Width of cropped image in px."),
            height: int = Query(None, description="Height of cropped image in px."),
        ) -> Response:
            """
            Crop a portion of the viewpoint.

            :param viewpoint_id: Unique viewpoint id
            :param min_x: The left pixel coordinate of the desired crop.
            :param min_y: The upper pixel coordinate of the desired crop.
            :param max_x: The right pixel coordinate of the desired crop.
            :param max_y: The lower pixel coordinate of the pixel crop.
            :param img_format: Desired format for cropped output. Valid options are defined by GDALImageFormats.
            :param compression: GDAL image compression.
            :param width: Optional. Width in px of the desired crop.  If provided, max_x will be ignored.
            :param height: Optional. Height in px of the desired crop.  If provided, max_y will be ignored.

            :return: StreamingResponse of cropped image binary with the appropriate mime type based on the img_format
            """

            viewpoint_item = await self.viewpoint_database.get_viewpoint(viewpoint_id)
            await validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.PREVIEW)

            ds, sensor_model = load_gdal_dataset(viewpoint_item.local_object_path)
            tile_factory = GDALTileFactory(
                ds, sensor_model, img_format, compression, gdalconst.GDT_Byte, viewpoint_item.range_adjustment
            )
            preview_options = tile_factory.default_gdal_translate_kwargs.copy()
            crop_width = width if width is not None else max_x - min_x
            crop_height = height if height is not None else max_y - min_y
            preview_options["srcWin"] = [min_x, min_y, crop_width, crop_height]

            preview_bytes = perform_gdal_translation(ds, preview_options)
            return StreamingResponse(io.BytesIO(preview_bytes), media_type=get_media_type(img_format), status_code=200)

        return api_router
