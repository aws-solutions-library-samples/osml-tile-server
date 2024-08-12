#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import inspect
import io
import logging
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict
from urllib import parse

import dateutil.parser
import numpy as np
from asgi_correlation_id import correlation_id
from boto3.resources.base import ServiceResource
from cryptography.fernet import Fernet
from fastapi import APIRouter, HTTPException, Query, Request, Response, status
from fastapi_versioning import version
from osgeo import gdalconst
from starlette.responses import FileResponse, StreamingResponse

import aws.osml.tile_server.ogc as ogc
from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType
from aws.osml.image_processing import MapTileId, MapTileSetFactory
from aws.osml.photogrammetry import ImageCoordinate
from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.utils import get_media_type, get_tile_factory_pool, perform_gdal_translation

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
        aws_ddb: ServiceResource,
        aws_sqs: ServiceResource,
        aws_s3: ServiceResource,
        encryptor: Fernet,
    ) -> None:
        """
        The `ViewpointRouter` class is responsible for creating an API router for endpoints related to viewpoints.

        :param aws_ddb: Instance of the ServiceResource class representing the AWS DDB service.
        :param aws_sqs: Instance of the ServiceResource class representing the AWS SQS service.
        :param aws_s3: Instance of the ServiceResource class representing the AWS S3 service.
        :return: None
        """
        logger = logging.getLogger("uvicorn.access")

        # These parameters are not really optional but we need to be able to construct the router to generate
        # documentation for these endpoints. We need to refactor this class to make better use of dependency
        # injection which will resolve this oddity.
        if aws_ddb is not None:
            self.viewpoint_database = ViewpointStatusTable(aws_ddb, logger)
        if aws_sqs is not None:
            self.viewpoint_queue = ViewpointRequestQueue(aws_sqs, ServerConfig.viewpoint_request_queue, logger)
        self.s3 = aws_s3
        self.encryptor = encryptor
        self.logger = logger

    @property
    def router(self) -> APIRouter:
        """
        Create and return a new instance of an APIRouter that is configured to handle the viewpoint endpoints.

        :return: the API router
        """
        api_router = APIRouter(
            prefix="/viewpoints",
            tags=["viewpoints"],
            dependencies=[],
            responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
        )

        @api_router.get("/")
        @version(1, 0)
        def list_viewpoints(max_results: int | None = None, next_token: str | None = None) -> ViewpointListResponse:
            """
            Retrieve a list of available viewpoints. Viewpoints are resources that are created to make images
            stored in the cloud available through these service APIs.

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
                    raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid next_token. {err}")
                if expiration_dt < now:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST, detail="next_token expired. Please submit a new request."
                    )
                if decrypted_endpoint_version != endpoint_version:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
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

        @api_router.post("/", status_code=status.HTTP_201_CREATED)
        def create_viewpoint(viewpoint_request: ViewpointRequest, response: Response) -> Dict[str, Any]:
            """
            Create a new viewpoint for an image stored in the cloud. This operation tells the tile server to
            parse and index information about the source image necessary to improve performance of all following
            calls to the tile server API.

            :param viewpoint_request: client's request which contains name, file source, and range type.
            :param request: A handle to the FastAPI request object.
            :return: Status associated with the request to create the viewpoint in the table.
            """
            expire_time = datetime.now(UTC) + timedelta(days=ServerConfig.ddb_ttl_days)

            # check if the viewpoint already exists.  If so, return existing viewpoint with status code 202
            try:
                existing_viewpoint = self.viewpoint_database.get_viewpoint(viewpoint_request.viewpoint_id).model_dump()
                response.status_code = status.HTTP_202_ACCEPTED
                return existing_viewpoint
            except Exception:
                pass
            # validate viewpoint id prior to creating viewpoint
            if not self._validate_viewpoint_id(viewpoint_request.viewpoint_id):
                # raise ValueError("Invalid viewpoint_id: must not contain whitespace and be URL safe.")
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail="Invalid viewpoint_id: must not contain whitespace and be URL safe.",
                )
            new_viewpoint_request = ViewpointModel(
                viewpoint_id=viewpoint_request.viewpoint_id,
                viewpoint_name=viewpoint_request.viewpoint_name,
                bucket_name=viewpoint_request.bucket_name,
                object_key=viewpoint_request.object_key,
                tile_size=viewpoint_request.tile_size,
                range_adjustment=viewpoint_request.range_adjustment,
                viewpoint_status=ViewpointStatus.REQUESTED,
                local_object_path=None,
                error_message=None,
                expire_time=int(expire_time.timestamp()),
            )
            db_response = self.viewpoint_database.create_viewpoint(new_viewpoint_request)

            attributes = {"correlation_id": {"StringValue": correlation_id.get(), "DataType": "String"}}
            # Place this request into SQS, then the worker will pick up in order to download the image from S3
            self.viewpoint_queue.send_request(new_viewpoint_request.model_dump(), attributes)

            return db_response

        @api_router.delete("/{viewpoint_id}")
        def delete_viewpoint(viewpoint_id: str) -> ViewpointModel:
            """
            Delete a viewpoint when it is no longer needed. This notifies the tile server to clean up any cached
            information and release resources allocated to the viewpoint that are no longer necessary.

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

        @api_router.put("/", status_code=status.HTTP_201_CREATED)
        def update_viewpoint(viewpoint_request: ViewpointUpdate) -> ViewpointModel:
            """
            Change a viewpoint that already exists. This operation can be called to update an existing viewpoint
            when the display options have changed or when the image has moved to a new location.

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
            Retrieve the current status information about a viewpoint along with a detailed description of all
            options chosen.

            :param viewpoint_id: Unique viewpoint id to get from the table.
            :return: Details from the viewpoint item in the table.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
            return viewpoint_item

        @api_router.get("/{viewpoint_id}/image/metadata")
        def get_image_metadata(viewpoint_id: str) -> FileResponse:
            """
            Get the metadata associated with the image. The specific format and amount of information will vary based
            on the source image format and image type.

            :param viewpoint_id: Unique viewpoint id to get from the table.
            :return: Viewpoint metadata associated with the viewpoint item from the table.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.METADATA)

            return FileResponse(
                viewpoint_item.local_object_path + ServerConfig.METADATA_FILE_EXTENSION, media_type="application/json"
            )

        @api_router.get("/{viewpoint_id}/image/bounds")
        def get_image_bounds(viewpoint_id: str) -> FileResponse:
            """
            Get the [min X, min Y, max X, max Y] boundary of the image in pixels. [0, 0] is assumed to be in the upper
            left corner of the image with x increasing in columns to the right and y increasing in rows down. The
            boundary coordinates are the upper left and lower right corners of the cropped region.

            :param viewpoint_id: Unique viewpoint id to get from the table.
            :return: Viewpoint bounds for the given table item.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.BOUNDS)

            return FileResponse(
                viewpoint_item.local_object_path + ServerConfig.BOUNDS_FILE_EXTENSION, media_type="application/json"
            )

        @api_router.get("/{viewpoint_id}/image/info")
        def get_image_info(viewpoint_id: str) -> FileResponse:
            """
            Get a sample GeoJSON feature that represents the extent / boundary of this image in the world.

            :param viewpoint_id: Unique viewpoint id to get from the table.
            :return: Viewpoint info associated with the given id.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.INFO)

            return FileResponse(
                viewpoint_item.local_object_path + ServerConfig.INFO_FILE_EXTENSION, media_type="application/json"
            )

        @api_router.get("/{viewpoint_id}/image/statistics")
        def get_image_statistics(viewpoint_id: str) -> FileResponse:
            """
            Get viewpoint statistics based on provided viewpoint id.

            :param viewpoint_id: Unique viewpoint id to get from the table.
            :return: Viewpoint statistics associated with the id.
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)

            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.STATISTICS)

            return FileResponse(
                viewpoint_item.local_object_path + ServerConfig.STATISTICS_FILE_EXTENSION, media_type="application/json"
            )

        @api_router.get("/{viewpoint_id}/image/preview.{img_format}")
        def get_image_preview(
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
            Get a preview/thumbnail image in the requested format

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
                return StreamingResponse(
                    io.BytesIO(preview_bytes), media_type=get_media_type(img_format), status_code=status.HTTP_200_OK
                )

        @api_router.get("/{viewpoint_id}/image/tiles/{z}/{x}/{y}.{tile_format}")
        def get_image_tile(
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
            Create a tile of this image using the options set when creating the viewpoint.

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
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Resolution Level for get tile request must be >= 0. Requested z={z}",
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
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Unable to read tiles from viewpoint {viewpoint_item.viewpoint_id}",
                        )

                    tile_size = viewpoint_item.tile_size
                    src_tile_size = 2**z * tile_size
                    image_bytes = tile_factory.create_encoded_tile(
                        src_window=[x * src_tile_size, y * src_tile_size, src_tile_size, src_tile_size],
                        output_size=(tile_size, tile_size),
                    )

                return StreamingResponse(
                    io.BytesIO(image_bytes), media_type=get_media_type(tile_format), status_code=status.HTTP_200_OK
                )
            except Exception as err:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch tile for image. {err}"
                )

        @api_router.get("/{viewpoint_id}/image/crop/{min_x},{min_y},{max_x},{max_y}.{img_format}")
        def get_image_crop(
            viewpoint_id: str = Path(description="Unique identifier for this viewpoint"),
            min_x: int = Path(description="The left pixel coordinate of the desired crop."),
            min_y: int = Path(description="The upper pixel coordinate of the desired crop."),
            max_x: int = Path(description="The right pixel coordinate of the desired crop."),
            max_y: int = Path(description="The lower pixel coordinate of the pixel crop."),
            img_format: GDALImageFormats = Path(
                default=GDALImageFormats.PNG,
                description="Desired format for cropped output. Valid options are defined by GDALImageFormats.",
            ),
            compression: GDALCompressionOptions = Query(
                default=GDALCompressionOptions.NONE, description="Desired compression algorithm for image."
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
            Crop out an arbitrary region of the full resolution image given a bounding box in pixel coordinates.
            [0, 0] is assumed to be in the upper left corner of the image with x increasing in columns to the
            right and y increasing in rows down. The [min_x, min_y, max_x, max_y] coordinates are the upper
            left and lower right corners of the cropped region.

            :param viewpoint_id: Unique viewpoint id to get from the table as a crop.
            :param min_x: The left pixel coordinate of the desired crop.
            :param min_y: The upper pixel coordinate of the desired crop.
            :param max_x: The right pixel coordinate of the desired crop.
            :param max_y: The lower pixel coordinate of the pixel crop.
            :param img_format: Desired format for cropped output. Valid options are defined by GDALImageFormats.
            :param compression: Desired compression algorithm for the output image.
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
                return StreamingResponse(
                    io.BytesIO(crop_bytes), media_type=get_media_type(img_format), status_code=status.HTTP_200_OK
                )

        @api_router.get("/{viewpoint_id}/map/tiles", response_model_exclude_none=True)
        def get_map_tilesets(viewpoint_id: str, request: Request) -> ogc.TileSetList:
            """
            Retrieves the list of tilesets available for this viewpoint. This endpoint conforms to the
            OGC API - Tiles: Tileset & Tilsets List specification.

            :param viewpoint_id: the viewpoint id
            :return: a list of tilesets that are available for this viewpoint
            """
            viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
            self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.TILE)

            return ogc.TileSetList(
                tilesets=[
                    ogc.TileSetItem(
                        title="WebMercatorQuad",
                        data_type=ogc.DataType.MAP,
                        crs="http://www.opengis.net/def/crs/EPSG/0/3857",
                        tile_matrix_set_uri="http://www.opengis.net/def/tilematrixset/OGC/1.0/WebMercatorQuad",
                        links=[ogc.Link(href=f"{request.url}/WebMercatorQuad", rel="self", type="application/json")],
                    ),
                    ogc.TileSetItem(
                        title="WebMercatorQuadx2",
                        data_type=ogc.DataType.MAP,
                        crs="http://www.opengis.net/def/crs/EPSG/0/3857",
                        tile_matrix_set_uri="http://www.opengis.net/def/tilematrixset/OGC/1.0/WebMercatorQuadx2",
                        links=[ogc.Link(href=f"{request.url}/WebMercatorQuadx2", rel="self", type="application/json")],
                    ),
                ]
            )

        @api_router.get("/{viewpoint_id}/map/tiles/{tile_matrix_set_id}", response_model_exclude_none=True)
        def get_map_tileset_metadata(viewpoint_id: str, tile_matrix_set_id: str, request: Request) -> ogc.TileSetMetadata:
            """
            Retrieves the metadata for a specific tileset. This endpoint conforms to the OGC API - Tiles: Tileset &
            Tilsets List specification.

            :param viewpoint_id: the viewpoint id
            :param tile_matrix_set_id: the name of the tile matrix set (e.g. WebMercatorQuad)
            :param request: A handle to the FastAPI request object.
            :return: the tileset metadata
            """

            try:
                # Find the tile in the named tileset
                tile_set = MapTileSetFactory.get_for_id(tile_matrix_set_id)
                if not tile_set:
                    raise ValueError(f"Unsupported tile set: {tile_matrix_set_id}")

                viewpoint_item = self.viewpoint_database.get_viewpoint(viewpoint_id)
                self._validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.TILE)

                tile_format = GDALImageFormats.PNG
                compression = GDALCompressionOptions.NONE
                output_type = gdalconst.GDT_Byte

                tile_factory_pool = get_tile_factory_pool(
                    tile_format, compression, viewpoint_item.local_object_path, output_type, viewpoint_item.range_adjustment
                )
                with tile_factory_pool.checkout_in_context() as tile_factory:
                    # Use the sensor model to find the geographic location of the 4 image corners
                    width = tile_factory.raster_dataset.RasterXSize
                    height = tile_factory.raster_dataset.RasterYSize

                    image_corners = [[0, 0], [width, 0], [width, height], [0, height]]
                    geo_image_corners = [
                        tile_factory.sensor_model.image_to_world(ImageCoordinate(corner)) for corner in image_corners
                    ]

                    # Create the 2D geospatial bounding box for the image by taking the min/max values of the
                    # geographic image corners
                    geo_image_corner_latitudes = [corner.latitude for corner in geo_image_corners]
                    geo_image_corner_longitude = [corner.longitude for corner in geo_image_corners]
                    bounding_box = ogc.BoundingBox2D(
                        lower_left=(
                            np.degrees(min(geo_image_corner_longitude)),
                            np.degrees(min(geo_image_corner_latitudes)),
                        ),
                        upper_right=(
                            np.degrees(max(geo_image_corner_longitude)),
                            np.degrees(max(geo_image_corner_latitudes)),
                        ),
                    )

                    # Generate tile matrix limits for each resolution level in the pyramid
                    highest_single_tile_matrix_number = 0
                    tile_matrix_set_limits = []
                    for tile_matrix in range(0, 25):
                        # Calculate the tile limits for this resolution level
                        min_tile_col, min_tile_row, max_tile_col, max_tile_row = tile_set.get_tile_matrix_limits_for_area(
                            boundary_coordinates=geo_image_corners, tile_matrix=tile_matrix
                        )

                        tile_matrix_set_limits.append(
                            ogc.TileMatrixLimits(
                                tile_matrix=str(tile_matrix),
                                min_tile_row=min_tile_row,
                                max_tile_row=max_tile_row,
                                min_tile_col=min_tile_col,
                                max_tile_col=max_tile_col,
                            )
                        )

                        # This keeps track of the last resolution level where the entire image fit into a single
                        # map tile.
                        if min_tile_col == max_tile_col and min_tile_row == max_tile_row:
                            highest_single_tile_matrix_number = tile_matrix

                        # Check to see if the collection of map tiles at this level is bigger than the full
                        # resolution image. If so we can stop generating tile matrix limits since any additional
                        # levels are likely to be beyond the resolution of the data itself.
                        center_tile = tile_set.get_tile(
                            MapTileId(
                                tile_matrix=tile_matrix,
                                tile_row=int((max_tile_row + min_tile_row) / 2),
                                tile_col=int((max_tile_col + min_tile_col) / 2),
                            )
                        )
                        max_tile_span = max(1 + max_tile_col - min_tile_col, 1 + max_tile_row - min_tile_row) * max(
                            center_tile.size
                        )
                        max_pixel_span = max(width, height)
                        if max_tile_span > max_pixel_span:
                            break

                    # Create a point that is at the center of the image at the resolution level that would
                    # show a thumbnail or overview on the map. The actual requirements for this center point
                    # field are underspecified in the OGC API, so we're giving them a starting point where the
                    # user could see the whole image and drill down into an area of interest.
                    center_point = ogc.TilePoint(
                        coordinates=(
                            np.degrees(np.mean(geo_image_corner_longitude)),
                            np.degrees(np.mean(geo_image_corner_latitudes)),
                        ),
                        tile_matrix=str(highest_single_tile_matrix_number),
                    )

                    return ogc.TileSetMetadata(
                        data_type=ogc.DataType.MAP,
                        crs="http://www.opengis.net/def/crs/EPSG/0/3857",
                        links=[ogc.Link(href=f"{request.url}", rel="self", type="application/json")],
                        tile_matrix_set_limits=tile_matrix_set_limits,
                        bounding_box=bounding_box,
                        center_point=center_point,
                    )

            except Exception as err:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch tile set metadata. {err}"
                )

        @api_router.get("/{viewpoint_id}/map/tiles/{tile_matrix_set_id}/{tile_matrix}/{tile_row}/{tile_col}.{tile_format}")
        def get_map_tile(
            viewpoint_id: str,
            tile_matrix_set_id: str,
            tile_matrix: int,
            tile_row: int,
            tile_col: int,
            tile_format: GDALImageFormats = Path(GDALImageFormats.PNG, description="Output image type. Defaults to PNG."),
            compression: GDALCompressionOptions = Query(
                GDALCompressionOptions.NONE, description="Compression Algorithm for image."
            ),
        ) -> Response:
            """
            Create a tile by warping the image into an orthophoto and clipping it at the appropriate resolution/bounds
            for the requested tile. This endpoint conforms to the OGC API - Tiles specification.

            :param viewpoint_id: the viewpoint id
            :param tile_matrix_set_id: the name of the tile matrix set (e.g. WebMercatorQuad)
            :param tile_matrix: the zoom level or tile matrix it
            :param tile_row: the tile row in the tile matrix
            :param tile_col: the tile column in the tile matrix
            :param tile_format: the desired output format
            :param compression: the desired compression
            :return: a binary image containing the map tile created from this viewpoint
            """
            if tile_matrix < 0:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Resolution Level for get tile request must be >= 0. Requested z={tile_matrix}",
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
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail=f"Unable to read tiles from viewpoint {viewpoint_item.viewpoint_id}",
                        )

                    # Find the tile in the named tileset
                    tile_set = MapTileSetFactory.get_for_id(tile_matrix_set_id)
                    if not tile_set:
                        raise ValueError(f"Unsupported tile set: {tile_matrix_set_id}")
                    tile_id = MapTileId(tile_matrix=tile_matrix, tile_row=tile_row, tile_col=tile_col)
                    tile = tile_set.get_tile(tile_id)

                    # Create an orthophoto for this tile
                    image_bytes = tile_factory.create_orthophoto_tile(geo_bbox=tile.bounds, tile_size=tile.size)

                if image_bytes is None:
                    # OGC Tiles API Section 7.1.7.B indicates that a 204 should be returned for empty tiles
                    return Response(status_code=status.HTTP_204_NO_CONTENT)

                return StreamingResponse(
                    io.BytesIO(image_bytes), media_type=get_media_type(tile_format), status_code=status.HTTP_200_OK
                )
            except Exception as err:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch tile for image. {err}"
                )

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
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Cannot view {api_operation} for this image since this has already been " f"deleted.",
            )
        elif current_status == ViewpointStatus.REQUESTED:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="This viewpoint has been requested and not in READY state. Please try again " "later.",
            )

    @staticmethod
    def _validate_viewpoint_id(viewpoint_id: str) -> bool:
        """
        This is a helper function to validate that the user supplied viewpoint ID does not contain whitespace and
        is URL safe.

        :param id: The viewpoint_id input string.
        :return: True/False if the viewpoint ID is valid or not.
        """
        try:
            no_whitespace = "".join(viewpoint_id.split())
            encoded = parse.quote(no_whitespace, safe="")
        except Exception:
            return False
        return viewpoint_id == encoded
