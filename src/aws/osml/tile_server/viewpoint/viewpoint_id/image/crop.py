#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Response, status
from osgeo import gdalconst

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services
from aws.osml.tile_server.utils import get_media_type, get_tile_factory_pool

crop_router = APIRouter(
    prefix="/crop",
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@crop_router.get("/{min_x},{min_y},{max_x},{max_y}.{img_format}")
def get_image_crop(
    aws: Annotated[get_aws_services, Depends()],
    viewpoint_id: str = Path(description="Unique identifier for this viewpoint"),
    min_x: int = Path(description="The left pixel coordinate of the desired crop."),
    min_y: int = Path(description="The upper pixel coordinate of the desired crop."),
    max_x: int = Path(description="The right pixel coordinate of the desired crop."),
    max_y: int = Path(description="The lower pixel coordinate of the pixel crop."),
    img_format: GDALImageFormats = Path(
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

    :param aws: Injected AWS services.
    :param viewpoint_id: Unique viewpoint id to get from the table as a crop.
    :param min_x: The left pixel coordinate of the desired crop.
    :param min_y: The upper pixel coordinate of the desired crop.
    :param max_x: The right pixel coordinate of the desired crop.
    :param max_y: The lower pixel coordinate of the pixel crop.
    :param img_format: Desired format for cropped output. Valid options are defined by GDALImageFormats.
    :param compression: Desired compression algorithm for the output image.
    :param width: Optional width in px of the desired crop, if provided, max_x will be ignored.
    :param height: Optional height in px of the desired crop, if provided, max_y will be ignored.
    :return: Response of cropped image binary with the appropriate mime type based on the img_format
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)
    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.PREVIEW)

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
        return Response(bytes(crop_bytes), media_type=get_media_type(img_format), status_code=status.HTTP_200_OK)
