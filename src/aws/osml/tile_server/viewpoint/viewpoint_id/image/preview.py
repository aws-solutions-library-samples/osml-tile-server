#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Response, status
from osgeo import gdalconst

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services
from aws.osml.tile_server.utils import get_media_type, get_tile_factory_pool, perform_gdal_translation

preview_router = APIRouter(
    prefix="/preview",
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@preview_router.get(".{img_format}")
def get_image_preview(
    aws: Annotated[get_aws_services, Depends()],
    viewpoint_id: str,
    img_format: GDALImageFormats = Path(description="Output image type."),
    max_size: int = 1024,
    width: int = 0,
    height: int = 0,
    compression: GDALCompressionOptions = Query(GDALCompressionOptions.NONE, description="Compression Algorithm for image."),
) -> Response:
    """
    Get a preview/thumbnail image in the requested format

    :param aws: Injected AWS services.
    :param viewpoint_id: Unique viewpoint id to get from the table.
    :param img_format: The Desired format for preview output, valid options are defined by GDALImageFormats.
    :param max_size: Max size of the preview image, defaults to 1024 pixels.
    :param width: Preview width in pixels that supersedes scale if > 0.
    :param height: Preview height in pixels that supersedes scale if > 0.
    :param compression: GDAL image compression format to use.
    :return: Response of preview binary with the appropriate mime type based on the img_format
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)
    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.PREVIEW)

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
        return Response(bytes(preview_bytes), media_type=get_media_type(img_format), status_code=status.HTTP_200_OK)
