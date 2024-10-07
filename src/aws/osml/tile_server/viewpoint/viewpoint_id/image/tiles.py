#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from osgeo import gdalconst

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services
from aws.osml.tile_server.utils import get_media_type, get_tile_factory_pool

tiles_router = APIRouter(
    prefix="/tiles",
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@tiles_router.get("/{z}/{x}/{y}.{tile_format}")
def get_image_tile(
    viewpoint_id: str,
    z: int,
    x: int,
    y: int,
    aws: Annotated[get_aws_services, Depends()],
    tile_format: GDALImageFormats = Path(description="Output image type."),
    compression: GDALCompressionOptions = Query(GDALCompressionOptions.NONE, description="Compression Algorithm for image."),
) -> Response:
    """
    Create a tile of this image using the options set when creating the viewpoint.

    :param aws: Injected AWS services.
    :param viewpoint_id: Unique viewpoint id to get from the table.
    :param z: Resolution-level in the image pyramid 0 = full resolution, 1 = full/2, 2 = full/4, ...
    :param x: Tile row location in pixels for the given tile.
    :param y: Tile column location in pixels for the given tile.
    :param tile_format: Desired format for tile output, valid options are defined by GDALImageFormats.
    :param compression: GDAL tile compression format.
    :return: Response of tile image binary payload.
    """
    if z < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Resolution Level for get tile request must be >= 0. Requested z={z}",
        )
    try:
        viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)
        validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.TILE)

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
        return Response(content=bytes(image_bytes), media_type=get_media_type(tile_format), status_code=status.HTTP_200_OK)
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch tile for image. {err}"
        )
