#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Response, status
from osgeo import gdalconst

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType
from aws.osml.image_processing import MapTileId, MapTileSetFactory
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services
from aws.osml.tile_server.utils import get_media_type, get_tile_factory_pool


def _invert_tile_row_index(tile_row: int, tile_matrix: int) -> int:
    return 2**tile_matrix - 1 - tile_row


tile_matrix_router = APIRouter(
    prefix="/{tile_matrix}",
    tags=["map"],
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@tile_matrix_router.get("/{tile_row}/{tile_col}.{tile_format}")
def get_map_tile(
    aws: Annotated[get_aws_services, Depends()],
    viewpoint_id: str,
    tile_matrix_set_id: str,
    tile_matrix: int,
    tile_row: int,
    tile_col: int,
    tile_format: GDALImageFormats = Path(description="Output image type."),
    compression: GDALCompressionOptions = Query(GDALCompressionOptions.NONE, description="Compression Algorithm for image."),
    invert_y: bool = Query(False, description="Invert the TMS tile y-index."),
) -> Response:
    """
    Create a tile by warping the image into an orthophoto and clipping it at the appropriate resolution/bounds
    for the requested tile. This endpoint conforms to the OGC API - Tiles specification.

    :param aws: Injected AWS services.
    :param viewpoint_id: The viewpoint id.
    :param tile_matrix_set_id: The name of the tile matrix set (e.g. WebMercatorQuad).
    :param tile_matrix: The zoom level or tile matrix it.
    :param tile_row: The tile row in the tile matrix.
    :param tile_col: The tile column in the tile matrix.
    :param tile_format: The desired output format.
    :param compression: The desired compression.
    :param invert_y: Whether to invert the tile y index.
    :return: A Response binary image containing the map tile created from this viewpoint.
    """
    if tile_matrix < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Resolution Level for get tile request must be >= 0. Requested z={tile_matrix}",
        )
    if invert_y:
        tile_row = _invert_tile_row_index(tile_row, tile_matrix)
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

        return Response(bytes(image_bytes), media_type=get_media_type(tile_format), status_code=status.HTTP_200_OK)
    except Exception as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Failed to fetch tile for image. {err}"
        )
