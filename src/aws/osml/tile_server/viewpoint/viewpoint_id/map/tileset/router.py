#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

import numpy as np
from fastapi import APIRouter, Depends, HTTPException, Request, status
from osgeo import gdalconst

import aws.osml.tile_server.ogc as ogc
from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats
from aws.osml.image_processing import MapTileId, MapTileSetFactory
from aws.osml.photogrammetry import ImageCoordinate
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services
from aws.osml.tile_server.utils import get_tile_factory_pool

from .tile import tile_matrix_router

tile_tileset_metadata_router = APIRouter(
    prefix="/{tile_matrix_set_id}",
    tags=["map"],
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@tile_tileset_metadata_router.get("", response_model_exclude_none=True)
def get_map_tileset_metadata(
    viewpoint_id: str, tile_matrix_set_id: str, aws: Annotated[get_aws_services, Depends()], request: Request
) -> ogc.TileSetMetadata:
    """
    Retrieves the metadata for a specific tileset. This endpoint conforms to the OGC API - Tiles: Tileset &
    Tilsets List specification.

    :param aws: Injected AWS services.
    :param viewpoint_id: The viewpoint id.
    :param tile_matrix_set_id: The name of the tile matrix set (e.g. WebMercatorQuad).
    :param request: A handle to the FastAPI request object.
    :return: A TileSetMetadata object containing the tileset metadata.
    """

    try:
        # Find the tile in the named tileset
        tile_set = MapTileSetFactory.get_for_id(tile_matrix_set_id)
        if not tile_set:
            raise ValueError(f"Unsupported tile set: {tile_matrix_set_id}")

        viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)
        validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.TILE)

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
                max_tile_span = max(1 + max_tile_col - min_tile_col, 1 + max_tile_row - min_tile_row) * max(center_tile.size)
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


tile_tileset_metadata_router.include_router(tile_matrix_router)
