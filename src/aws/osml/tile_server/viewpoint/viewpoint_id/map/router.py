#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, Request, status

import aws.osml.tile_server.ogc as ogc
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services

from .tileset.router import tile_tileset_metadata_router

map_tiles_router = APIRouter(
    prefix="/map/tiles",
    tags=["map"],
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@map_tiles_router.get("", response_model_exclude_none=True)
def get_map_tilesets(viewpoint_id: str, aws: Annotated[get_aws_services, Depends()], request: Request) -> ogc.TileSetList:
    """
    Retrieves the list of tilesets available for this viewpoint. This endpoint conforms to the
    OGC API - Tiles: Tileset & Tilsets List specification.

    :param aws: Injected AWS services.
    :param viewpoint_id: The viewpoint id.
    :return: A TileSetList object containing a list of tilesets that are available for this viewpoint.
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)
    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.TILE)

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


map_tiles_router.include_router(tile_tileset_metadata_router)
