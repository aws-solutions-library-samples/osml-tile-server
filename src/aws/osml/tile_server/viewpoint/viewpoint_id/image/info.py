#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, status
from starlette.responses import FileResponse

from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.services import ViewpointApiNames, get_aws_services, validate_viewpoint_status

info_router = APIRouter(
    prefix="/info",
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@info_router.get("")
def get_image_info(viewpoint_id: str, aws: Annotated[get_aws_services, Depends()]) -> FileResponse:
    """
    Get a sample GeoJSON feature that represents the extent / boundary of this image in the world.

    :param viewpoint_id: Unique viewpoint id to get from the table.
    :return: Viewpoint info associated with the given id.
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)

    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.INFO)

    return FileResponse(viewpoint_item.local_object_path + ServerConfig.INFO_FILE_EXTENSION, media_type="application/json")
