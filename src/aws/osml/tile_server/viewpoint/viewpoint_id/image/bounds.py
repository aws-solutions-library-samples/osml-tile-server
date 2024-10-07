#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, status
from starlette.responses import FileResponse

from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services

bounds_router = APIRouter(
    prefix="/bounds",
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@bounds_router.get("")
def get_image_bounds(viewpoint_id: str, aws: Annotated[get_aws_services, Depends()]) -> FileResponse:
    """
    Get the [min X, min Y, max X, max Y] boundary of the image in pixels. [0, 0] is assumed to be in the upper
    left corner of the image with x increasing in columns to the right and y increasing in rows down. The
    boundary coordinates are the upper left and lower right corners of the cropped region.

    :param aws: Injected AWS services.
    :param viewpoint_id: Unique viewpoint id to get from the table.
    :return: FileResponse containing bounds for the given item.
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)

    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.BOUNDS)

    return FileResponse(viewpoint_item.local_object_path + ServerConfig.BOUNDS_FILE_EXTENSION, media_type="application/json")
