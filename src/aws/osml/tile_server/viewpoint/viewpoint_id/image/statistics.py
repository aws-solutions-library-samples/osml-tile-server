#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, status
from starlette.responses import FileResponse

from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services

statistics_router = APIRouter(
    prefix="/statistics",
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@statistics_router.get("")
def get_image_statistics(viewpoint_id: str, aws: Annotated[get_aws_services, Depends()]) -> FileResponse:
    """
    Get viewpoint statistics based on provided viewpoint id.

    :param aws: Injected AWS services.
    :param viewpoint_id: Unique viewpoint id to get from the table.
    :return: FileResponse containing statistics associated with the viewpoint.
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)

    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.STATISTICS)

    return FileResponse(
        viewpoint_item.local_object_path + ServerConfig.STATISTICS_FILE_EXTENSION, media_type="application/json"
    )
