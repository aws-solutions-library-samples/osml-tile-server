#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from typing import Annotated

from fastapi import APIRouter, Depends, status
from starlette.responses import FileResponse

from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.models import ViewpointApiNames, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services

metadata_router = APIRouter(
    prefix="/metadata",
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@metadata_router.get("")
def get_image_metadata(viewpoint_id: str, aws: Annotated[get_aws_services, Depends()]) -> FileResponse:
    """
    Get the metadata associated with the image. The specific format and amount of information will vary based
    on the source image format and image type.

    :param aws: Injected AWS services.
    :param viewpoint_id: Unique viewpoint id to get from the table.
    :return: FileResponse containing metadata associated with the viewpoint item from the table.
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)

    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.METADATA)

    return FileResponse(
        viewpoint_item.local_object_path + ServerConfig.METADATA_FILE_EXTENSION, media_type="application/json"
    )
