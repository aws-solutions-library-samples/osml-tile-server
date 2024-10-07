#  Copyright 2024 Amazon.com, Inc. or its affiliates.

import shutil
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status

from aws.osml.tile_server.models import ViewpointApiNames, ViewpointModel, validate_viewpoint_status
from aws.osml.tile_server.services import get_aws_services

from .image.router import image_router
from .map.router import map_tiles_router

viewpoint_id_router = APIRouter(
    prefix="/{viewpoint_id}",
    tags=["viewpoint"],
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@viewpoint_id_router.get("")
def describe_viewpoint(viewpoint_id: str, aws: Annotated[get_aws_services, Depends()]) -> ViewpointModel:
    """
    Retrieve the current status information about a viewpoint along with a detailed description of all
    options chosen.

    :param aws: Injected AWS services
    :param viewpoint_id: Unique viewpoint id to get from the table.
    :return: Details from the viewpoint item in the table.
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)
    return viewpoint_item


@viewpoint_id_router.delete("", status_code=status.HTTP_204_NO_CONTENT)
def delete_viewpoint(viewpoint_id: str, aws: Annotated[get_aws_services, Depends()]) -> None:
    """
    Delete a viewpoint when it is no longer needed. This notifies the tile server to clean up any cached
    information and release resources allocated to the viewpoint that are no longer necessary.

    :param aws: Injected AWS services.
    :param viewpoint_id: Unique viewpoint id to get from the table.
    :return: None
    """
    try:
        viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_id)
    except Exception:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"viewpoint_id {viewpoint_id} not found.")

    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.UPDATE)

    if viewpoint_item:
        shutil.rmtree(Path(viewpoint_item.local_object_path).parent, ignore_errors=True)
    aws.viewpoint_database.delete_viewpoint(viewpoint_id)


viewpoint_id_router.include_router(image_router)
viewpoint_id_router.include_router(map_tiles_router)
