#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import inspect
import logging
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Dict
from urllib import parse

import dateutil.parser
from asgi_correlation_id import correlation_id
from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi_versioning import version

from aws.osml.tile_server.app_config import ServerConfig
from aws.osml.tile_server.models import (
    ViewpointApiNames,
    ViewpointListResponse,
    ViewpointModel,
    ViewpointRequest,
    ViewpointStatus,
    ViewpointUpdate,
    validate_viewpoint_status,
)
from aws.osml.tile_server.services import get_aws_services, get_encryptor

from .viewpoint_id.router import viewpoint_id_router

logger = logging.getLogger("uvicorn.access")


def _validate_viewpoint_id(viewpoint_id: str) -> bool:
    """
    This is a helper function to validate that the user supplied viewpoint ID does not contain whitespace and
    is URL safe.

    :param id: The viewpoint_id input string.
    :return: True/False if the viewpoint ID is valid or not.
    """
    try:
        no_whitespace = "".join(viewpoint_id.split())
        encoded = parse.quote(no_whitespace, safe="")
    except Exception:
        return False
    return viewpoint_id == encoded


viewpoint_router = APIRouter(
    prefix="/viewpoints",
    tags=["viewpoints"],
    dependencies=[],
    responses={status.HTTP_404_NOT_FOUND: {"description": "Not found!"}},
)


@viewpoint_router.get("/")
@version(1, 0)
def list_viewpoints(
    aws: Annotated[get_aws_services, Depends()],
    encryptor: Annotated[get_encryptor, Depends()],
    max_results: int | None = None,
    next_token: str | None = None,
) -> ViewpointListResponse:
    """
    Retrieve a list of available viewpoints. Viewpoints are resources that are created to make images
    stored in the cloud available through these service APIs.

    :param aws: Injected AWS services
    :param encryptor: Injected encryptor
    :param max_results: Optional max number of viewpoints requested
    :param next_token: Optional token to begin a query from provided by the previous query response.
    :return: List of viewpoints with details from the table.
    """
    current_function_name = inspect.stack()[0].function
    current_route = [route for route in viewpoint_router.routes if route.name == current_function_name][0]
    current_endpoint = current_route.endpoint
    endpoint_major_version, endpoint_minor_version = getattr(current_endpoint, "_api_version", (0, 0))
    endpoint_version = f"{endpoint_major_version}.{endpoint_minor_version}"

    if next_token:
        try:
            decrypted_token = encryptor.decrypt(next_token.encode("utf-8")).decode("utf-8")
            decrypted_next_token, expiration_iso, decrypted_endpoint_version = decrypted_token.split("|")
            expiration_dt = dateutil.parser.isoparse(expiration_iso)
            now = datetime.now(UTC)
        except Exception as err:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Invalid next_token. {err}")
        if expiration_dt < now:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="next_token expired. Please submit a new request."
            )
        if decrypted_endpoint_version != endpoint_version:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="next_token is not compatible with this endpoint version. Please submit a new request.",
            )
    else:
        decrypted_next_token = None

    results = aws.viewpoint_database.get_viewpoints(max_results, decrypted_next_token)
    if results.next_token:
        expiration = datetime.now(UTC) + timedelta(days=1)
        token_string = f"{results.next_token}|{expiration.isoformat()}|{endpoint_version}"
        encrypted_token = encryptor.encrypt(token_string.encode("utf-8")).decode("utf-8")
        results.next_token = encrypted_token
    return results


@viewpoint_router.post("/", status_code=status.HTTP_201_CREATED)
def create_viewpoint(
    viewpoint_request: ViewpointRequest, aws: Annotated[get_aws_services, Depends()], response: Response
) -> Dict[str, Any]:
    """
    Create a new viewpoint for an image stored in the cloud. This operation tells the tile server to
    parse and index information about the source image necessary to improve performance of all following
    calls to the tile server API.

    :param aws: Injected AWS services
    :param viewpoint_request: client's request which contains name, file source, and range type.
    :param request: A handle to the FastAPI request object.
    :return: Status associated with the request to create the viewpoint in the table.
    """
    expire_time = datetime.now(UTC) + timedelta(days=ServerConfig.ddb_ttl_days)

    # check if the viewpoint already exists.  If so, return existing viewpoint with status code 202
    try:
        existing_viewpoint = aws.viewpoint_database.get_viewpoint(viewpoint_request.viewpoint_id).model_dump()
        response.status_code = status.HTTP_202_ACCEPTED
        return existing_viewpoint
    except Exception:
        pass
    # validate viewpoint id prior to creating viewpoint
    if not _validate_viewpoint_id(viewpoint_request.viewpoint_id):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Invalid viewpoint_id: must not contain whitespace and be URL safe.",
        )
    new_viewpoint_request = ViewpointModel(
        viewpoint_id=viewpoint_request.viewpoint_id,
        viewpoint_name=viewpoint_request.viewpoint_name,
        bucket_name=viewpoint_request.bucket_name,
        object_key=viewpoint_request.object_key,
        tile_size=viewpoint_request.tile_size,
        range_adjustment=viewpoint_request.range_adjustment,
        viewpoint_status=ViewpointStatus.REQUESTED,
        local_object_path=None,
        error_message=None,
        expire_time=int(expire_time.timestamp()),
    )
    db_response = aws.viewpoint_database.create_viewpoint(new_viewpoint_request)

    attributes = {"correlation_id": {"StringValue": correlation_id.get(), "DataType": "String"}}
    # Place this request into SQS, then the worker will pick up in order to download the image from S3
    aws.viewpoint_queue.send_request(new_viewpoint_request.model_dump(), attributes)

    return db_response


@viewpoint_router.put("/", status_code=status.HTTP_201_CREATED)
def update_viewpoint(viewpoint_request: ViewpointUpdate, aws: Annotated[get_aws_services, Depends()]) -> ViewpointModel:
    """
    Change a viewpoint that already exists. This operation can be called to update an existing viewpoint
    when the display options have changed or when the image has moved to a new location.

    :param aws: Injected AWS services.
    :param viewpoint_request: Client's request, which contains name, file source, and range type.
    :return: Updated viewpoint item details from the table.
    """
    viewpoint_item = aws.viewpoint_database.get_viewpoint(viewpoint_request.viewpoint_id)

    validate_viewpoint_status(viewpoint_item.viewpoint_status, ViewpointApiNames.UPDATE)

    viewpoint_item.viewpoint_name = viewpoint_request.viewpoint_name
    viewpoint_item.tile_size = viewpoint_request.tile_size
    viewpoint_item.range_adjustment = viewpoint_request.range_adjustment

    return aws.viewpoint_database.update_viewpoint(viewpoint_item)


viewpoint_router.include_router(viewpoint_id_router)
