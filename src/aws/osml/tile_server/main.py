import logging
import sys
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Tuple

import uvicorn
from boto3.resources.base import ServiceResource
from botocore.exceptions import ClientError
from fastapi import FastAPI, status
from osgeo import gdal
from pydantic import BaseModel

from .app_config import ServerConfig
from .utils.aws_services import initialize_ddb, initialize_s3, initialize_sqs
from .viewpoint.database import ViewpointStatusTable
from .viewpoint.queue import ViewpointRequestQueue
from .viewpoint.routers import ViewpointRouter
from .viewpoint.worker import ViewpointWorker

# Configure GDAL to throw Python exceptions on errors
gdal.UseExceptions()

# Configure logger
logger = logging.getLogger("uvicorn")


def initialize_services() -> Tuple[ServiceResource, ServiceResource, ServiceResource]:
    """
    Initialize AWS services required by the application.

    This function initializes DynamoDB, S3, and SQS services,
    handling any exceptions that occur during the process.

    :return: Tuple containing the initialized service resources for ddb, s3, and sqs clients.

    :raises: SystemExit if any service fails to initialize.
    """
    try:
        ddb = initialize_ddb()
        s3 = initialize_s3()
        sqs = initialize_sqs()
    except ClientError as err:
        logger.error(f"Fatal error occurred while initializing AWS services. Exception: {err}")
        sys.exit("Fatal error occurred while initializing AWS services. Exiting.")

    return ddb, s3, sqs


aws_ddb, aws_s3, aws_sqs = initialize_services()


def initialize_viewpoint_components() -> Tuple[ViewpointStatusTable, ViewpointRequestQueue, ViewpointRouter]:
    """
    Initialize viewpoint-related components.

    This function creates instances of the ViewpointStatusTable,
    ViewpointRequestQueue, and ViewpointRouter using initialized AWS services.

    :return: Tuple containing initialized viewpoint components.

    :raises: ClientError if the database client failed to initialize
    """
    try:
        database = ViewpointStatusTable(aws_ddb)
    except ClientError as err:
        logger.error(f"Fatal error occurred while initializing viewpoint database. Exception: {err}")
        sys.exit("Fatal error occurred while initializing viewpoint database. Exiting.")

    request_queue = ViewpointRequestQueue(aws_sqs, ServerConfig.viewpoint_request_queue)
    router = ViewpointRouter(database, request_queue, aws_s3)
    return database, request_queue, router


@asynccontextmanager
async def lifespan() -> AbstractAsyncContextManager[None] | FastAPI:
    """
    Start the Viewpoint Worker as part of the FastAPI lifespan.

    :return: The lifespan construct associated with the fast API

    This function starts the Viewpoint Worker as part of the FastAPI lifespan,
    and stops it after yield. For more information refer to FastAPI events
    documentation at : https://fastapi.tiangolo.com/advanced/events/

    Note:
        aws_s3, viewpoint_database and viewpoint_request_queue should be predefined
        before this function's call. They represent your AWS S3 bucket instance,
        your database instance and a queue of requests respectively.
    """
    viewpoint_worker = ViewpointWorker(viewpoint_request_queue, aws_s3, viewpoint_database)
    viewpoint_worker.start()
    yield
    viewpoint_worker.join(timeout=20)


# Initialize FastAPI app
app = FastAPI(
    title="OSML Tile Server",
    description="A minimalistic tile server for imagery hosted in the cloud",
    version="0.1.0",
    terms_of_service="https://example.com/terms/",
    contact={
        "name": "Amazon Web Services",
        "email": "aws-osml-admin@amazon.com",
        "url": "https://github.com/aws-solutions-library-samples/osml-tile-server/issues",
    },
    license_info={
        "license": """© 2023 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
        This AWS Content is provided subject to the terms of the AWS Customer Agreement
        available at https://aws.amazon.com/agreement or other written agreement between
        Customer and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.""",
        "name": "TEST",
    },
    lifespan=lifespan,
)

viewpoint_database, viewpoint_request_queue, viewpoint_router = initialize_viewpoint_components()

# Include the viewpoint router in the FastAPI app
app.include_router(viewpoint_router.router)


class HealthCheck(BaseModel):
    """
    A Pydantic model for a health check response.

    Attributes:
        status (str): Status of the health check. Defaults is "OK".
    """

    status: str = "OK"


@app.get("/")
async def root() -> str:
    """
    Root endpoint for the application.

    Returns a welcome message and basic application information including
    contact and license details.

    :return: Welcome message with application information.
    """
    homepage_description = f"""Hello! Welcome to {app.title} - {app.version}! {app.description}.

    If you need help or support, please cut an issue ticket of this product - {app.contact["url"]}.

    The license of this product: {app.license_info["license"]}
    """
    return homepage_description


@app.get(
    "/ping",
    tags=["healthcheck"],
    summary="Perform a Health Check",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status.HTTP_200_OK,
    response_model=HealthCheck,
)
async def healthcheck() -> HealthCheck:
    """
    Endpoint to perform a healthcheck on. This endpoint can primarily be used by Docker
    to ensure robust container orchestration and management is in place. Other
    services which rely on proper functioning of the API service will not deploy if this
    endpoint returns any other HTTP status code except 200 (OK).

    :return: JSON response with the health status, 200
    """
    return HealthCheck(status="OK")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=4557, reload=True)
