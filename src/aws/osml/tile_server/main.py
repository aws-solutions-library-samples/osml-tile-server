import asyncio
import logging
import sys
from typing import Any, Tuple

import uvicorn
from botocore.exceptions import ClientError
from fastapi import FastAPI
from osgeo import gdal

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
    },
)


def initialize_services() -> Tuple[Any, Any, Any]:
    """
    Initialize AWS services required by the application.

    This function initializes DynamoDB, S3, and SQS services,
    handling any exceptions that occur during the process.

    :raises SystemExit: If any service fails to initialize.
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


def initialize_viewpoint_components() -> Tuple[Any, Any, Any]:
    """
    Initialize viewpoint-related components.

    This function creates instances of the ViewpointStatusTable,
    ViewpointRequestQueue, and ViewpointRouter using initialized AWS services.

    :return: Tuple containing initialized viewpoint components.
    :rtype: Tuple
    """
    try:
        database = ViewpointStatusTable(aws_ddb)
    except ClientError as err:
        logger.error(f"Fatal error occurred while initializing viewpoint database. Exception: {err}")
        sys.exit("Fatal error occurred while initializing viewpoint database. Exiting.")

    request_queue = ViewpointRequestQueue(aws_sqs, ServerConfig.viewpoint_request_queue)
    router = ViewpointRouter(database, request_queue, aws_s3)
    return database, request_queue, router


viewpoint_database, viewpoint_request_queue, viewpoint_router = initialize_viewpoint_components()

# Include the viewpoint router in the FastAPI app
app.include_router(viewpoint_router.router)


@app.on_event("startup")
async def run_viewpoint_worker() -> None:
    """
    Start the Viewpoint Worker on application startup.

    This asynchronous function is executed when the FastAPI application starts.
    It creates and runs the ViewpointWorker to process viewpoint requests.
    """
    viewpoint_worker = ViewpointWorker(viewpoint_request_queue, aws_s3, viewpoint_database)
    asyncio.create_task(viewpoint_worker.run())


@app.get("/")
async def root() -> str:
    """
    Root endpoint for the application.

    Returns a welcome message and basic application information including
    contact and license details.

    :return: Welcome message with application information.
    :rtype: str
    """
    homepage_description = f"""Hello! Welcome to {app.title} - {app.version}! {app.description}.

    If you need help or support, please cut an issue ticket of this product - {app.contact["url"]}.

    The license of this product: {app.license_info["license"]}
    """
    return homepage_description


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=4557, reload=True)
