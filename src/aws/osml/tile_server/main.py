#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import sys
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from time import sleep

import uvicorn
from asgi_correlation_id import CorrelationIdFilter, CorrelationIdMiddleware
from botocore.exceptions import ClientError
from cryptography.fernet import Fernet
from fastapi import FastAPI, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi_versioning import VersionedFastAPI
from osgeo import gdal
from pythonjsonlogger.jsonlogger import JsonFormatter

from .app_config import ServerConfig
from .utils import (
    HealthCheck,
    ThreadingLocalContextFilter,
    configure_logger,
    initialize_aws_services,
    initialize_token_key,
    read_token_key,
)
from .viewpoint.routers import ViewpointRouter
from .viewpoint.worker import ViewpointWorker

# Configure GDAL to throw Python exceptions on errors
gdal.UseExceptions()

uvicorn_log_level_lookup = {
    logging.CRITICAL: "critical",
    logging.ERROR: "error",
    logging.WARNING: "warning",
    logging.INFO: "info",
    logging.DEBUG: "debug",
}


try:
    aws_ddb, aws_s3, aws_sqs = initialize_aws_services()
except ClientError as err:
    sys.exit(f"Fatal error occurred while initializing AWS services. Exiting. {err}")


def configure_tile_server_logging() -> logging.Logger:
    default_formatter = JsonFormatter(fmt="%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    correlation_formatter = JsonFormatter(
        fmt="%(asctime)s %(levelname)s %(correlation_id)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    worker_filter = ThreadingLocalContextFilter(["correlation_id"])
    access_filter = CorrelationIdFilter(default_value="-")
    log_level = ServerConfig.tile_server_log_level
    uvicorn_logger = logging.getLogger("uvicorn")
    configure_logger(uvicorn_logger, log_level, log_formatter=default_formatter)
    worker_logger = uvicorn_logger.getChild("worker")
    configure_logger(worker_logger, log_level, log_formatter=correlation_formatter, log_filter=worker_filter)
    uvicorn_error_logger = logging.getLogger("uvicorn.error")
    configure_logger(uvicorn_error_logger, log_level, log_formatter=default_formatter)
    uvicorn_access_logger = logging.getLogger("uvicorn.access")
    configure_logger(uvicorn_access_logger, log_level, log_formatter=correlation_formatter, log_filter=access_filter)

    return worker_logger


def initialize_viewpoint_router() -> ViewpointRouter:
    """
    This function creates an instance of a ViewpointRouter using initialized AWS services.
    Application will exit if the router fails to create.

    :return: An instance of a ViewpointRouter.
    """
    initialize_token_key()
    sleep(1)

    encryptor = Fernet(read_token_key())
    try:
        return ViewpointRouter(aws_ddb, aws_sqs, aws_s3, encryptor)
    except Exception as err:
        sys.exit(f"Fatal error occurred while initializing AWS services. Check your credentials! Exiting. {err}")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AbstractAsyncContextManager[None] | FastAPI:
    """
    Start the Viewpoint Worker as part of the FastAPI lifespan.

    :return: The lifespan construct associated with the fast API

    This function starts the Viewpoint Worker as part of the FastAPI lifespan,
    and stops it after yield.
    For more information, refer to FastAPI events
    documentation at : https://fastapi.tiangolo.com/advanced/events/

    Note:
        aws_s3, viewpoint_database and viewpoint_request_queue should be predefined
        before this function's call.
        They represent your AWS S3 bucket instance,
        your database instance and a queue of requests respectively.
    """
    # startup functions before serving requests

    worker_logger = configure_tile_server_logging()

    # create viewpoint worker
    viewpoint_worker = ViewpointWorker(aws_sqs, aws_s3, aws_ddb, worker_logger)
    viewpoint_worker.start()
    yield
    # shutdown functions after done serving requests
    viewpoint_worker.join(timeout=20)


# Initialize FastAPI app
app = FastAPI(title="OSML Tile Server")

viewpoint_router = initialize_viewpoint_router()

# Include the viewpoint router in the FastAPI app
app.include_router(viewpoint_router.router)

# Apply API versioning
app = VersionedFastAPI(
    app,
    enable_latest=True,
    description="A minimalistic tile server for imagery hosted in the cloud",
    terms_of_service="https://example.com/terms/",
    contact={
        "name": "Amazon Web Services",
        "email": "aws-osml-admin@amazon.com",
        "url": "https://github.com/aws-solutions-library-samples/osml-tile-server/issues",
    },
    license_info={
        "license": """Copyright 2023-2024 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
        This AWS Content is provided subject to the terms of the AWS Customer Agreement
        available at https://aws.amazon.com/agreement or other written agreement between
        Customer and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.""",
        "name": "Amazon Web Services",
    },
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["X-Requested-With", "X-Request-ID"],
    expose_headers=["X-Request-ID"],
)
app.add_middleware(CorrelationIdMiddleware)


@app.get("/", response_class=HTMLResponse)
async def root() -> str:
    """
    Root endpoint for the application.

    Returns a welcome message and basic application information including
    contact and license details.

    :return: Welcome message with application information.
    """
    homepage_description = f"""
    <html>
        <head>
            <title>{app.title}</title>
        </head>
        <body>
            <h1>{app.title} - {app.version}</h1>
            <p>{app.description}.</p>
            <p>If you need help or support,
                please cut an issue ticket <a href="{app.contact["url"]}">in our github project</a>.</p>
            <p><a href="/latest/docs">Swagger</a></p>
            <p><a href="/latest/redoc">ReDoc</a></p>
            <p>The license of this product: {app.license_info["license"]}</p>
        </body>
    </html>
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
    uvicorn.run(
        app, host="0.0.0.0", port=4557, reload=True, log_level=uvicorn_log_level_lookup[ServerConfig.tile_server_log_level]
    )
