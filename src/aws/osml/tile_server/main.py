import asyncio
import logging
import sys

from botocore.exceptions import ClientError
from fastapi import FastAPI
from osgeo import gdal

from .app_config import ServerConfig
from .utils.aws_services import initialize_ddb, initialize_s3, initialize_sqs
from .viewpoint.database import ViewpointStatusTable
from .viewpoint.queue import ViewpointRequestQueue
from .viewpoint.routers import ViewpointRouter
from .viewpoint.worker import ViewpointWorker

gdal.UseExceptions()

logger = logging.getLogger("uvicorn")
app = FastAPI(
    title="OSML Tile Server",
    description="A minimalistic tile server for imagery hosted in the cloud",
    version="0.1.0",
    terms_of_service="http://example.com/terms/",
    contact={
        "name": "Amazon Web Services",
        "email": "aws-osml-admin@amazon.com",
        "url": "https://github.com/aws-solutions-library-samples/osml-tile-server/issues",
    },
    license_info={
        "license": """© 2023 Amazon Web Services, Inc. or its affiliates. All Rights Reserved.
        This AWS Content is provided subject to the terms of the AWS Customer Agreement
        available at http://aws.amazon.com/agreement or other written agreement between
        Customer and either Amazon Web Services, Inc. or Amazon Web Services EMEA SARL or both.""",
    },
)

# initialize aws services
aws_ddb = initialize_ddb()
aws_s3 = initialize_s3()
aws_sqs = initialize_sqs()

# viewpoint
try:
    viewpoint_database = ViewpointStatusTable(aws_ddb)
except ClientError as err:
    logger.error("Fatal error occurred while initializing viewpoint database. Exception: {}".format(err))
    sys.exit("Fatal error occurred while initializing viewpoint database.  Exiting.")

viewpoint_request_queue = ViewpointRequestQueue(aws_sqs, ServerConfig.viewpoint_request_queue)
viewpoint_router = ViewpointRouter(viewpoint_database, viewpoint_request_queue, aws_s3)

# routers api
app.include_router(viewpoint_router.router)


@app.on_event("startup")
async def run_viewpoint_worker():
    viewpoint_worker = ViewpointWorker(viewpoint_request_queue, aws_s3, viewpoint_database)
    loop = asyncio.get_event_loop()
    loop.create_task(viewpoint_worker.run())


@app.get("/")
async def root():
    homepage_description = f"""Hello! Welcome to {app.title} - {app.version}! {app.description}.

    If you need help or support, please cut an issue ticket of this product - {app.contact["url"]}.

    The license of this product: {app.license_info["license"]}
    """
    return homepage_description


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=4557, reload=True, debug=True)
