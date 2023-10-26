import logging
import sys

from botocore.exceptions import ClientError
from fastapi import FastAPI
from osgeo import gdal

from .utils.aws_services import initialize_ddb, initialize_s3
from .viewpoint.database import ViewpointStatusTable
from .viewpoint.routers import ViewpointRouter

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

gdal.UseExceptions()

# initialize aws services
aws_ddb = initialize_ddb()
aws_s3 = initialize_s3()

# viewpoint
try:
    viewpoint_database = ViewpointStatusTable(aws_ddb)
except ClientError as err:
    logger.error("Fatal error occurred while initializing viewpoint database. Exception: {}".format(err))
    sys.exit("Fatal error occurred while initializing viewpoint database.  Exiting.")
viewpoint_router = ViewpointRouter(viewpoint_database, aws_s3)

# routers api
app.include_router(viewpoint_router.router)


@app.get("/")
async def root():
    homepage_description = f"""Hello! Welcome to {app.title} - {app.version}! {app.description}.

    If you need help or support, please cut an issue ticket of this product - {app.contact["url"]}.

    The license of this product: {app.license_info["license"]}
    """
    return homepage_description
