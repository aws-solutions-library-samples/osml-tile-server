import logging
import sys

from botocore.exceptions import ClientError
from fastapi import FastAPI
from osgeo import gdal

from aws.osml.tile_server.utils.aws_services import initialize_ddb, initialize_s3
from aws.osml.tile_server.viewpoint.database import ViewpointStatusTable
from aws.osml.tile_server.viewpoint.routers import ViewpointRouter

logger = logging.getLogger("uvicorn")

app = FastAPI()
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
    return {"message": "Hello Tile Server!"}
