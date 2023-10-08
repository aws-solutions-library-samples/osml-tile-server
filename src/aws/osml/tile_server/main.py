import logging

from fastapi import FastAPI
from osgeo import gdal

from aws.osml.tile_server.utils.aws_services import initialize_ddb, initialize_s3
from aws.osml.tile_server.viewpoint.database import ViewpointStatusTable
from aws.osml.tile_server.viewpoint.routers import ViewpointRouter

app = FastAPI()
gdal.UseExceptions()

# initialize aws services
aws_ddb = initialize_ddb()
aws_s3 = initialize_s3()

# viewpoint
viewpoint_database = ViewpointStatusTable(aws_ddb)
viewpoint_router = ViewpointRouter(viewpoint_database, aws_s3)

# routers api
app.include_router(viewpoint_router.router)

logger = logging.getLogger("uvicorn")


@app.get("/")
async def root():
    return {"message": "Hello Tile Server!"}
