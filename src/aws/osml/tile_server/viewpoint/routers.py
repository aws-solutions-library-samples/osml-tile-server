
from secrets import token_hex
import shutil
from fastapi import APIRouter, HTTPException, Response, Query
from starlette.responses import StreamingResponse
from aws.osml.photogrammetry.coordinates import ImageCoordinate

from aws.osml.tile_server.viewpoint.models import (
    ViewpointRequest,
    PixelRangeAdjustmentType,
    ViewpointDescription,
    ViewpointStatus,
    InternalViewpointState,
    ViewpointSummary,
    UpdateViewpointRequest
)
from aws.osml.tile_server.viewpoint.depends import ViewpointStatusTableDep
from aws.osml.tile_server.viewpoint.utils import get_media_type, get_tile_factory, get_viewpoint_detail
from aws.osml.gdal import GDALImageFormats, GDALCompressionOptions, load_gdal_dataset

import boto3
import botocore


from pathlib import Path
import os
import io
from typing import List
from osgeo import gdalconst, gdal

import logging

logger = logging.getLogger("uvicorn")
gdal.UseExceptions()

s3_resource = boto3.resource('s3')

FILESYSTEM_CACHE_ROOT = os.getenv("VIEWPOINT_FILESYSTEM_CACHE", "/tmp/viewpoint")

router = APIRouter(
    prefix="/viewpoints",
    tags=["viewpoints"],
    dependencies=[],
    responses={404: {"description": "Not found!"}},
)

@router.get("/")
async def list_viewpoints(viewpoint_status_table: ViewpointStatusTableDep) -> List[ViewpointSummary]:
    """
    Get a list of viewpoints in the database

    :param viewpoint_status_table: Viewpoint Status Table

    :return: a list of viewpoints with details
    """
    viewpoint_list = viewpoint_status_table.list_viewpoints()

    if not viewpoint_list:
        raise HTTPException(status_code=200, detail="The Viewpoint List is empty! Please create a viewpoint request!")

    return viewpoint_status_table.list_viewpoints()


@router.post("/", status_code=201)
async def create_viewpoint(request: ViewpointRequest, viewpoint_status_table: ViewpointStatusTableDep) -> ViewpointDescription:
    """
    Create a viewpoint item, then it copies the imagery file from S3 to EFS, then create a item into the database

    :param request: client's request which contains name, file source, and range type
    :param viewpoint_status_table: Viewpoint Status Table

    TODO: Below requires CDK changes
        - utilize efs service
        - utilize dynamodb
        - utilize sqs for background tasks

    :return: success creation of viewpoint
    """

    # Create unique viewpoint_id
    viewpoint_id = token_hex(16)

    file_name = request.object_key.split("/")[-1]

    # TODO rename local to EFS (CDK changes required)
    local_viewpoint_folder = Path(FILESYSTEM_CACHE_ROOT, viewpoint_id)
    local_viewpoint_folder.mkdir(parents=True, exist_ok=True)
    local_object_path = Path(local_viewpoint_folder, file_name)

    try:
        s3_resource.Object(request.bucket_name, request.object_key).download_file(str(local_object_path.absolute()))
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == "404":
            raise HTTPException(status_code=404, detail=f"The {request.bucket_name} bucket does not exist! Error={err}")
        elif err.response['Error']['Code'] == "403":
            raise HTTPException(status_code=403, detail=f"You do not have permission to access {request.bucket_name} bucket! Error={err}")

        raise HTTPException(status_code=400, detail=f"Image Tile Server cannot process your S3 request! Error={err}")

    # TODO convert this to DDB (CDK changes required)
    # Create description of the viewpoint for future reference
    new_viewpoint_description = ViewpointDescription(
        viewpoint_id=viewpoint_id,
        viewpoint_name=request.viewpoint_name,
        bucket_name=request.bucket_name,
        object_key=request.object_key,
        tile_size=request.tile_size,
        range_adjustment=request.range_adjustment,
        status=ViewpointStatus.REQUESTED,
        local_object_path=local_object_path
    )

    new_viewpoint_state = InternalViewpointState(
        description=new_viewpoint_description,
        local_object_path=local_object_path
    )

    viewpoint_status_table.update_state(new_viewpoint_state)
    new_viewpoint_state.description.status = ViewpointStatus.READY
    viewpoint_status_table.update_state(new_viewpoint_state)

    return new_viewpoint_description


@router.get("/{viewpoint_id}")
async def describe_viewpoint(viewpoint_id: str, viewpoint_status_table: ViewpointStatusTableDep) -> ViewpointDescription:
    """
    Get viewpoint details based on provided viewpoint id

    :param viewpoint_id: Unique viewpoint id
    :param viewpoint_status_table: Viewpoint Status Table

    :return: viewpoint detail
    """
    viewpoint_state = await get_viewpoint_detail(viewpoint_id, viewpoint_status_table)

    return viewpoint_state.description


@router.put("/{viewpoint_id}", status_code=201)
async def update_viewpoint(viewpoint_id: str, request: UpdateViewpointRequest, viewpoint_status_table: ViewpointStatusTableDep) -> ViewpointDescription:
    viewpoint_state = await get_viewpoint_detail(viewpoint_id, viewpoint_status_table)

    cannot_update_status = [ViewpointStatus.DELETED, ViewpointStatus.REQUESTED, ViewpointStatus.UPDATING]

    logger.info(viewpoint_state.description.status)
    logger.info(cannot_update_status)
    logger.info(viewpoint_state.description.status in cannot_update_status)

    if viewpoint_state.description.status in cannot_update_status:
        raise HTTPException(status_code=403, detail="You cannot update this Viewpoint Item does not exist! Please provide a correct viewpoint id!")
    
    update_viewpoint_description = ViewpointDescription(
        viewpoint_name=request.viewpoint_name,
        tile_size=request.tile_size,
        range_adjustment=request.range_adjustment,
    )

    update_viewpoint_state = InternalViewpointState(
        description=update_viewpoint_description,
        local_object_path=viewpoint_state.description.local_object_path
    )

    viewpoint_status_table.update_state(update_viewpoint_state)

    return update_viewpoint_description


@router.delete("/{viewpoint_id}")
async def delete_viewpoint(viewpoint_id: str, viewpoint_status_table: ViewpointStatusTableDep) -> ViewpointDescription:
    """
    Remove the file from the EFS and update the database to indicate that it has been deleted

    :param viewpoint_id: Unique viewpoint id
    :param viewpoint_status_table: Viewpoint Status Table

    :return: success that viewpoint has been deleted
    """
    viewpoint_state = await get_viewpoint_detail(viewpoint_id, viewpoint_status_table)

    if viewpoint_state.description.status == ViewpointStatus.DELETED:
        raise HTTPException(status_code=400, detail="This viewpoint file has already been deleted! Viewpoint: {viewpoint_state.description}")

    if viewpoint_state:
        shutil.rmtree(viewpoint_state.local_object_path.parent, ignore_errors=True)

    viewpoint_state.description.status = ViewpointStatus.DELETED
    viewpoint_state.local_object_path = None
    viewpoint_status_table.update_state(viewpoint_state)

    return viewpoint_state.description


@router.get("/{viewpoint_id}/metadata")
async def get_metadata(viewpoint_id: str, viewpoint_status_table: ViewpointStatusTableDep):
    viewpoint_state = await get_viewpoint_detail(viewpoint_id, viewpoint_status_table)

    # Convert to reusable methods
    if viewpoint_state.description.status == ViewpointStatus.DELETED:
        raise HTTPException(status_code=400, detail="Cannot view MetaData for this image since this is already been deleted.")
    elif viewpoint_state.description.status == ViewpointStatus.REQUESTED:
        raise HTTPException(status_code=400, detail="This viewpoint has been requested and not in READY state. Please try again later.")
    
    viewpoint_path = viewpoint_state.local_object_path.as_posix()

    ds, sm = load_gdal_dataset(viewpoint_path)
    metadata = ds.GetMetadata()

    if not metadata:
        raise HTTPException(status_code=200, detail="The metadata is empty!")
    
    return { "metadata": metadata }


@router.get("/{viewpoint_id}/bounds")
async def get_bounds(viewpoint_id: str, viewpoint_status_table: ViewpointStatusTableDep):
    viewpoint_state = await get_viewpoint_detail(viewpoint_id, viewpoint_status_table)

    if viewpoint_state.description.status == ViewpointStatus.DELETED:
        raise HTTPException(status_code=400, detail="Cannot view Bounds for this image since this is already been deleted.")
    elif viewpoint_state.description.status == ViewpointStatus.REQUESTED:
        raise HTTPException(status_code=400, detail="This viewpoint has been requested and not in READY state. Please try again later.")

    viewpoint_path = viewpoint_state.local_object_path.as_posix()

    ds, sm = load_gdal_dataset(viewpoint_path)
    width = ds.RasterXSize
    height = ds.RasterYSize

    world_coordinates = []
    for coordinates in [[0, 0], [0, height], [width, height], [width/2, height/2]]:
        world_coordinates.append(sm.image_to_world(ImageCoordinate(coordinates)).to_dms_string())

    return { "bounds": world_coordinates }

    
@router.get("/{viewpoint_id}/info")
async def get_info(viewpoint_id: str, viewpoint_status_table: ViewpointStatusTableDep):
    # *Body:* Empty
    # *Response:* Viewpoint Info [as feature]
    # So I'd start the info with creating a GeoJSON feature that has a Polygon geometry for the image boundary 
    # (i.e. the world coordinates above). 
    # It would also be worth looking at examples of STAC items to see what other information they include.
    viewpoint_state = await get_viewpoint_detail(viewpoint_id, viewpoint_status_table)

    # Convert to reusable methods
    if viewpoint_state.description.status == ViewpointStatus.DELETED:
        raise HTTPException(status_code=400, detail="Cannot view MetaData for this image since this is already been deleted.")
    elif viewpoint_state.description.status == ViewpointStatus.REQUESTED:
        raise HTTPException(status_code=400, detail="This viewpoint has been requested and not in READY state. Please try again later.")
    
    viewpoint_path = viewpoint_state.local_object_path.as_posix()

    ds, sm = load_gdal_dataset(viewpoint_path)
    width = ds.RasterXSize
    height = ds.RasterYSize

    world_coordinates = []
    for coordinates in [[0, 0], [0, height], [width, height], [width/2, height/2]]:
        world_coordinates.append(sm.image_to_world(ImageCoordinate(coordinates)).to_dms_string())
    
    return { "features": None }

@router.get("/{viewpoint_id}/statistics")
async def get_statistics(viewpoint_id: str, viewpoint_status_table: ViewpointStatusTableDep):
    viewpoint_state = await get_viewpoint_detail(viewpoint_id, viewpoint_status_table)

    # Convert to reusable methods
    if viewpoint_state.description.status == ViewpointStatus.DELETED:
        raise HTTPException(status_code=400, detail="Cannot view MetaData for this image since this is already been deleted.")
    elif viewpoint_state.description.status == ViewpointStatus.REQUESTED:
        raise HTTPException(status_code=400, detail="This viewpoint has been requested and not in READY state. Please try again later.")

    viewpoint_path = viewpoint_state.local_object_path.as_posix()

    try: 
        gdalOptions = gdal.InfoOptions(format='json', showMetadata=False, )
        gdalInfo = gdal.Info(viewpoint_path, options=gdalOptions)
    except Exception as err:
        raise HTTPException(status_code=400, detail=f"Failed to fetch statistics of an image. {err}")
    
    return { "image_statistics": gdalInfo }

@router.get("/{viewpoint_id}/preview")
async def get_preview():
    # *Body:* Empty
    # *Response:* Image overview in format
    pass

@router.get("/{viewpoint_id}/tiles/{z}/{x}/{y}.{tile_format}")
async def get_tile(viewpoint_id: str,
                   z: int,
                   x: int,
                   y: int,
                   viewpoint_status_table: ViewpointStatusTableDep,
                   tile_format: GDALImageFormats = Path(GDALImageFormats.PNG,
                                                        description="Output image type. Defaults to PNG."),
                   compression: GDALCompressionOptions = Query(GDALCompressionOptions.NONE,
                                                               description="Compression Algorithm for image."),
                   ) -> Response:

    viewpoint_state = await get_viewpoint_detail(viewpoint_id, viewpoint_status_table)

    if viewpoint_state.description.status != ViewpointStatus.READY:
        raise HTTPException(status_code=400,
                            detail=f"Viewpoint is not READY to serve tiles. Current status is: {viewpoint_state.description.status}")

    if viewpoint_state.description.range_adjustment is not PixelRangeAdjustmentType.NONE:
        tile_factory = get_tile_factory(tile_format, compression, str(viewpoint_state.local_object_path.absolute()),
                                        output_type=gdalconst.GDT_Byte,
                                        range_adjustment=viewpoint_state.description.range_adjustment)
    else:
        tile_factory = get_tile_factory(tile_format, compression, str(viewpoint_state.local_object_path.absolute()))
    
    if tile_factory is None:
        raise HTTPException(status_code=500,
                            detail=f"Unable to read tiles from viewpoint {viewpoint_state.description.viewpoint_id}")

    tile_size = viewpoint_state.description.tile_size
    image_bytes = tile_factory.create_encoded_tile([x * tile_size, y * tile_size, tile_size, tile_size])
    
    return StreamingResponse(io.BytesIO(image_bytes), media_type=get_media_type(tile_format), status_code=200)

@router.get("/{viewpoint_id}/crop/{minx},{miny},{maxx},{maxy}/{width}x{height}.{tile_format}")
async def get_crop():
    # *Body:* Empty
    # *Response:* Image crop in format   
    pass


