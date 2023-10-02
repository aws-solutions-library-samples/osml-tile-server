import logging

from fastapi import HTTPException
from aws.osml.tile_server.viewpoint.depends import ViewpointStatusTableDep
from aws.osml.tile_server.viewpoint.models import PixelRangeAdjustmentType, ViewpointDescription, ViewpointStatus

from typing import Optional
from functools import cache
from fastapi import HTTPException

from aws.osml.gdal import GDALImageFormats, GDALCompressionOptions, load_gdal_dataset
from aws.osml.image_processing import GDALTileFactory


logger = logging.getLogger("uvicorn")


async def get_viewpoint_detail(
        viewpoint_id: str,
        viewpoint_status_table: ViewpointStatusTableDep
    ) -> ViewpointDescription:
    """
    Get viewpoint detail, if viewpoint does not exist, it will throw an error stating
        that it does not exist

    :param viewpoint_id: Unique viewpoint id
    :param viewpoint_status_table: Viewpoint Status Table

    :return: viewpoint detail
    """
    viewpoint_detail = viewpoint_status_table.get_state(viewpoint_id)
    
    if not viewpoint_detail:
        raise HTTPException(status_code=404, detail="Viewpoint Item does not exist! Please provide a correct viewpoint id!") 
    
    return viewpoint_detail

def get_media_type(tile_format: GDALImageFormats) -> str:
    if tile_format == GDALImageFormats.PNG:
        return "image/png"
    elif tile_format == GDALImageFormats.NITF:
        return "image/nitf"
    elif tile_format == GDALImageFormats.JPEG:
        return "image/jpeg"
    elif tile_format == GDALImageFormats.GTIFF:
        return "image/tiff"
    return "image"

@cache
def get_tile_factory(tile_format: GDALImageFormats,
                     compression: GDALCompressionOptions,
                     local_object_path: str,
                     output_type: Optional[int] = None,
                     range_adjustment: PixelRangeAdjustmentType = PixelRangeAdjustmentType.NONE) -> GDALTileFactory:
    ds, sensor_model = load_gdal_dataset(local_object_path)
    tile_factory = GDALTileFactory(ds,
                                   sensor_model,
                                   tile_format,
                                   compression,
                                   output_type,
                                   range_adjustment
                                   )
    return tile_factory
