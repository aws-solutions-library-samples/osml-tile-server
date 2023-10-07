import logging

from fastapi import HTTPException
from aws.osml.tile_server.viewpoint.models import PixelRangeAdjustmentType, ViewpointApiNames, ViewpointStatus

from typing import Optional
from functools import cache
from fastapi import HTTPException

from aws.osml.gdal import GDALImageFormats, GDALCompressionOptions, load_gdal_dataset
from aws.osml.image_processing import GDALTileFactory

def get_media_type(tile_format: GDALImageFormats) -> str:
    """
    Obtain the meta type based on the given tile format

    :param tile_format: tile format

    :return: image format
    """
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
    """
    This is a helper function which is to validate if we can execute an operation based on the 
    given status

    :param tile_format: current status of a viewpoint
    :param compression: api operation
    :param local_object_path: the path of an imagery
    :param output_type: the GDAL pixel type in the output tile
    :param range_adjustment: the type of scaling used to convert raw pixel values to the output range

    :return: factory capable of producing tiles from a given GDAL raster dataset
    """
    
    ds, sensor_model = load_gdal_dataset(local_object_path)
    tile_factory = GDALTileFactory(ds,
                                   sensor_model,
                                   tile_format,
                                   compression,
                                   output_type,
                                   range_adjustment
                                   )
    return tile_factory


async def validate_viewpoint_status(current_status: ViewpointStatus, api_operation: ViewpointApiNames) -> None:
    """
    This is a helper function which is to validate if we can execute an operation based on the 
    given status

    :param current_status: current status of a viewpoint
    :param api_operation: api operation

    :return: viewpoint detail
    """
    if current_status == ViewpointStatus.DELETED:
        raise HTTPException(status_code=400, detail=f"Cannot view {api_operation} for this image since this has already been deleted.")
    elif current_status == ViewpointStatus.REQUESTED:
        raise HTTPException(status_code=400, detail="This viewpoint has been requested and not in READY state. Please try again later.")
