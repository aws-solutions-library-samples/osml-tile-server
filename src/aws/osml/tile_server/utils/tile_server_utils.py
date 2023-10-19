from functools import cache
from typing import Optional

from fastapi import HTTPException
from osgeo import gdal

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType, load_gdal_dataset
from aws.osml.image_processing import GDALTileFactory
from aws.osml.tile_server.viewpoint.models import ViewpointApiNames, ViewpointStatus


def get_media_type(tile_format: GDALImageFormats) -> str:
    """
    Obtain the meta type based on the given tile format

    :param tile_format: tile format

    :return: image format
    """
    supported_media_types = {
        GDALImageFormats.PNG.value.lower(): "image/png",
        GDALImageFormats.NITF.value.lower(): "image/nitf",
        GDALImageFormats.JPEG.value.lower(): "image/jpeg",
        GDALImageFormats.GTIFF.value.lower(): "image/tiff",
    }
    default_media_type = "image"
    return supported_media_types.get(tile_format.lower(), default_media_type)


@cache
def get_tile_factory(
    tile_format: GDALImageFormats,
    compression: GDALCompressionOptions,
    local_object_path: str,
    output_type: Optional[int] = None,
    range_adjustment: RangeAdjustmentType = RangeAdjustmentType.NONE,
) -> GDALTileFactory:
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
    return GDALTileFactory(ds, sensor_model, tile_format, compression, output_type, range_adjustment)


async def validate_viewpoint_status(current_status: ViewpointStatus, api_operation: ViewpointApiNames) -> None:
    """
    This is a helper function which is to validate if we can execute an operation based on the
    given status

    :param current_status: current status of a viewpoint
    :param api_operation: api operation

    :return: viewpoint detail
    """
    if current_status == ViewpointStatus.DELETED:
        raise HTTPException(
            status_code=400, detail=f"Cannot view {api_operation} for this image since this has already been deleted."
        )
    elif current_status == ViewpointStatus.REQUESTED:
        raise HTTPException(
            status_code=400, detail="This viewpoint has been requested and not in READY state. Please try again later."
        )


def generate_preview(local_object_path: str, img_format: GDALImageFormats, scale: int) -> Optional[bytearray]:
    ds, sensor_model = load_gdal_dataset(local_object_path)
    tmp_name = "preview.tmp"
    options_list = [f"-outsize {scale}% {scale}%", f"-of {img_format.value}"]
    options_string = " ".join(options_list)

    gdal.Translate(tmp_name, ds, options=options_string)

    # Read the VSIFile
    vsifile_handle = None
    try:
        vsifile_handle = gdal.VSIFOpenL(tmp_name, "r")
        if vsifile_handle is None:
            return None
        stat = gdal.VSIStatL(tmp_name, gdal.VSI_STAT_SIZE_FLAG)
        vsibuf = gdal.VSIFReadL(1, stat.size, vsifile_handle)
        return vsibuf
    finally:
        if vsifile_handle is not None:
            gdal.VSIFCloseL(vsifile_handle)
        gdal.GetDriverByName(img_format).Delete(tmp_name)
