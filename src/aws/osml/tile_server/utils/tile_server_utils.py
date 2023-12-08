import logging
import threading
import time
from contextlib import contextmanager
from functools import lru_cache
from math import ceil, log
from threading import RLock
from typing import Dict, List, Optional
from uuid import uuid4

from osgeo import gdal
from osgeo.gdal import Dataset

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats, RangeAdjustmentType, load_gdal_dataset
from aws.osml.image_processing import GDALTileFactory

logger = logging.getLogger("uvicorn")


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


def get_standard_overviews(width: int, height: int, preview_size: int) -> List[int]:
    """
    This utility computes a list of reduced resolution scales that define a standard image pyramid for a given
    image and desired final preview size.

    :param width: width of the full image at highest resolution
    :param height: height of the full image at highest resolution
    :param preview_size: the desired size of the lowest resolution / thumbnail image.
    :return: The list of scale factors needed for each level in the tile pyramid e.g. [2, 4, 8, 16 ...]
    """
    min_side = min(width, height)
    num_overviews = ceil(log(min_side / preview_size) / log(2))
    if num_overviews > 0:
        result = []
        for i in range(1, num_overviews + 1):
            result.append(2**i)
        return result
    return []


class TileFactoryPool:
    def __init__(
        self,
        tile_format: GDALImageFormats,
        tile_compression: GDALCompressionOptions,
        local_object_path: str,
        output_type: Optional[int] = None,
        range_adjustment: RangeAdjustmentType = RangeAdjustmentType.NONE,
    ):
        self.lock = RLock()
        self.current_inventory = []
        self.total_inventory = 0
        self.tile_format = tile_format
        self.tile_compression = tile_compression
        self.local_object_path = local_object_path
        self.output_type = output_type
        self.range_adjustment = range_adjustment

    def checkout(self) -> GDALTileFactory:
        tf = None
        with self.lock:
            if self.current_inventory:
                tf = self.current_inventory.pop(0)

        if tf is None:
            thread_id = threading.get_native_id()

            start_time = time.perf_counter()
            ds, sensor_model = load_gdal_dataset(self.local_object_path)
            tf = GDALTileFactory(
                ds, sensor_model, self.tile_format, self.tile_compression, self.output_type, self.range_adjustment
            )
            end_time = time.perf_counter()
            logger.info(
                f"New TileFactory for {self.local_object_path}"
                f" created by thread {thread_id} in {end_time - start_time} seconds."
            )

            with self.lock:
                self.total_inventory += 1
                logger.info(f"Total TileFactory count for {self.local_object_path} is {self.total_inventory}")

        return tf

    def checkin(self, tf: GDALTileFactory) -> None:
        with self.lock:
            self.current_inventory.append(tf)

    @contextmanager
    def checkout_in_context(self):
        tf = None
        try:
            tf = self.checkout()
            yield tf
        finally:
            if tf:
                self.checkin(tf)


@lru_cache(maxsize=20)
def get_tile_factory_pool(
    tile_format: GDALImageFormats,
    tile_compression: GDALCompressionOptions,
    local_object_path: str,
    output_type: Optional[int] = None,
    range_adjustment: RangeAdjustmentType = RangeAdjustmentType.NONE,
) -> TileFactoryPool:
    return TileFactoryPool(tile_format, tile_compression, local_object_path, output_type, range_adjustment)


def perform_gdal_translation(dataset: Dataset, gdal_options: Dict) -> Optional[bytearray]:
    """
    Performs GDAL (Geospatial Data Abstraction Library) translation and reads a VSIFile.

    The GDAL translation is done on given dataset with gdal options. Then a VSIFile is read and returned from the
    translated dataset.

    Parameters
    ----------
    dataset : Dataset
        The input dataset to be translated.
    gdal_options : dict
        The options for GDAL translation.

    Returns
    -------
    vsibuf : bytearray, optional
        The read VSIFile. If the VSIFile cannot be opened, returns None.

    Raises
    ------
    GDALException
        If the translation is not successful or VSIFile cannot be read.

    Note ---- This function also takes care of cleaning up the temporary file memory in case of any exception by
    using finally clause.
    """
    tmp_name = f"/vsimem/{uuid4()}"

    gdal.Translate(tmp_name, dataset, **gdal_options)

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
        gdal.GetDriverByName(gdal_options.get("format")).Delete(tmp_name)
