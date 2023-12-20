#  Copyright 2023 Amazon.com, Inc. or its affiliates.

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
    Obtain the meta-type based on the given tile format.

    :param tile_format: GDAL Image format associated with the tile.

    :return: The associated image format in plain text.
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

    :param width: Width of the full image at the highest resolution.
    :param height: Height of the full image at the highest resolution.
    :param preview_size: The desired size of the lowest resolution / thumbnail image.
    :return: The list of scale factors needed for each level in the tile pyramid, e.g. [2, 4, 8, 16 ...]
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
    """
    Class representing a pool of GDALTileFactory objects.
    """

    def __init__(
        self,
        tile_format: GDALImageFormats,
        tile_compression: GDALCompressionOptions,
        local_object_path: str,
        output_type: Optional[int] = None,
        range_adjustment: RangeAdjustmentType = RangeAdjustmentType.NONE,
    ) -> None:
        """
        This initializes a TileFactoryPool, representing a pool of GDALTileFactory objects. The purpose
        of this class is to manage resources (the GDALTileFactory objects) efficiently, typically for a
        multithreading environment.


        :param tile_format: The image format of the tiles.
        :param tile_compression: The compression options for the tiles.
        :param local_object_path: The local path to the object.
        :param output_type: The output type. Defaults to None.
        :param range_adjustment: The range adjustment type, defaults to RangeAdjustmentType.NONE.

        :return: None
        """
        self.lock = RLock()
        self.current_inventory = []
        self.total_inventory = 0
        self.tile_format = tile_format
        self.tile_compression = tile_compression
        self.local_object_path = local_object_path
        self.output_type = output_type
        self.range_adjustment = range_adjustment

    def checkout(self) -> GDALTileFactory:
        """
        Handles the checkout process. If the current inventory is not empty,
        it pops out the first GDALTileFactory object. If the inventory is empty,
        a new GDALTileFactory object is created, added to the inventory, and returned.

        :return: Instance of the GDALTileFactory class.
        """
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
        """
        Adds a GDALTileFactory object to the current inventory.

        :param tf: GDALTileFactory object to be checked in to the index.
        :return: None
        """

        with self.lock:
            self.current_inventory.append(tf)

    @contextmanager
    def checkout_in_context(self) -> None:
        """
        A context manager for using the `checkout` method in a `with` statement. Exception safe - resources are
        guaranteed to be `checkin`ed. It yields the resource obtained from `checkout` for use inside the `with`
        statement.
        :return: None
        :raise Exception: Any exceptions raised within the `with` block or by `checkout` and `checkin` methods
        """
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
    """
    Create and return a pool of tile factories.

    :param tile_format: The format of the tiles to be created.
    :param tile_compression: The compression options for the tiles.
    :param local_object_path: The path to the local object storage.
    :param output_type: The optional output type for the tiles.
    :param range_adjustment: The range adjustment type for the tiles.
    :return: The tile factory pool.

    """
    return TileFactoryPool(tile_format, tile_compression, local_object_path, output_type, range_adjustment)


def perform_gdal_translation(dataset: Dataset, gdal_options: Dict) -> Optional[bytearray]:
    """
    Perform GDAL translation on a dataset with given GDAL options.

    :param dataset: The input GDAL dataset to be translated.
    :param gdal_options: Options for the GDAL translation.
    :return: A bytearray containing the translated data, or None if translation fails.
    """
    if "format" in gdal_options and isinstance(gdal_options.get("format"), GDALImageFormats):
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
    else:
        raise ValueError("gdal_options missing format key ({'format': <GDALImageFormats object>})")
