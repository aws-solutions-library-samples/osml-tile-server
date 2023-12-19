#  Copyright 2023 Amazon.com, Inc. or its affiliates.

from unittest import TestCase
from unittest.mock import patch

import pytest
from osgeo import gdal
from test_config import TestConfig

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats
from aws.osml.tile_server.utils import (
    get_media_type,
    get_standard_overviews,
    get_tile_factory_pool,
    perform_gdal_translation,
)

gdal.UseExceptions()


class TestTileServerUtils(TestCase):
    def test_get_media_type(self):
        assert get_media_type(GDALImageFormats.PNG) == "image/png"
        assert get_media_type(GDALImageFormats.NITF) == "image/nitf"
        assert get_media_type(GDALImageFormats.JPEG) == "image/jpeg"
        assert get_media_type(GDALImageFormats.GTIFF) == "image/tiff"
        assert get_media_type("OtherImageFormat") == "image"

    def test_get_standard_overviews(self):
        overviews = get_standard_overviews(10240, 10240, 512)
        assert overviews == [2, 4, 8, 16, 32]

        overviews = get_standard_overviews(512, 512, 512)
        assert overviews == []

    @patch("aws.osml.tile_server.utils.tile_server_utils.TileFactoryPool")
    def test_get_tile_factory_pool(self, mock_factory):
        mock_tile_format = GDALImageFormats.NITF
        mock_tile_compression = GDALCompressionOptions.NONE
        mock_path = "path"
        get_tile_factory_pool(mock_tile_format, mock_tile_compression, mock_path)
        assert mock_factory.has_been_called_with(mock_tile_format, mock_tile_compression, mock_path)

    def test_perform_gdal_translation(self):
        test_ds = gdal.Open(TestConfig.test_file_path)
        buf = perform_gdal_translation(test_ds, {"format": GDALImageFormats.NITF})
        assert isinstance(buf, bytearray)

    def test_perform_gdal_translation_no_format(self):
        test_ds = gdal.Open(TestConfig.test_file_path)
        with pytest.raises(ValueError):
            perform_gdal_translation(test_ds, {})

    @patch("aws.osml.tile_server.utils.tile_server_utils.gdal.VSIFOpenL")
    def test_perform_gdal_translation_vsifile_handle(self, mock_gdal):
        mock_gdal.return_value = None
        test_ds = gdal.Open(TestConfig.test_file_path)
        buf = perform_gdal_translation(test_ds, {"format": GDALImageFormats.NITF})
        assert buf is None
