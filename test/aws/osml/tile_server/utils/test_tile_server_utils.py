#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest
from unittest import TestCase
from unittest.mock import patch

from osgeo import gdal
from test_config import TestConfig

from aws.osml.gdal import GDALCompressionOptions, GDALImageFormats
from aws.osml.tile_server.utils import (
    get_media_type,
    get_standard_overviews,
    get_tile_factory_pool,
    perform_gdal_translation,
)


class TestTileServerUtils(TestCase):
    """Unit tests for utility functions in tile_server."""

    def test_get_media_type(self):
        """Test retrieving the media type based on GDAL image format."""
        self.assertEqual(get_media_type(GDALImageFormats.PNG), "image/png")
        self.assertEqual(get_media_type(GDALImageFormats.NITF), "image/nitf")
        self.assertEqual(get_media_type(GDALImageFormats.JPEG), "image/jpeg")
        self.assertEqual(get_media_type(GDALImageFormats.GTIFF), "image/tiff")
        self.assertEqual(get_media_type("OtherImageFormat"), "image")

    def test_get_standard_overviews(self):
        """Test calculating standard overview levels for image pyramids."""
        overviews = get_standard_overviews(10240, 10240, 512)
        self.assertEqual(overviews, [2, 4, 8, 16, 32])

        overviews = get_standard_overviews(512, 512, 512)
        self.assertEqual(overviews, [])

    @patch("aws.osml.tile_server.utils.tile_server_utils.TileFactoryPool")
    def test_get_tile_factory_pool(self, mock_factory):
        """Test creating and retrieving a TileFactoryPool from the LRU cache."""
        mock_tile_format = GDALImageFormats.NITF
        mock_tile_compression = GDALCompressionOptions.NONE
        mock_path = "path"
        get_tile_factory_pool(mock_tile_format, mock_tile_compression, mock_path)
        assert mock_factory.has_been_called_with(mock_tile_format, mock_tile_compression, mock_path)

    def test_perform_gdal_translation(self):
        """Test performing a GDAL translation with a valid format."""
        test_ds = gdal.Open(TestConfig.test_file_path)
        buf = perform_gdal_translation(test_ds, {"format": GDALImageFormats.NITF})

        self.assertIsInstance(buf, bytearray)

    def test_perform_gdal_translation_no_format(self):
        """Test that perform_gdal_translation raises an error when no format is provided."""
        test_ds = gdal.Open(TestConfig.test_file_path)

        with self.assertRaises(ValueError):
            perform_gdal_translation(test_ds, {})

    @patch("osgeo.gdal.VSIFOpenL", autospec=True)
    def test_perform_gdal_translation_vsifile_handle(self, mock_vsi_open):
        """Test handling of a null VSI file handle during GDAL translation."""
        mock_vsi_open.return_value = None
        test_ds = gdal.Open(TestConfig.test_file_path)

        buf = perform_gdal_translation(test_ds, {"format": GDALImageFormats.NITF})

        self.assertIsNone(buf)


if __name__ == "__main__":
    unittest.main()
