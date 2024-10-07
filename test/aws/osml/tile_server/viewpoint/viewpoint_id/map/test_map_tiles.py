#  Copyright 2024 Amazon.com, Inc. or its affiliates.

from unittest import TestCase

import pytest


class TestMapTiles(TestCase):
    """Unit tests for map tiles endpoint in tile_server."""

    @pytest.mark.skip(reason="Test not implemented")
    def test_get_map_tiles(self):
        pass

    def test_invert_tile_row_index(self):
        from aws.osml.tile_server.viewpoint.viewpoint_id.map.tileset.tile import _invert_tile_row_index

        sample_tile_row = 347
        sample_tile_matrix = 10
        expected_inverted_tile_row = 676
        inverted_tile_row = _invert_tile_row_index(sample_tile_row, sample_tile_matrix)
        assert inverted_tile_row == expected_inverted_tile_row
