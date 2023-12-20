#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import pickle
from unittest import TestCase
from unittest.mock import patch

from aws.osml.tile_server.utils import initialize_token_key, read_token_key


class TestToken(TestCase):
    @patch("builtins.open")
    @patch.object(pickle, "dump")
    def test_initialize_token_key(self, mock_pickle_dump, mock_open):
        initialize_token_key()
        assert mock_open.has_been_called()
        assert mock_pickle_dump.has_been_called()

    @patch("builtins.open")
    @patch.object(pickle, "load", return_value=b"dummytoken")
    def test_read_token_key(self, mock_pickle_read, mock_open):
        token_key = read_token_key()
        assert token_key == b"dummytoken"
