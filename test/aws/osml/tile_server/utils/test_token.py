#  Copyright 2023-2024 Amazon.com, Inc or its affiliates.
import unittest
from unittest.mock import mock_open, patch

from aws.osml.tile_server.utils import initialize_token_key, read_token_key


class TestToken(unittest.TestCase):
    @patch("aws.osml.tile_server.utils.token.path.isfile")
    @patch("builtins.open", new_callable=mock_open)
    def test_initialize_token_key_file_does_not_exist(self, mocked_open, mocked_isfile):
        mocked_isfile.return_value = False

        initialize_token_key()

        mocked_open.assert_called_once()
        handle = mocked_open.return_value

        handle.write.assert_called()

    @patch("aws.osml.tile_server.utils.token.path.isfile")
    @patch("builtins.open", new_callable=mock_open)
    def test_initialize_token_key_file_exists(self, mock_open, mock_isfile):
        mock_isfile.return_value = True

        initialize_token_key()

        mock_open.assert_not_called()

    @patch("builtins.open", new_callable=mock_open, read_data='{"token": "good_key"}')
    def test_read_token_key_valid(self, mocked_open):
        actual_token = read_token_key()
        expected_token = "good_key".encode()
        self.assertEqual(actual_token, expected_token)

        mocked_open.assert_called_once()
        handle = mocked_open.return_value
        handle.read.assert_called_once()

    @patch("builtins.open", new_callable=mock_open, read_data="bad_token")
    def test_read_token_key_invalid(self, mocked_open):
        token = read_token_key()
        self.assertEqual(token, None)

        mocked_open.assert_called_once()
        handle = mocked_open.return_value
        handle.read.assert_called_once()


if __name__ == "__main__":
    unittest.main()
