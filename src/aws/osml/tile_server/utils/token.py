#  Copyright 2023 Amazon.com, Inc. or its affiliates.

import json
import logging
from os import path
from typing import Optional

from cryptography.fernet import Fernet

from ..app_config import ServerConfig

TS_TOKEN_FILE_NAME = "ts-token"

logger = logging.getLogger("uvicorn")


def initialize_token_key() -> None:
    """
    Initializes and stores a new encryption token key in a JSON file if one does not already exist.

    :return: None
    """
    file_path = path.join(f"/{ServerConfig.efs_mount_name}", TS_TOKEN_FILE_NAME)
    if not path.isfile(file_path):
        key = Fernet.generate_key()
        with open(file_path, "w") as token_file:  # Change to "w" for writing text.
            json.dump({"token": key.decode()}, token_file)  # Store as JSON.


def read_token_key() -> Optional[bytes]:
    """
    Reads and returns the encryption token key from a JSON file.

    :return: The encryption token key as a dictionary if the file exists and contains valid JSON, None otherwise.
    """
    file_path = path.join(f"/{ServerConfig.efs_mount_name}", TS_TOKEN_FILE_NAME)
    try:
        with open(file_path, "r") as token_file:
            token: str = json.load(token_file).get("token")
            return token.encode()
    except Exception as err:
        # Handle the case where the file is empty or not valid JSON
        logger.error(f"Not able to token key from json file {file_path} wth error: {err}")
        return None
